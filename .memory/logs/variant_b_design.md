# Variant B — Slice 1 design: durable materialization model

Status: DESIGN DRAFT — awaiting correctness + protocol/persistence review, then
performance `DESIGN_GATE`. No productive code changed.
Date: 2026-07-20
Scope: Slice 1 only (`.github/prompts/variant-b.prompt.md`). Slice 2+ (fetch
modes, orchestration, delta resolution, wiring) are explicitly OUT of scope.

Reconciliation basis: `.memory/logs/variant_b_reconciliation.md`, requirement
mappings #2, #8, #13, and "Bottom line for Slice 1+". This is an ADAPT-and-
extend design against the current schema, not a new subsystem.

---

## 1. Repository+commit materialization state record

**Decision: EXTEND `ZAOG_COMMIT_HIST` in place. Do not create a new table.**

Justification against actual current schema:
- `ZAOG_COMMIT_HIST` primary key is already exactly `(MANDT, REPO_KEY,
  COMMIT_SHA1)` — the identical grain the owner spec asks for. A new table
  would duplicate this key space and require keeping two per-commit tables in
  lockstep, which the owner spec explicitly forbids ("do not create
  duplicates").
- It is already the sole current source of "have" candidates
  (`zcl_abapgit_ortec_repo_state=>get_complete_commits`,
  `zcl_abapgit_ortec_fetch_neg=>get_have_commits`), so extending it in place
  keeps one authoritative table instead of introducing a second lookup a
  caller could forget to join.
- Existing columns `BRANCH_NAME` (last branch seen at write time) and
  `FETCHED_AT` are demoted to diagnostic/informational only — they are never
  read by the new certificate API. No column is removed (no data loss, no
  conversion of existing rows required beyond an additive append).

### DDIC delta — `ZAOG_COMMIT_HIST` (append only, key unchanged)

| Field | Type | Length | Notes |
|---|---|---|---|
| HIST_LEVEL | CHAR | 1 | `'U'`=unknown (default/initial), `'G'`=graph-complete, `'F'`=full-complete. Same inline-CHAR1 convention as `ZAOG_OBJ_STORE-STATUS`. |
| SNAP_STATE | CHAR | 1 | `'N'`=none (default/initial), `'P'`=pending, `'C'`=complete, `'I'`=invalid. |
| ATTEMPT_ID | CHAR | 32 | Same convention/length as `zcl_abapgit_ortec_pack_raw=>ty_session_id`; generated via `cl_system_uuid=>create_uuid_c32_static( )`. Blank when no attempt is in flight. |
| VERIFIED_AT | TZNTSTMPL (ROLLNAME) | — | Timestamp of the last successful verification (graph or snapshot). Untouched by `invalidate_commit`. |
| UPDATED_AT | TZNTSTMPL (ROLLNAME) | — | Timestamp of the last state transition of any kind. |

No index changes required: all API access is by the existing full primary
key, or by `(REPO_KEY, ...)` prefix for the bounded branch-cascade case (§2).

---

## 2. Branch state changes (`ZAOG_REPO_STATE` / `zcl_abapgit_ortec_repo_state`)

**DDIC delta:** add one field.

| Field | Type | Length | Notes |
|---|---|---|---|
| SNAP_STATE | CHAR | 1 | Denormalized copy of the referenced commit's `ZAOG_COMMIT_HIST.SNAP_STATE` at last publish. Lets a branch-readiness check be a single-row read with no join to the commit table. |

**Semantic (behavioral, non-DDIC) change — no physical rename:**
- `CURR_COMMIT` keeps its current meaning: last resolved remote tip for this
  branch. Advisory only, implies no certificate.
- `FETCH_COMMIT` is repurposed to be exactly the owner's "materialized commit
  pointer": the commit SHA whose certification this branch row currently
  publishes. It is written **only** by `publish_snapshot_complete` (§3), never
  by a raw fetch. This is the actual behavior change: today
  `update_after_fetch` sets `FETCH_COMMIT` immediately after a network fetch,
  before any verification — that write must move out of the raw-fetch path
  once Slice 2/3 lands. Slice 1 only introduces the new authoritative writer;
  it does not yet remove the old writer (that removal is Slice 9 territory
  once the new orchestration replaces the call site).
- `IS_SHALLOW` / `DEEPEN_LVL` are **deprecated for the new code path**:
  physically retained (no DDIC change, no data loss), but never read or
  written by `ZCL_ABAPGIT_ORTEC_MAT_STATE` or any Slice-1 logic. Candidate
  for physical removal in Slice 9 once Slice 2's explicit fetch modes replace
  every remaining reader.
- No column is removed and no existing SELECT against `ZAOG_REPO_STATE`
  breaks — the change is purely additive plus a documented meaning narrowing
  of an existing field's *future* writer.

**Open question for reviewer:** whether to physically rename `FETCH_COMMIT` →
`MAT_COMMIT` for clarity despite the DDIC rename/conversion cost, or keep the
name and rely on this document + code comments. Design recommendation: keep
the name (avoid unnecessary schema churn in a slice that must stay minimal).

---

## 3. Materialization API — new class `ZCL_ABAPGIT_ORTEC_MAT_STATE` (27 chars)

No existing ORTEC class is a natural fit: `zcl_abapgit_ortec_repo_state` owns
branch pointers (not per-commit certification), `zcl_abapgit_ortec_fetch_neg`
only *consumes* certification for have-negotiation, and
`zcl_abapgit_ortec_obj_store` owns payload, not certificates. A new class
keeps "branch state vs. certification" cleanly separated per the owner
invariant.

```abap
CLASS zcl_abapgit_ortec_mat_state DEFINITION PUBLIC FINAL CREATE PUBLIC.
  PUBLIC SECTION.
    TYPES ty_repo_key    TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.
    TYPES ty_hist_level  TYPE c LENGTH 1.
    TYPES ty_snap_state  TYPE c LENGTH 1.
    TYPES ty_attempt_id  TYPE c LENGTH 32.

    CONSTANTS: BEGIN OF cs_hist_level,
                 unknown        TYPE ty_hist_level VALUE 'U',
                 graph_complete TYPE ty_hist_level VALUE 'G',
                 full_complete  TYPE ty_hist_level VALUE 'F',
               END OF cs_hist_level.
    CONSTANTS: BEGIN OF cs_snap_state,
                 none     TYPE ty_snap_state VALUE 'N',
                 pending  TYPE ty_snap_state VALUE 'P',
                 complete TYPE ty_snap_state VALUE 'C',
                 invalid  TYPE ty_snap_state VALUE 'I',
               END OF cs_snap_state.

    TYPES: BEGIN OF ty_state,
             repo_key    TYPE ty_repo_key,
             commit_sha1 TYPE zif_abapgit_git_definitions=>ty_sha1,
             hist_level  TYPE ty_hist_level,
             snap_state  TYPE ty_snap_state,
             attempt_id  TYPE ty_attempt_id,
             verified_at TYPE timestampl,
             updated_at  TYPE timestampl,
           END OF ty_state.

    "! Single-row keyed read. Returns an initial-state row (hist_level
    "! space, snap_state space) if no row exists — callers must treat that
    "! identically to explicit UNKNOWN/NONE, never as an error.
    CLASS-METHODS get_state
      IMPORTING iv_repo_key     TYPE ty_repo_key
                iv_commit       TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rs_state) TYPE ty_state.

    "! Start (or resume) a materialization attempt for one commit. Creates
    "! the row if absent (hist_level=UNKNOWN, snap_state=NONE). Never
    "! downgrades an existing hist_level. Sets snap_state=PENDING only when
    "! it is currently NONE or INVALID (leaves COMPLETE/PENDING untouched).
    CLASS-METHODS begin_attempt
      IMPORTING iv_repo_key         TYPE ty_repo_key
                iv_commit           TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rv_attempt_id) TYPE ty_attempt_id
      RAISING   zcx_abapgit_ortec_git.

    "! Upgrade hist_level to GRAPH_COMPLETE. Raises if iv_attempt_id does
    "! not match the row's current attempt_id (stale/superseded attempt).
    "! No-op-safe (idempotent) if already GRAPH_COMPLETE or FULL_COMPLETE.
    CLASS-METHODS mark_graph_complete
      IMPORTING iv_repo_key   TYPE ty_repo_key
                iv_commit     TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_attempt_id TYPE ty_attempt_id
      RAISING   zcx_abapgit_ortec_git.

    "! Orchestrator-owned publication boundary: marks the commit's
    "! snapshot COMPLETE and atomically (same LUW, no COMMIT WORK) updates
    "! the branch's materialized commit pointer + denormalized snap_state.
    "! Raises if hist_level < GRAPH_COMPLETE (snapshot cannot precede
    "! graph) or if iv_attempt_id is stale. Issues NO COMMIT WORK — the
    "! calling orchestrator commits once, after this call and any related
    "! object-store writes all succeed, so a crash/rollback before that
    "! single COMMIT WORK discards everything this call did too.
    CLASS-METHODS publish_snapshot_complete
      IMPORTING iv_repo_key    TYPE ty_repo_key
                iv_branch_name TYPE string
                iv_commit      TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_attempt_id  TYPE ty_attempt_id
      RAISING   zcx_abapgit_ortec_git.

    "! Upgrade hist_level to FULL_COMPLETE. Raises unless current
    "! hist_level is already GRAPH_COMPLETE (or FULL_COMPLETE, idempotent).
    "! Does not touch snap_state.
    CLASS-METHODS mark_full_complete
      IMPORTING iv_repo_key   TYPE ty_repo_key
                iv_commit     TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_attempt_id TYPE ty_attempt_id
      RAISING   zcx_abapgit_ortec_git.

    "! Blunt, always-safe reset: hist_level -> UNKNOWN, snap_state ->
    "! INVALID, attempt_id cleared. verified_at is left untouched (last
    "! known-good verification time kept for diagnostics); updated_at is
    "! refreshed. Cascades to every ZAOG_REPO_STATE row of this repo whose
    "! materialized pointer (fetch_commit) equals iv_commit, forcing their
    "! denormalized snap_state to INVALID too (bounded by branch count for
    "! this repo, never repo-object-count).
    CLASS-METHODS invalidate_commit
      IMPORTING iv_repo_key TYPE ty_repo_key
                iv_commit   TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING   zcx_abapgit_ortec_git.

    "! True iff hist_level IN (GRAPH_COMPLETE, FULL_COMPLETE). Replaces
    "! zcl_abapgit_ortec_fetch_neg=>is_commit_complete's tree walk with an
    "! O(1) certificate read (see performance model).
    CLASS-METHODS is_graph_have_eligible
      IMPORTING iv_repo_key   TYPE ty_repo_key
                iv_commit     TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rv_yes) TYPE abap_bool.

    "! True iff hist_level = FULL_COMPLETE.
    CLASS-METHODS is_full_have_eligible
      IMPORTING iv_repo_key   TYPE ty_repo_key
                iv_commit     TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rv_yes) TYPE abap_bool.

    "! Set-based cleanup of orphaned in-flight attempts for one repository
    "! (attempt_id populated, updated_at older than iv_max_age_hours).
    "! Clears attempt_id only; does not alter hist_level/snap_state (a
    "! partially-finished attempt simply becomes retryable at its last
    "! certified level). One bulk UPDATE, never a per-row loop.
    CLASS-METHODS clean_incomplete_attempts
      IMPORTING iv_repo_key       TYPE ty_repo_key
                iv_max_age_hours  TYPE i DEFAULT 24
      RETURNING VALUE(rv_cleaned) TYPE i
      RAISING   zcx_abapgit_ortec_git.
  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.
```

`begin_attempt` rule (no separate "begin snapshot attempt" method exists per
the owner's method list — one attempt id covers both phases):

```
IF row absent: INSERT (hist_level = UNKNOWN, snap_state = NONE).
attempt_id = new uuid; updated_at = now.
IF snap_state IN (NONE, INVALID): snap_state = PENDING.
  " else (COMPLETE or PENDING already) leave snap_state unchanged.
hist_level is NEVER modified here.
```

---

## 4. No auto-backfill of certification (migration rule)

Non-negotiable per owner spec: old `ZAOG_COMMIT_HIST` rows, old
`FETCH_COMMIT`/`CURR_COMMIT` pointers, and `ZAOG_OBJ_STORE` presence must
**never** imply `hist_level IN ('G','F')` or `snap_state = 'C'`.

- The two new `ZAOG_COMMIT_HIST` columns and the one new `ZAOG_REPO_STATE`
  column are additive; every pre-existing row gets them as space/initial on
  DDIC activation.
- Every reader (`get_state`, `is_graph_have_eligible`,
  `is_full_have_eligible`) treats an initial/space value identically to the
  explicit `UNKNOWN`/`NONE` constants — this is enforced structurally by
  comparing against `cs_hist_level-graph_complete`/`-full_complete` and
  `cs_snap_state-complete`, which space never equals.
- One optional, purely cosmetic one-time bulk `UPDATE` at activation
  (`UPDATE zaog_commit_hist SET hist_level = 'U' snap_state = 'N' WHERE
  hist_level = space.`, and the equivalent single-column update on
  `zaog_repo_state`) turns implicit-space into explicit-`UNKNOWN`/`NONE` for
  diagnostic readability. This is a single set-based statement, not a
  per-row loop, and changes no observable behavior (see AC4 below).
- **No batch "verify everything now" pass is part of Slice 1.**
  Certification is lazy/on-demand: existing rows become `GRAPH_COMPLETE` /
  `COMPLETE` only the next time each branch goes through
  `begin_attempt` → `mark_graph_complete` → `publish_snapshot_complete`
  under the Slice 3/5 orchestration. A big-bang verification pass would
  itself require walking every existing commit's full object graph up
  front — exactly the O(N) anti-pattern the reconciliation flagged in
  `get_reachable_objects`/`populate_cache`.

---

## 5. State machine

See `.memory/diagrams/variant_b_flow.mmd`. Reachable combined states are
`(UNKNOWN,NONE)`, `(UNKNOWN,PENDING)`, `(UNKNOWN,INVALID)`,
`(GRAPH_COMPLETE,PENDING)`, `(GRAPH_COMPLETE,COMPLETE)`,
`(FULL_COMPLETE,PENDING)`, `(FULL_COMPLETE,COMPLETE)`. `(GRAPH_COMPLETE or
FULL_COMPLETE, INVALID)` is deliberately **not** reachable in Slice 1:
`invalidate_commit` is blunt and always resets `hist_level` to `UNKNOWN` in
the same call, so `snap_state = INVALID` only ever coexists with
`hist_level = UNKNOWN`. A snapshot-only retry (graph still good, snapshot
materialization failed mid-batch) is represented by staying at
`(GRAPH_COMPLETE, PENDING)` — no `mark_snapshot_invalid` method exists or is
needed, matching the owner's exact 8-method list.

Illegal transitions (raise `zcx_abapgit_ortec_git`, no state change):
`mark_graph_complete`/`publish_snapshot_complete`/`mark_full_complete` with a
stale `attempt_id`; `publish_snapshot_complete` from `hist_level = UNKNOWN`;
`mark_full_complete` from `hist_level = UNKNOWN`.

---

## Mandatory performance model (Slice 1 scope only)

**Expected cardinality:** `ZAOG_COMMIT_HIST` rows per repository are bounded
by distinct commit SHAs ever certified — orders of magnitude smaller than
`ZAOG_OBJ_STORE`'s object count `N` (which also counts every tree and blob).
Typically low hundreds to low thousands of rows per active repository even
for large histories. `ZAOG_REPO_STATE` rows per repository are bounded by
branch count (`B`), realistically single digits to low hundreds.

**SQL-call complexity per operation** (all keyed by the full primary key
`(REPO_KEY, COMMIT_SHA1)` unless noted — never a table scan):

| Method | SQL calls | Complexity |
|---|---|---|
| `get_state` | 1 SELECT SINGLE | O(1) |
| `begin_attempt` | 1 UPDATE, +1 INSERT if row absent | O(1) |
| `mark_graph_complete` | 1 SELECT SINGLE + 1 UPDATE | O(1) |
| `publish_snapshot_complete` | 1 SELECT SINGLE + 1 UPDATE (commit) + 1 UPDATE/INSERT (branch) | O(1) |
| `mark_full_complete` | 1 SELECT SINGLE + 1 UPDATE | O(1) |
| `invalidate_commit` | 1 UPDATE (commit, PK) + 1 UPDATE (branch cascade, WHERE repo_key+fetch_commit) | O(1) + O(B) |
| `is_graph_have_eligible` / `is_full_have_eligible` | 1 SELECT SINGLE | O(1) |
| `clean_incomplete_attempts` | 1 bulk UPDATE (WHERE repo_key + attempt_id <> space + updated_at < cutoff) | O(in-flight attempts for repo), never O(N) |

None of these methods ever selects from or joins `ZAOG_OBJ_STORE`. This is
the central performance change of Slice 1: today
`zcl_abapgit_ortec_fetch_neg=>is_commit_complete` proves have-eligibility by
walking the commit's entire reachable object set
(`get_reachable_sha1s`) — cost proportional to the commit's tree size. After
this design, have-eligibility is one indexed row read, independent of tree
size.

**HTTP-call complexity:** 0. This API is pure persistence; it makes no
network calls.

**Row/byte batch limits:** not applicable — every method (except
`clean_incomplete_attempts`) touches exactly one commit row and, for
`invalidate_commit`, a small bounded set of branch rows for one repo. No
chunking loop is needed (unlike `ZAOG_OBJ_STORE`'s 1000-row SHA1 packages).

**Peak-memory model:** at most one row of CHAR/timestamp fields (well under
200 bytes) resident per call. No XSTRING/blob payload is ever read or held —
consistent with "branch/commit state never owns payload".

**Cache scope:** none for Slice 1. Every read is already O(1); adding a
session cache would add invalidation complexity without a measurable win.
Deferred: if profiling later shows redundant same-LUW lookups, a
hashed-table cache keyed by `(repo_key, commit_sha1)` could reuse
`zcl_abapgit_ortec_obj_store`'s invalidate-on-write pattern — explicitly out
of scope now.

**Transaction owner:** the calling orchestrator (Slice 3/5). Zero `COMMIT
WORK` statements inside `ZCL_ABAPGIT_ORTEC_MAT_STATE` (grep-verifiable at
implementation time). All writes happen in the caller's open LUW; a failed
attempt that never reaches the orchestrator's own `COMMIT WORK` is discarded
by that LUW's rollback. `clean_incomplete_attempts` is the separate,
explicitly-committed recovery path for attempts that die *between* LUWs
(e.g. session termination), not a substitute for LUW-scoped rollback.

**Expected behavior at 1 / 1,000 / 40,000 / 1,000,000 stored objects (N =
`ZAOG_OBJ_STORE` row count for the repo):** identical and constant at every
scale. Every method's cost is structurally independent of `N` because none
of them ever reads `ZAOG_OBJ_STORE` — `get_state`, `begin_attempt`,
`mark_*_complete`, and `is_*_have_eligible` remain 1–3 single-row keyed SQL
calls whether the repository holds 1 object or 1,000,000.

**Large-repository acceptance criteria for this slice:**
- AC1: `get_state`/`is_graph_have_eligible`/`is_full_have_eligible` never
  reference `zaog_obj_store` in their implementation (structural,
  code-review-checkable, independent of any specific object count).
- AC2: zero occurrences of `COMMIT WORK` in
  `zcl_abapgit_ortec_mat_state` (grep-verifiable).
- AC3: `invalidate_commit`'s branch cascade `WHERE` clause always includes
  `repo_key = iv_repo_key`; never a cross-repo scan.
- AC4: the one-time migration `UPDATE` is a single set-based statement
  bounded by a one-time full scan of `ZAOG_COMMIT_HIST`/`ZAOG_REPO_STATE`
  at activation only — a one-time O(existing rows) cost, structurally
  distinct from the steady-state O(1) API calls above.
- AC5: a unit test seeding a legacy-shaped row (space `HIST_LEVEL`/
  `SNAP_STATE`) asserts `is_graph_have_eligible` returns `abap_false` and
  `get_state` does not report `COMPLETE`/`GRAPH_COMPLETE` — proving no
  auto-backfill.

---

## Summary of files this slice would touch (implementation, not yet started)

- `src/ortec/git/zaog_commit_hist.tabl.xml` — append 5 fields.
- `src/ortec/git/zaog_repo_state.tabl.xml` — append 1 field.
- `src/ortec/git/zcl_abapgit_ortec_mat_state.clas.abap` (+`.xml`) — new class.
- No changes to `zcl_abapgit_ortec_obj_store`, `zcl_abapgit_ortec_fetch_neg`,
  or `zcl_abapgit_ortec_repo_state` method bodies in Slice 1 itself (Slice 1
  is schema + new class only); wiring `is_commit_complete` callers to the new
  certificate and moving the `FETCH_COMMIT` write out of `update_after_fetch`
  are Slice 2/3/5 activities once explicit fetch modes exist.

---

## Review resolution (correctness + protocol/persistence, 2026-07-20)

Verdicts: correctness review `APPROVE_WITH_MINOR_REVISIONS`
(`.memory/reviews/variant_b_design_review.md`); protocol/persistence review
`APPROVE_WITH_MINOR_REVISIONS` (appended to `.memory/logs/protocol_persistence.md`).
Slice 1's own additive artifacts (DDIC append + new inert class) are approved
as-is. The following are recorded as binding **preconditions for Slice 2/3**,
not Slice 1 rework:

- **DR-001 (major, Slice 2/3 precondition):** `FETCH_COMMIT` today has two
  live, unverified-value readers beyond `update_after_fetch`'s write:
  `zcl_abapgit_ortec_fastpath`'s "remote tip unchanged" fast-path shortcut
  and `zcl_abapgit_ortec_filter_walk`'s default walk-target lookup. Slice
  2/3 must migrate both readers to consult `is_graph_have_eligible`/
  `SNAP_STATE` (or an explicit interim compatibility rule) in the same
  change that reroutes the writer through `publish_snapshot_complete` —
  otherwise the fast path can keep trusting a pre-certification value.
  `zcl_abapgit_ortec_cache_admin`'s read of `FETCH_COMMIT` is a diagnostic
  report only, off the hot path, and needs no migration.
- **DR-002 (minor, resolved here):** the old `update_after_fetch` write to
  `FETCH_COMMIT` is **disabled/rerouted in Slice 2/3** (the first slice with
  a concrete new call site to replace it), not left coexisting until Slice
  9. Slice 9 only removes now-dead code/tests at that point, never an
  still-active write path.
- **Persistence clarification (non-blocking, resolved here):** the single
  `ATTEMPT_ID` (CHAR32) on `ZAOG_COMMIT_HIST` is the parent/coarse
  materialization-attempt identity only. Slice 8's per-network-attempt
  correlation/session/pack IDs and staged-row visibility must live in a
  separate attempt-scoped staging construct that references this
  `ATTEMPT_ID` as its parent key — they must not be crammed into this one
  column. No Slice 1 schema change is required now; this only constrains
  Slice 8's design.
