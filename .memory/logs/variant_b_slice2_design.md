# Variant B — Slice 2 design: explicit fetch modes and one request serializer

Status: DESIGN APPROVED — correctness review `APPROVE_WITH_MINOR_REVISIONS`
(resolved), protocol/persistence review `APPROVE_WITH_MINOR_REVISIONS`
(resolved), performance `DESIGN_GATE` `APPROVE_WITH_MINOR_REVISIONS`
(resolved). Ready for senior implementation. No productive code changed yet.
Date: 2026-07-21.
Scope: Slice 2 only (`.github/prompts/variant-b.prompt.md`, topic
`variant-b-partial-clone`). Slice 3 (cold-branch blobless orchestration),
Slice 4 (blob materialization/bulk fetch orchestration) and Slice 5 (wiring
into branch pull/switch decision flow) are explicitly OUT of scope.

Reconciliation basis: `.memory/logs/variant_b_slice2_reconciliation.md`
(current-source evidence, cited by section below) and
`.memory/logs/variant_b_design.md` (Slice 1 design + binding "Review
resolution" preconditions DR-001/DR-002). Slice 1's
`ZCL_ABAPGIT_ORTEC_MAT_STATE` (already implemented, not modified here) is
consumed read-only.

---

## 1. The fetch-mode type

**Decision: a new class, `ZCL_ABAPGIT_ORTEC_FETCH_REQ` (27 chars), owns both
the mode enum and the request serializer.**

Justification against current source (reconciliation §5 "Gaps for Slice 2
design"): no existing class is a natural owner.
`zcl_abapgit_ortec_fastpath` owns HTTP orchestration and the (to-be-replaced)
boolean-flag buffer builder, not a mode taxonomy; `zcl_abapgit_ortec_fetch_neg`
only *consumes* have-eligibility; `zcl_abapgit_git_transport` owns the
standard (non-ORTEC) protocol and must stay unaware of ORTEC-specific modes.
Bundling the enum with the serializer (rather than a separate constants-only
class) mirrors the Slice 1 precedent: `ZCL_ABAPGIT_ORTEC_MAT_STATE` combines
its own `cs_hist_level`/`cs_snap_state` constants with the API that
interprets them in one class, because the constants have no meaning outside
that API's own decision logic. A fetch mode has no meaning outside the
serializer that turns it into wire bytes — same reasoning applies.

```abap
CLASS zcl_abapgit_ortec_fetch_req DEFINITION PUBLIC FINAL CREATE PUBLIC.
  PUBLIC SECTION.
    TYPES ty_fetch_mode TYPE c LENGTH 1.

    CONSTANTS: BEGIN OF cs_fetch_mode,
                 incremental_thin           TYPE ty_fetch_mode VALUE 'T',
                 incremental_self_contained TYPE ty_fetch_mode VALUE 'S',
                 initial_branch_blobless    TYPE ty_fetch_mode VALUE 'B',
                 materialize_blobs          TYPE ty_fetch_mode VALUE 'M',
                 recovery_branch_full       TYPE ty_fetch_mode VALUE 'R',
               END OF cs_fetch_mode.
    ...
ENDCLASS.
```

Single-char codes (not a string enum) deliberately match the existing
`ty_hist_level`/`ty_snap_state` convention — consistent, DDIC-storable if a
future slice ever needs to persist "which mode produced this attempt" for
diagnostics, and grep-friendly.

---

## 2. Request-serializer design

### 2.1 Design principle: the serializer is a pure function

`build_request` takes an explicit mode, already-resolved want/have SHA lists,
and an already-parsed capability string; it makes **zero SQL and zero HTTP
calls**. Certification (which SHAs are eligible to be a `have`) and
capability discovery (what did the server advertise) are the CALLER's job,
done via separate, already-testable primitives (`get_verified_have_commits`,
`parse_capabilities`). This mirrors the existing precedent that
`build_upload_pack_buffer` is deliberately public and pure specifically so
"unit tests can verify the wire-line shape and ordering directly without a
live HTTP client" (fastpath class doc, reconciliation §1.6) — the new
serializer keeps that property while removing the boolean-flag ambiguity.

### 2.2 Signature

```abap
TYPES: BEGIN OF ty_request,
         mode        TYPE ty_fetch_mode,
         buffer      TYPE string,   " assembled pkt-line request body, ready to send
         used_thin   TYPE abap_bool,
         used_filter TYPE string,   " 'blob:none' / 'tree:0' / '' (empty = no filter line)
         have_count  TYPE i,
       END OF ty_request.

CONSTANTS c_materialize_batch_max TYPE i VALUE 100.

CLASS-METHODS build_request
  IMPORTING
    iv_mode            TYPE ty_fetch_mode
    it_want_hashes     TYPE zif_abapgit_git_definitions=>ty_sha1_tt
    it_certified_haves TYPE zif_abapgit_git_definitions=>ty_sha1_tt OPTIONAL
    iv_server_caps     TYPE string OPTIONAL
  RETURNING VALUE(rs_request) TYPE ty_request
  RAISING   zcx_abapgit_ortec_git.

"! Extracts the capability substring (post-NUL, pre-newline) from a raw v1
"! ref-advertisement payload. Replaces the identical logic duplicated today
"! in fetch_tip_commits and try_filtered_commit_fetch (reconciliation
"! §1.5) with a single implementation.
CLASS-METHODS parse_capabilities
  IMPORTING iv_ref_data   TYPE string
  RETURNING VALUE(rv_caps) TYPE string.
```

Callers first call `parse_capabilities` once per connection (as
`fetch_tip_commits`/`try_filtered_commit_fetch` already do inline today),
then `build_request` per attempt. Keeping capability *parsing* separate from
capability *policy* (which mode requires which token) means the wire-tests
(§6) can pass a literal capability string (e.g.
`'multi_ack side-band-64k filter thin-pack ofs-delta'`) without constructing
a fake NUL-delimited ref advertisement.

### 2.3 Per-mode wire-shape decision table

None of the five modes use `shallow` or `deepen` — this is the direct,
structural realization of "delete progressive-deepen ... `deepen N` is never
completeness/correctness" (owner invariant): the new serializer's code path
has no branch that can emit either token, for any mode.

| Mode | want | have | shallow | deepen | filter | thin/ofs-delta | Required server capability |
|---|---|---|---|---|---|---|---|
| `INITIAL_BRANCH_BLOBLESS` | 1 (target tip) | never | never | never | `blob:none`, always | never | `filter` — **hard requirement**; missing → structured unsupported-capability raise, no fallback buffer |
| `INCREMENTAL_THIN` | 1 (target tip) | certified only | never | never | never | advertised only, and only if `it_certified_haves` non-empty (nothing to delta against otherwise) | `thin-pack`/`ofs-delta` — soft: absent → request still built, just without those tokens |
| `INCREMENTAL_SELF_CONTAINED` | 1 (target tip) | certified allowed | never | never | never | never (structurally forced off, independent of caps) | none |
| `MATERIALIZE_BLOBS` | N blob SHAs, `1 <= N <= c_materialize_batch_max` | never | never | never | never | never | `allow-reachable-sha1-in-want` (or `allow-tip-sha1-in-want`) — **hard requirement**; missing → structured unsupported-capability raise |
| `RECOVERY_BRANCH_FULL` | 1 (target tip) | never | never | never | never | never | none (plain v1 want, always legal) |

`MATERIALIZE_BLOBS` exceeding `c_materialize_batch_max` is a caller
programming error (Slice 4's bulk-collection loop must chunk itself, per the
mandatory batching pattern) — `build_request` raises rather than silently
truncating or emitting an oversized want list (AC3, §7).

### 2.4 Structured unsupported-capability result

**Decision: extend `zcx_abapgit_ortec_git`, not a new exception class.**
The class already carries a typed decision flag for exactly this kind of
caller-actionable signal (`mv_retry_without_haves`, read via the existing
`is_retry_without_haves` helper) — adding a second, analogous flag is
consistent and avoids a new global object for a single boolean + a name.

```abap
DATA mv_unsupported_capability TYPE abap_bool READ-ONLY.
DATA mv_missing_capability     TYPE string    READ-ONLY. " e.g. 'filter'

CLASS-METHODS raise_unsupported_capability
  IMPORTING iv_mode        TYPE zcl_abapgit_ortec_fetch_req=>ty_fetch_mode
            iv_capability  TYPE string
  RAISING   zcx_abapgit_ortec_git.
```

`build_request` calls `raise_unsupported_capability` (never falls through
to an unfiltered/unbounded buffer) whenever a mode's hard-required capability
is absent from `iv_server_caps`. Callers (Slice 3 for
`INITIAL_BRANCH_BLOBLESS`, Slice 4 for `MATERIALIZE_BLOBS`) catch this
specific flag to decide their own fallback policy (e.g. "cold-init cannot
proceed on this server" is a user-facing error, not a silent huge fetch) —
Slice 2 only guarantees the signal exists and is structured, not what a
caller does with it.

---

## 3. `zcl_abapgit_ortec_fastpath` — replaced / adapted / deleted

Source line anchors per reconciliation §1.2–§1.6.

**Replaced (call sites rerouted to `zcl_abapgit_ortec_fetch_req=>build_request`):**
- `build_upload_pack_buffer` ([L1297](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1297)) — its `iv_allow_thin`/`iv_force_full`
  boolean pair is replaced end-to-end by an explicit `iv_mode`. The three
  live callers below stop calling it directly.
- `upload_pack` (private helper, [L1268](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1268) region) — still resolves haves via
  `get_verified_have_commits` (§4) when the mode allows haves, then calls
  the serializer instead of `build_upload_pack_buffer`; its own
  `iv_force_full`/`iv_allow_thin` parameters are replaced by `iv_mode`.
- `upload_pack_by_branch` ([L752](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L752)) / `upload_pack_by_commit` ([L928](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L928)) — the
  existing three-tier retry SHAPE is preserved (it already maps cleanly):
  tier 1 `iv_allow_thin = abap_true` → `INCREMENTAL_THIN`; tier 2
  `iv_allow_thin = abap_false` → `INCREMENTAL_SELF_CONTAINED`; tier 3 (today
  the progressive widening loop, [L879-L919](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L879-L919) branch /
  [L989-L1018](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L989-L1018) commit) → **one** `RECOVERY_BRANCH_FULL` attempt, no widening
  loop, no `DO c_progressive_max_steps TIMES`.
- `complete_missing_object` ([L111-L128](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L111-L128) decl, body ~L1055 region) — its
  `iv_force_full = abap_true, iv_deepen_level = 1` single-object call becomes
  `MATERIALIZE_BLOBS` with `it_want_hashes = [iv_sha1]` (a batch of exactly
  one).
- `try_filtered_commit_fetch` ([L462-L500](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L462-L500)) and `fetch_tip_commits`
  ([L323-L383](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L323-L383)) — their duplicated inline capability-parsing +
  `filter blob:none`/`filter tree:0` buffer assembly is replaced by
  `parse_capabilities` + `build_request( iv_mode = initial_branch_blobless )`
  for `try_filtered_commit_fetch` (blob:none, matches the mode's fixed
  filter value); `fetch_tip_commits`'s `filter tree:0` request is a distinct
  shape not covered by the owner's 5-mode list (no blob AND no tree) —
  **left as a local, hand-built buffer in Slice 2** (out of scope: the
  owner spec defines exactly 5 modes and does not include a
  commits-plus-shallow-trees-only variant; inventing a 6th mode is not this
  slice's call to make). This is a deliberate scope boundary, not an
  oversight — flagged for the reviewer.

**Deleted from the live flow now, physically removed in Slice 9 (phased
removal rule, matching DR-002's own precedent in the Slice 1 review):**
- Constants `c_progressive_start_min`, `c_progressive_widen_factor`,
  `c_progressive_max_deepen`, `c_progressive_max_steps` ([L221-L239](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L221-L239)  region).*
- Methods `first_progressive_deepen`, `next_progressive_deepen` (declarations
  in the same region; implementations at [L1283-L1293](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L1283-L1293)).
- The progressive `DO ... TIMES` retry loops themselves ([L879-L919](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L879-L919) /
  [L989-L1018](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L989-L1018)).

Once Slice 2 lands, nothing in the live call graph references these
constants/methods; their unit tests (the "widening formula" tests the
class's own doc comment references) and declarations are removed together
once confirmed dead, in Slice 9 — never deleted while still referenced, and
never left as a second, parallel "still works but unused" code path beyond
that point (same rule DR-002 states for the old `update_after_fetch` writer).

**Unchanged:** `is_retry_without_haves` and `mv_retry_without_haves` — the
tier-escalation *decision* ("did the server lie about nothing-new") is
orthogonal to the wire-shape change and still gates whether tier 3
(`RECOVERY_BRANCH_FULL`) is attempted at all.

\* Exact line numbers shift once earlier edits in the same file land; the
named constants/methods, not the line numbers, are the authoritative
deletion target.

---

## 4. Have-candidate certification

**Decision: `zcl_abapgit_ortec_fetch_neg=>is_commit_complete`'s
*implementation* is swapped to delegate to
`zcl_abapgit_ortec_mat_state=>is_graph_have_eligible`; its name, signature,
and callers (`get_verified_have_commits`) are unchanged.**

Current state (reconciliation §2.1–§2.2): `get_have_commits` produces
*candidates* (repo_state's complete-commit history + a bounded ancestor
walk, capped at 200 — [L98-L132](src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L98-L132)); `is_commit_complete` proves
*eligibility* for each candidate via a tree walk
(`get_reachable_sha1s` + `has_dangling_delta_base`, [L159-L204](src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap#L159-L204)) — cost
proportional to that commit's reachable-object count. This is exactly the
walk Slice 1's `ZCL_ABAPGIT_ORTEC_MAT_STATE` doc already names as the
target for replacement ("Replaces
`zcl_abapgit_ortec_fetch_neg=>is_commit_complete`'s tree walk with an O(1)
certificate read").

```abap
METHOD is_commit_complete.
  IF iv_repo_key IS INITIAL OR iv_commit IS INITIAL.
    RETURN.
  ENDIF.
  rv_yes = zcl_abapgit_ortec_mat_state=>is_graph_have_eligible(
    iv_repo_key = iv_repo_key
    iv_commit   = iv_commit ).
ENDMETHOD.
```

`is_graph_have_eligible` returns true for `hist_level IN (GRAPH_COMPLETE,
FULL_COMPLETE)` — exactly the "certified" set both `INCREMENTAL_THIN` and
`INCREMENTAL_SELF_CONTAINED` are allowed to draw haves from per the owner
spec (neither mode distinguishes graph-only from full-complete for have
eligibility; `is_full_have_eligible` is not needed by any Slice 2 mode and
is left for a future full-blob-completeness check, e.g. Slice 4).
`get_verified_have_commits` (the public entry point `upload_pack`/the new
serializer's caller already uses) is unchanged — it still calls
`get_have_commits` for candidates, then filters through
`is_commit_complete`, which now means "certified by
`ZCL_ABAPGIT_ORTEC_MAT_STATE`" instead of "tree-walk verified". No caller of
`get_verified_have_commits` needs to change for this swap alone.

This is a genuine behavioral change even though the public signature is
identical: a commit that is present and structurally complete in
`ZAOG_OBJ_STORE` but has never been through
`begin_attempt`→`mark_graph_complete` will now be correctly reported as
**not** have-eligible (matching Slice 1 AC5's "no auto-backfill" guarantee)
where the old tree walk would have said yes. This is intentional and is the
entire point of Slice 1 — it must not be treated as a regression to fix.

`zcl_abapgit_ortec_repo_state=>get_complete_commits` is a third, already-gated
`FETCH_COMMIT` reader that feeds this same pipeline (`get_have_commits` →
`get_verified_have_commits`): its candidates are always independently
verified through `is_commit_complete` before being trusted, so this swap
preserves (rather than incidentally leaves open) that reader's safety too.

**Concurrency note (protocol/persistence review):** `is_graph_have_eligible`
is a plain, unlocked `SELECT SINGLE` — a stale-but-committed read is
possible if a concurrent `invalidate_commit` commits between this read and
the caller's use of the result. This cannot resurrect a genuinely-missing
object (an already-fetched blob was never deleted), but it can, in a narrow
window, cause one in-flight fetch attempt to still offer a just-invalidated
commit as a `have`; the server accepts the client's stale `have` as a valid
common base and simply doesn't resend those objects, so the condition
self-corrects on the next attempt/read rather than corrupting state. This is
an inherent property of any O(1) certificate read replacing a
fresh-per-call tree walk, not a defect introduced by this slice.

---

## 5. DR-001 / DR-002 migration timing: deferred to Slice 3, not Slice 2

**Decision: neither `zcl_abapgit_ortec_fastpath`'s `pull_by_branch`
fast-path shortcut ([L679-L686](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap#L679-L686)) nor
`zcl_abapgit_ortec_filter_walk`'s default walk-target lookup
([L118-L124](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap#L118-L124)) is touched in Slice 2. Both migrate in Slice 3, together
with `update_after_fetch`'s writer.**

Justification:

1. **Scope boundary, not laziness.** Slice 2's own "does NOT do" list
   excludes "cold-branch blobless graph acquisition orchestration" (Slice 3)
   and "wiring into branch pull/switch decision flow" (Slice 5).
   `pull_by_branch`'s shortcut is exactly a branch-readiness/orchestration
   decision ("is my cached snapshot still trustworthy for this branch") —
   not a wire-shape concern, and not a method Slice 2 otherwise touches
   (Slice 2's fastpath changes are confined to
   `upload_pack`/`upload_pack_by_branch`/`upload_pack_by_commit`/
   `complete_missing_object`/`try_filtered_commit_fetch`/`fetch_tip_commits`
   — none of which is `pull_by_branch`). `zcl_abapgit_ortec_filter_walk` is
   not touched by Slice 2 at all under this design (§3 lists no filter_walk
   changes). Forcing either migration into Slice 2 would smuggle
   orchestration logic into a slice whose entire purpose is to be a small,
   independently reviewable wire-protocol change.

2. **No trust regression from deferring.** Slice 2 does not change what
   `FETCH_COMMIT` means or how it is written — `update_after_fetch` keeps
   writing it exactly as today. Both readers therefore see **identical**
   behavior before and after Slice 2 lands. The owner invariant "no reader
   ships trusting uncertified state" constrains *new* code from introducing
   a fresh trust gap; it does not require every slice touching a
   neighboring file to retroactively close a pre-existing, already-tracked
   gap (DR-001 is already a named, binding precondition in the Slice 1
   review, not a silent omission). Deferring is therefore safe, not merely
   convenient.

3. **DR-002 specifically cannot be resolved correctly in Slice 2, only
   cosmetically.** `publish_snapshot_complete` requires `hist_level >=
   GRAPH_COMPLETE`, which nothing in Slice 2 ever sets — Slice 2 defines
   *how to shape a request*, not *when a fetched pack is graph-complete*
   (that determination is Slice 3's cold-branch orchestration and Slice 4's
   blob-materialization orchestration). Rerouting `update_after_fetch` to
   call `publish_snapshot_complete` in Slice 2 would force a choice between
   (a) never actually calling it (a no-op rename, not a real DR-002 fix) or
   (b) calling it with a **fabricated** "graph complete" claim the code
   cannot honestly justify yet — which is strictly worse than the current,
   already-acknowledged gap: it would plant a **false** certificate rather
   than leave a **known-uncertified** legacy field, directly violating
   "failed attempts publish no ready objects/branch state/certificates" (a
   fabricated success is not a failed attempt publishing nothing — it is a
   non-attempt publishing something). Slice 3 is the first slice with
   genuine orchestration context (a real `begin_attempt` →
   `mark_graph_complete` → `publish_snapshot_complete` sequence tied to an
   actual cold-fetch outcome) to do this honestly.

4. **Consistent with the Slice 1 review's own wording.** The review names
   the precondition "Slice 2/3" (either), explicitly leaving the choice to
   this design. Slice 3 is the more coherent owner because it is the slice
   that actually acquires and certifies a branch's graph for the first
   time — the natural point to also stop trusting the old field and start
   writing the new one, in one coherent change instead of two
   uncoordinated ones landing in different slices.

**Binding consequence for Slice 3's own design:** Slice 3's design MUST open
by migrating both `FETCH_COMMIT` readers (`pull_by_branch`,
`get_remote_files_for_stage`) to consult `is_graph_have_eligible`/
`SNAP_STATE` (or an explicit, documented interim compatibility rule) in the
*same* change that reroutes `update_after_fetch`'s write through
`publish_snapshot_complete` — this repeats DR-001's original wording
verbatim as a non-negotiable Slice 3 precondition, now with Slice 2's reasons
for not doing it early attached.

---

## 6. Wire-test plan (acceptance criteria for implementation, not the tests)

All assertions are against `zcl_abapgit_ortec_fetch_req=>build_request( ... )-buffer`
(a plain string), following the existing `ltcl_fastpath_protocol` pattern of
asserting on the raw buffer without a live HTTP client.

| Mode | Required tokens present | Forbidden tokens absent | Capability-gate assertion |
|---|---|---|---|
| `INITIAL_BRANCH_BLOBLESS` | exactly one `want <tip>` line; capability list contains `filter`; a `filter blob:none` line | `have `, `shallow `, `deepen`, `thin-pack`, `ofs-delta` | `iv_server_caps` without `filter` → `raise_unsupported_capability` with `mv_missing_capability = 'filter'`; buffer NOT returned |
| `INCREMENTAL_THIN` | one `want <tip>` line; one `have <sha>` line per entry in `it_certified_haves` | `deepen`, `shallow`, `filter` | caps WITH `thin-pack`/`ofs-delta` AND non-empty haves → capability list contains `thin-pack ofs-delta`; caps WITHOUT them, OR empty haves → capability list omits both, no exception raised (soft requirement) |
| `INCREMENTAL_SELF_CONTAINED` | one `want <tip>` line; one `have <sha>` line per entry in `it_certified_haves` | `thin-pack`, `ofs-delta`, `deepen`, `shallow`, `filter` | none (mode never advertises thin regardless of caps) |
| `MATERIALIZE_BLOBS` | one `want <sha>` line per entry in `it_want_hashes` (order preserved) | `have `, `shallow `, `deepen`, `filter`, `thin-pack`, `ofs-delta` | `iv_server_caps` without `allow-reachable-sha1-in-want`/`allow-tip-sha1-in-want` → `raise_unsupported_capability`; `it_want_hashes` count `> c_materialize_batch_max` → raise (distinct message, not the capability exception) |
| `RECOVERY_BRANCH_FULL` | exactly one `want <tip>` line with minimal capability list (`side-band-64k no-progress multi_ack` only) | `have `, `shallow `, `deepen`, `filter`, `thin-pack`, `ofs-delta` | none (always legal) |

Cross-mode assertions:
- For every mode, the assembled buffer never contains the literal substring
  `deepen` (AC4, §7) — one shared assertion helper across all five test
  cases.
- `parse_capabilities` unit tests (moved out of the two duplicated inline
  copies): NUL-not-found input → empty string, no exception; capability
  line with no trailing newline before end-of-string → still parsed
  (matches existing `try_filtered_commit_fetch` behavior, which has no
  `lv_nl_pos > 0` fallback issue since it never guards that branch — flag
  as a pre-existing edge case to preserve, not fix, in Slice 2).

---

## 7. Mandatory performance model

**Expected production cardinality:** one `build_request` call per fetch
*attempt* (1–3 attempts per logical fetch: the existing thin → self-contained
→ recovery cascade, now mode-driven instead of boolean-driven). Per-call
input sizes: `it_want_hashes` is 1 for four of five modes, `1..100` for
`MATERIALIZE_BLOBS`; `it_certified_haves` is `0..200` (existing
`get_have_commits` cap, unchanged by Slice 2).

**SQL-call complexity:**
- `build_request`/`parse_capabilities` themselves: **0 SQL calls** — pure
  string assembly over already-resolved inputs.
- Upstream have-resolution (`get_verified_have_commits`, unchanged
  entry point, changed internals per §4): 1 SELECT for `get_complete_commits`
  + a bounded ancestor-walk BFS (existing, unchanged cost) to build up to 200
  candidates, then up to 200 `is_graph_have_eligible` calls — now **1 SELECT
  SINGLE each (O(1))** instead of a tree walk each. Total: ≤ 1 + ancestor-walk
  cost + 200 SELECT SINGLEs, **independent of repository object count N**
  (strict improvement over today, where each of the 200 checks cost O(commit
  tree size)).
- Follow-on optimization opportunity (not required by Slice 2, flagged for
  Slice 3+): a bulk `is_graph_have_eligible` variant taking a table of
  commits and issuing one `SELECT ... FOR ALL ENTRIES` would cut the 200
  SELECT SINGLEs to 1 query. Out of scope now because 200 keyed SELECT
  SINGLEs is still O(1) each and bounded (never O(N)), matching the
  "batch limits, not correctness" nature of this optimization.

**HTTP-call complexity:** `build_request` makes **0 HTTP calls**. Total HTTP
calls per logical fetch is unchanged from today (1 per attempted tier, up to
3), except `RECOVERY_BRANCH_FULL` is guaranteed a **fresh** `zcl_abapgit_http_client`
instance per the owner spec (still 1 call, just never a reused/pooled
client).

**Row/byte batch limits:** have-list cap 200 (existing, unchanged, enforced
in `get_have_commits`); `MATERIALIZE_BLOBS` want-list cap
`c_materialize_batch_max = 100` (new, enforced inside `build_request` itself
as a hard raise, not just a caller convention — AC3).

**Peak-memory model:** `build_request` holds only the assembled `string`
buffer: at most ~(1 want line + 200 have lines) × ~50 bytes ≈ 10 KB for
`INCREMENTAL_*` modes, or ~100 want lines × ~50 bytes ≈ 5 KB for
`MATERIALIZE_BLOBS`. No XSTRING/blob payload ever enters this class — the
serializer never sees the HTTP response. `RECOVERY_BRANCH_FULL`'s "explicit
memory gate" (owner spec: "the current HTTP layer may materialize the full
response XSTRING") is enforced by the **caller**, before invoking the HTTP
client — `build_request` only marks the returned `ty_request-mode` so the
caller knows which gate applies. **Binding consequence:** whichever slice
wires branch pull/switch decisions (Slice 5 per the top-level spec) MUST
implement this memory gate as a named, reviewable design element before
`RECOVERY_BRANCH_FULL` is exercised against a live HTTP client — this is not
optional orchestration polish, it is the owner's explicit mitigation for a
known full-response-XSTRING materialization risk.

**Cache scope:** none. `build_request`/`parse_capabilities` are pure,
stateless functions of their inputs — same "already O(1)/O(200), no
redundant computation to cache" reasoning as Slice 1's mat_state.

**Transaction owner:** N/A for the serializer (zero `COMMIT WORK`, zero
`MODIFY`/`INSERT`/`UPDATE` — grep-verifiable, AC1). The have-certification
read path (`get_verified_have_commits`/`is_graph_have_eligible`) is
read-only, same as today; no new writer is introduced by Slice 2 (DR-002 is
deferred, §5).

**Expected behavior at 1 / 1,000 / 40,000 / 1,000,000 stored objects (N =
`ZAOG_OBJ_STORE` row count for the repo):** identical and constant at every
scale for `build_request` itself (cost is a function of want-count ≤ 100 and
have-count ≤ 200, never of N). The per-candidate *certification* step is
likewise now N-independent (§4, thanks to Slice 1's certificate replacing
the O(tree size) walk per candidate) — Slice 2 introduces no new N-dependent
cost anywhere in the fetch-mode-relevant path it actually touches, and
removes a pre-existing implicit per-candidate N-dependency (the old
`is_commit_complete` tree walk) from it.

**Correction (performance `DESIGN_GATE` finding, not fully accurate as
originally stated above):** the upstream *candidate-collection* step is
**not** N-independent and Slice 2 does not change that.
`get_have_commits` → `collect_ancestor_haves` (pre-existing, untouched by
this slice) runs `SELECT obj_sha1, obj_data FROM zaog_obj_store WHERE
repo_key = @iv_repo_key AND obj_type = 'commit' AND status = 'R'` —
unbounded by the 100/200 caps (those apply only after this read, when
trimming the resulting BFS) and reading every ready commit object's full
`obj_data` for the repository. This is a real, repository-commit-count-
dependent cost on the same have-resolution hot path `build_request` depends
on for `it_certified_haves`, executed on every `upload_pack` call that
resolves haves (both `INCREMENTAL_THIN` and `INCREMENTAL_SELF_CONTAINED`,
up to 2× per logical fetch). It predates Slice 2, is outside this slice's
touched-file list (§8), and is not a regression this slice introduces — but
it must not be described as already solved. **Tracked as a Slice 3
candidate:** Slice 3 already touches the have-negotiation/graph-acquisition
area and is the natural place to bound or eliminate this unbounded read; if
Slice 3's own scope cannot absorb it, it must be named explicitly in a later
slice's design rather than silently carried forward again.

**Large-repository acceptance criteria:**
- AC1: `build_request`/`parse_capabilities` contain zero SQL statements and
  zero HTTP-client calls (structural, grep-verifiable).
- AC2: `INITIAL_BRANCH_BLOBLESS`'s code path has no branch capable of
  appending a `have`/`shallow`/`deepen` token — this is what makes "no
  silently issue a huge unfiltered fetch" durable rather than incidental
  (a future maintenance edit cannot accidentally reintroduce it without
  touching a completely different mode's branch).
- AC3: `MATERIALIZE_BLOBS` raises when `it_want_hashes` exceeds
  `c_materialize_batch_max`, rather than silently truncating or emitting an
  oversized want list — the mode-level enforcement backstop even if a
  Slice 4 caller's own chunking has a bug.
- AC4: none of the five modes' decision branches reference `deepen` in any
  form (no constant, no widening loop, no conditional emission) —
  structurally realizes "`deepen N` is never completeness/correctness".
- AC5: a unit test asserts that `INITIAL_BRANCH_BLOBLESS` with
  `iv_server_caps` lacking `filter` raises
  `mv_unsupported_capability = abap_true` rather than falling through to a
  plain want-only buffer (the exact "do not silently issue a huge unfiltered
  fetch" requirement, made test-enforceable).
- AC6: a unit test seeds `it_certified_haves` from a commit whose
  `ZCL_ABAPGIT_ORTEC_MAT_STATE` state is `hist_level = UNKNOWN` (never
  certified) and asserts that commit is absent from
  `get_verified_have_commits`'s result — proving §4's swap actually enforces
  certification rather than trusting object-store presence alone (extends
  Slice 1's own AC5 "no auto-backfill" guarantee into the negotiation path).

---

## 8. Summary of files this slice would touch (implementation not yet started)

- `src/ortec/git/zcl_abapgit_ortec_fetch_req.clas.abap` (+`.xml`) — **new**
  class: fetch-mode enum, `build_request`, `parse_capabilities`,
  `c_materialize_batch_max`.
- `src/ortec/git/zcx_abapgit_ortec_git.clas.abap` — add
  `mv_unsupported_capability`, `mv_missing_capability`,
  `raise_unsupported_capability`.
- `src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap` — reroute
  `upload_pack`, `upload_pack_by_branch`, `upload_pack_by_commit`,
  `complete_missing_object`, `try_filtered_commit_fetch` to the new
  serializer + explicit modes; remove the progressive-deepen retry loops
  from the live flow (constants/methods themselves deleted in Slice 9, §3).
  `fetch_tip_commits` is explicitly NOT rerouted (its `filter tree:0` shape
  has no matching mode in the owner's 5-mode list, §3). Also resets
  `zcl_abapgit_ortec_pack_stream`'s decode-local completion budget
  (`reset_completion_budget( )` or equivalent) immediately before the
  `RECOVERY_BRANCH_FULL` tier's attempt, in addition to the existing
  once-per-call reset (DR-004, review resolution below).
- `src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap` — `is_commit_complete`
  body swapped to delegate to
  `zcl_abapgit_ortec_mat_state=>is_graph_have_eligible` (§4). No signature
  change; `get_have_commits`/`get_verified_have_commits` untouched.
- New/updated test include(s) for the wire-shape assertions in §6, likely
  alongside the existing `ltcl_fastpath_protocol` pattern (exact file TBD at
  implementation time).

**Explicitly NOT touched in Slice 2** (per §5 and the slice's own out-of-scope
list): `zcl_abapgit_ortec_filter_walk.clas.abap`,
`zcl_abapgit_ortec_repo_state.clas.abap` (DR-001/DR-002 readers/writer,
deferred to Slice 3), `zcl_abapgit_ortec_mat_state.clas.abap` (consumed
read-only), `zcl_abapgit_git_transport.clas.abap` (standard non-ORTEC path).

---

## Review resolution (correctness review, 2026-07-21)

Verdict: `APPROVE_WITH_MINOR_REVISIONS`
(`.memory/reviews/variant_b_slice2_design_review.md`), 3 required revisions
(DR-001, DR-002, DR-004) and 2 optional (DR-003, DR-005). Resolved below.
No change to the approved architecture: the mode enum, the serializer class,
the DR-001/DR-002-from-Slice-1 deferral-to-Slice-3 decision (§5), and the
`is_commit_complete` swap (§4) all stand exactly as designed.

- **DR-001 (major, resolved here — deferred to Slice 3, not special-cased in
  Slice 2):** `try_filtered_commit_fetch`'s existing broad
  `CATCH zcx_abapgit_exception zcx_abapgit_ortec_git. CLEAR rv_applicable.`
  is left as-is in Slice 2; `mv_unsupported_capability` is built and
  unit-tested in isolation (AC5, §6) but is **not** plumbed past this catch
  this slice. Special-casing the flag inside `try_filtered_commit_fetch`
  alone cannot actually deliver end-to-end, caller-actionable surfacing: its
  only caller, `zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage`,
  is explicitly untouched in Slice 2 (§5), so a re-raise would propagate an
  exception into a caller with no matching handling for this specific case
  beyond its own current behavior, and a distinct returning parameter would
  be unread by that same untouched caller — either sub-option quietly
  requires editing `filter_walk` in the same breath, which would violate the
  slice boundary §5 already draws. Per the review's own finding, net
  behavior at this call site is unchanged from today either way (today's
  code already silently falls back to `get_files_remote` on missing filter
  capability), so deferring is safe, not merely convenient — the same
  standard §5 already applies to the two Slice-1-derived readers.
  **Binding consequence for Slice 3:** Slice 3's design MUST, as part of
  migrating `get_remote_files_for_stage` off `FETCH_COMMIT` (§5's existing
  binding consequence), also decide and implement how
  `try_filtered_commit_fetch`'s `mv_unsupported_capability` signal reaches
  that caller (re-raise past a narrowed catch, or a distinct returning
  parameter) instead of remaining folded into the current blanket
  "not applicable" swallow.
- **DR-002 (major, resolved here — binding consequence added, no Slice 2
  code change):** §3's existing scope-boundary reasoning for leaving
  `fetch_tip_commits` as a hand-built `deepen 1`/`filter tree:0` buffer
  stands unchanged. What was missing is a named follow-up, now added.
  **Binding consequence for Slice 3's own design** (analogous to §5's
  existing paragraph): Slice 3 must either (a) route `fetch_tip_commits`
  through `zcl_abapgit_ortec_fetch_req=>build_request`, extending the mode
  table with a 6th mode only if a commits-plus-shallow-trees-only shape is
  genuinely still required once Slice 3's cold-branch orchestration is
  designed, or (b) explicitly re-justify, in Slice 3's own design document,
  why `fetch_tip_commits` continues to exist as a hand-built buffer that
  emits `deepen`. This repeats the review's own DR-002 wording as a
  non-negotiable Slice 3 precondition, the same way §5 already treats the
  two Slice-1-derived preconditions — it must not become a second,
  permanently untracked legacy path.
- **DR-004 (major, resolved here — split decision, one part in-scope, one
  part deferred):**
  - **Decode-local cache reset — in scope for Slice 2 (§8 updated).**
    `zcl_abapgit_ortec_pack_stream=>reset_completion_budget( )` (or
    equivalent) is called at the `RECOVERY_BRANCH_FULL` call site,
    immediately before that tier's `build_request`/HTTP-client attempt, in
    addition to (not instead of) whatever reset already happens once per
    whole `upload_pack_by_branch`/`by_commit` call today. This does not
    depend on any Slice 3 concept — `reset_completion_budget` already
    exists and is callable today; the fix is purely about which call site
    invokes it, which is squarely inside §3's fastpath rewiring. Deciding
    this together with the genuinely-blocked half below, just for
    consistency, would have delayed a low-risk fix for no reason.
  - **Attempt/session/pack IDs — deferred to Slice 3, explicitly.** These
    require a caller-visible attempt/session identity (Slice 1's
    `begin_attempt` result, or an equivalent correlation ID) to be threaded
    through `upload_pack_by_branch`/`by_commit`'s tier-retry call path; no
    such identity exists anywhere in that call path today, and wiring one in
    is orchestration work indistinguishable from the "cold-branch blobless
    orchestration" Slice 2's own out-of-scope list already excludes.
    Fabricating a fresh-looking ID with no real orchestration context to
    attach it to would be cosmetic, not a real fix — the same category of
    reasoning §5 already applies to why `publish_snapshot_complete` cannot
    be honestly called before Slice 3. **Binding consequence for Slice 3:**
    Slice 3's design MUST assign fresh attempt/session/pack IDs to the
    `RECOVERY_BRANCH_FULL` tier as part of its orchestration design, not
    merely inherit whatever identity (if any) the failed thin/self-contained
    tiers used.
- **DR-003 (minor, resolved here):** `zcl_abapgit_ortec_repo_state=>get_complete_commits`
  is a third, already-gated `FETCH_COMMIT` touchpoint (its candidates feed
  `get_have_commits` → `get_verified_have_commits`) whose safety is
  preserved, not incidentally, by §4's `is_commit_complete` swap — to be read
  as an addition to §4's evidence base alongside the two readers DR-001
  already names.
- **DR-005 (minor, resolved here):** `shallow` line emission is intentionally
  dropped for every mode in §2.3's wire-shape table because certified-have
  negotiation (Slice 1's `ZCL_ABAPGIT_ORTEC_MAT_STATE` certificate) already
  supersedes shallow-clone semantics for telling the server which history
  boundary is trustworthy — this is a deliberate removal distinct from,
  though motivated by the same shift away from progressive-deepen as, the
  `deepen` removal — to be read as an addition to §2.3/§3.

## Review resolution (protocol/persistence review, 2026-07-21)

Verdict: `APPROVE_WITH_MINOR_REVISIONS` (appended to
`.memory/logs/protocol_persistence.md`), documentation-only, no schema or
architecture change. Resolved:

- The `is_commit_complete` stale-read concurrency window is now documented
  explicitly in §4 above (self-correcting, not a correctness defect).
- Confirmed no DDIC object is introduced or changed by this slice (new
  class, exception attribute additions, and two method-body swaps only).
- Confirmed `reset_completion_budget()` is a single stateless counter reset
  with no dependency a still-running prior tier could need, since the three
  fastpath tiers execute strictly sequentially — the DR-004 decode-local
  cache reset placement (§8) is safe as designed.
- `zcx_abapgit_ortec_git`'s decision flags (`mv_is_corruption`,
  `mv_retry_without_haves`, and the new `mv_unsupported_capability`) remain
  safe as independent, convention-enforced (one flag per dedicated `raise*`
  factory) rather than structurally-exclusive attributes — matches the
  class's existing pattern, no change required.

**Risk flagged for Michael's review (not a design defect, not blocking):**
`MATERIALIZE_BLOBS`'s blob-SHA1-want pattern is protocol-correct and
correctly capability-gated, but whether the actual target git server(s)
honor arbitrary *blob* SHA1 wants in practice (versus commit/tag wants) once
that capability is advertised cannot be confirmed by source review alone.
Recommend a live-system smoke test before Slice 4 (the first slice expected
to exercise this mode for real) relies on it; Slice 2 itself never sends a
`MATERIALIZE_BLOBS` request from any currently-wired call site, so this does
not block Slice 2.
