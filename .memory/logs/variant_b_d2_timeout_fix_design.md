# Variant B D2 incident follow-up — TIME_OUT target design

```text
PACKET=COMPACT_HANDOFF_V1
TASK=D2-TIMEOUT-TARGET-DESIGN
BASELINE=2111b2887cc4fbf2ee481f753fd4af2c3e5085c4 (SYSTEM_NO_ROLL fix, IT8 retest pending)
STATUS=DESIGN_COMPLETE_AWAITING_REVIEW
```

Baseline verification: `git rev-parse HEAD` = `2111b2887cc4fbf2ee481f753fd4af2c3e5085c4`;
`git status --short` clean; `2111b288...` is HEAD itself (trivially its own
ancestor). `de0f11ce`/`cdc5caed` are both ancestors. The SYSTEM_NO_ROLL fix is
**not yet IT8-validated** (per `.memory/incidents/
variant_b_d2_it8_system_no_roll_timeout.md` §11.3) — this design proceeds
locally per the run brief's explicit permission, and the final retest plan
(§15) validates both fixes together.

## 1. Verified current call path (reconciled against source, not just the incident artifact)

Re-read directly from current source at this baseline (identical to the
incident artifact's own findings; no drift found):

```text
Stage-By-Filter (ZCL_ABAPGIT_GUI_PAGE_STAGE=>INIT_FILES)
  -> ZCL_ABAPGIT_STAGE_LOGIC=>ZIF_ABAPGIT_STAGE_LOGIC~GET
  -> ZCL_ABAPGIT_ORTEC_GIT_FACADE=>RESOLVE_FILTERED_REMOTE
  -> ZCL_ABAPGIT_ORTEC_FILTER_WALK=>GET_REMOTE_FILES_FOR_STAGE
       (cold-branch pre-step: TRY_FILTERED_COMMIT_FETCH - a bounded
       `filter blob:none` fetch, commit+trees only, already correct -
       untouched by this design)
  -> ZCL_ABAPGIT_ORTEC_OBJ_INDEX=>GET_FILES_FOR_FILTER
  -> ZCL_ABAPGIT_ORTEC_OBJ_INDEX=>BUILD_FILES_FROM_ROWS
       - builds lt_sha1s = deduplicated BLOB_SHA1 values from it_rows
         (the filter's own already-narrow row set, genuinely K-sized)
       - calls ZCL_ABAPGIT_ORTEC_MISSING_OBJ=>ENSURE_AVAILABLE(it_sha1s = lt_sha1s)
         exactly ONCE for the whole filtered row set (NOT once per row -
         confirmed by direct re-read, no loop wraps this call)
  -> ZCL_ABAPGIT_ORTEC_MISSING_OBJ=>ENSURE_AVAILABLE
       Step 1: GET_MISSING_SHA1S(it_sha1s)               - bulk, O(K) SQL, correct
       Step 2: UPLOAD_PACK_BY_COMMIT(iv_hash=iv_commit,
               iv_deepen_level=1)                        - THE DEFECT (see §3)
       Step 3: STORE_OBJECTS(et_objects from step 2)     - bulk insert, correct shape
       Step 4: GET_MISSING_SHA1S(it_sha1s) again          - bulk re-check, correct
  -> ZCL_ABAPGIT_GIT_TRANSPORT=>UPLOAD_PACK_BY_COMMIT
  -> ZCL_ABAPGIT_ORTEC_FASTPATH=>UPLOAD_PACK_BY_COMMIT
       - one HTTP info/refs capability probe
       - ZCL_ABAPGIT_ORTEC_FETCH_REQ=>PARSE_CAPABILITIES
       - ZCL_ABAPGIT_ORTEC_FASTPATH=>UPLOAD_PACK(iv_mode=INCREMENTAL_THIN,
         it_hashes=[iv_commit])                          - want=<commit>,
         thin/self-contained/recovery cascade, no filter, no want-list
         bound other than "1 commit"
  -> ZCL_ABAPGIT_ORTEC_FASTPATH=>UPLOAD_PACK
       - IF empty/zero-object response: SERVE_CACHED_WHEN_NOTHING_NEW
         (out of scope for this incident's TIME_OUT trace - only relevant
         to the already-fixed SYSTEM_NO_ROLL path)
       - ELSE: ZCL_ABAPGIT_ORTEC_PACK_STREAM=>DECODE_STREAMING
  -> ZCL_ABAPGIT_ORTEC_PACK_STREAM=>DECODE_STREAMING
       -> DECODE_AND_PERSIST_STREAMING (bounded, D2-owned, confirmed sound)
       -> RESOLVE_STREAMING
            -> PRELOAD_DELTA_ROWS (one bulk chunked warm-up call, confirmed sound)
            -> RESOLVE_ONE_META (recursive, per-row; GET_STAGED_DELTA_OBJECTS
               calls are cache hits when warm - confirmed sound, D1/D2-owned)
            [TIME_OUT fired here in the live dump - NOT because this pipeline
            is unbounded, but because the PACK ITSELF (fetched by Step 2
            above) was disproportionately large: 162,919 objects, 132,963
            of them unresolved deltas, for what should have been a K-object
            top-up]
```

No post-fetch retry of the filtered consumer exists today (`get_files_for_
filter`'s own retry-once-after-index-rebuild is a DIFFERENT retry, for a
stale `zaog_obj_index` snapshot, not for `ensure_available`'s fetch — that
retry is unaffected by and unrelated to this design).

## 2. Caller-required missing-set semantics (per method state)

| Method | Input cardinality | SHA identities actually needed | SHA identities requested from server today | Can fetch unrelated content? |
| --- | --- | --- | --- | --- |
| `build_files_from_rows` | K = distinct `blob_sha1` values across the filter's own already-narrow row set (typically tens to low hundreds for a real Stage-By-Filter path selection) | exactly those K blob SHA1s | (delegates to `ensure_available`) | (delegates) |
| `ensure_available` Step 1/4 | K (same set) | K (unchanged by this design) | N/A (local SQL only) | no |
| `ensure_available` Step 2 (current) | K (the caller's `it_sha1s`) is available but **unused** for the fetch itself — only `iv_commit` is sent | K | `want <iv_commit>` with `deepen=1`, no pathspec, no blob filter — the server returns the commit's ENTIRE reachable graph (commits+trees+blobs), confirmed 162,919 objects for one live incident pack | **yes — this is the confirmed mismatch** |
| `topup_missing_blobs` (walk_prep, second real caller) | K = missing blob SHA1s from a tree-prewarm walk | K | (delegates to `ensure_available`, same defect) | yes (same defect, same fix applies transparently) |

**`CALLER_NEED = bounded deduplicated missing SHA set for filtered paths` is
CONFIRMED.** `CURRENT_REMOTE_ACTION = branch/commit-scoped reachable-graph
fetch` is **CONFIRMED** (measured: 162,919 objects returned for a request
whose caller only needed the specific missing blob subset). The mismatch is
exactly as stated in the run brief.

## 3. Current oversized fetch semantics (root-cause confirmation, no new evidence needed)

Unchanged from the incident artifact's own §4.2/§9 finding, re-verified
against current source at this baseline (identical — `2111b288` did not
touch `zcl_abapgit_ortec_missing_obj.clas.abap` at all, confirmed via
`git show --stat 2111b288`, which lists only `zcl_abapgit_ortec_obj_store
.clas.abap`/`.testclasses.abap` and the two incident memory artifacts).
`ensure_available`'s Step 2 sends `want <iv_commit>` with `iv_deepen_level =
1` and no `filter`/pathspec — git's upload-pack protocol has no per-object
narrowing for a plain commit want; the server walks and serves the commit's
complete tree/blob closure. `it_sha1s` (the caller's real need) is never
placed on the wire at all in the current implementation.

## 4. Live capability evidence

No new live IT8 dump/HTTP probe was performed in this session (the run
brief permits reverifying "only if needed"; the incident artifact's dump
evidence plus the following source-level facts are sufficient to proceed).

| Fact | Classification |
| --- | --- |
| `zcl_abapgit_ortec_fetch_req=>build_request` requires `allow-reachable-sha1-in-want` OR `allow-tip-sha1-in-want` for `MATERIALIZE_BLOBS` mode, else raises `zcx_abapgit_ortec_git` with `mv_unsupported_capability = abap_true` | **advertised-capability requirement, enforced in code** (not inferred — this is a hard gate in the existing serializer, already shipped) |
| `zcl_abapgit_ortec_fastpath=>complete_missing_object` already performs a real, single-object `MATERIALIZE_BLOBS` fetch in production code (currently unreachable — its only caller, `pack_stream=>complete_missing_base`, is a permanent no-op per finding F-2C-001) | **behavior inferred from a previously-designed/tested code path, not from a live request made in this session** |
| `zcl_abapgit_ortec_cold_init=>materialize_tip_snapshot`/`materialize_batch` (Package B, checkpoints B1/B2+B3) already implement a fully adaptive, row- and byte-bounded, oversize-splitting, multi-want `MATERIALIZE_BLOBS` batch fetcher, and Package B is marked `SAP_VALIDATED_COMPLETE` in `.memory/state.md` | **behavior inferred from a previously-validated Package (design/ABAP-Unit level); state.md does not record a specific live capability probe transcript for this exact remote, so treat the capability itself as "believed advertised, not freshly re-confirmed in this session"** |
| Every prior live dump in this incident (both SYSTEM_NO_ROLL and TIME_OUT) reached the server successfully via `INCREMENTAL_THIN`/`INCREMENTAL_SELF_CONTAINED` capability negotiation without any capability error | **advertised capability, confirmed live**: `thin-pack`, `ofs-delta`, `side-band-64k`, `multi_ack` are definitely advertised by this exact remote (these dumps prove real traffic succeeded through `build_request`'s other mode branches) |
| `allow-reachable-sha1-in-want`/`allow-tip-sha1-in-want` specifically: **NOT VERIFIED live in this session** — no dump or trace in the incident evidence exercised `MATERIALIZE_BLOBS` mode against this exact remote | **behavior not verified** — flagged as the one open capability question; Candidate E's structured-rejection path (§6) is the safety net if this remote does not advertise it |

Because the capability-not-verified risk is real, the design (§6/§7) treats
the capability check as **mandatory and structured-failing**, never
optimistically assumed, and the IT8 retest plan (§15) explicitly re-confirms
it against the live remote before declaring the incident closed.

## 5. Candidate comparison

| | Correctness | Capability requirement | HTTP requests | SQL calls | Row/byte bound | Compressed-response memory risk | Pack/decode impact | Txn/attempt behavior | GitHub compatibility | Failure/fallback |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| **A — bounded multi-want MATERIALIZE_BLOBS top-up (reusing `cold_init`'s existing adaptive batcher)** | High — reuses already-designed/tested logic, no new algorithm | `allow-reachable-sha1-in-want`/`allow-tip-sha1-in-want` (checked, structured-fail if absent) | O(ceil(K / adaptive_batch_rows)), never O(K), never O(1)-for-huge-K | O(K) bulk chunked (unchanged D1/D2 pipeline) | Row-bounded (50–1000 adaptive) AND byte-bounded (26 MiB/batch ceiling, oversize-split to 10 levels, solo-object raise) | Bounded per batch, never a single N-object response | Unchanged — same `decode_streaming` pipeline every other mode already uses | None — no `begin_attempt`/certification (matches today's `ensure_available` contract) | GitHub advertises both capabilities per existing `complete_missing_object` doc citation | Structured `zcx_abapgit_ortec_git` (incl. `mv_unsupported_capability`), caller already treats any raise as "no fast-path benefit, fall back safely" |
| **B — path-independent `MATERIALIZE_BLOBS` via existing serializer only (no adaptive batching, single request up to 1000)** | Medium — correctness fine, but no oversize protection for a batch whose blobs happen to be large (could reproduce a smaller-scale memory risk) | Same as A | O(ceil(K/1000)) | Same as A | Row-bounded only (hard 1000 cap), **no byte bound** | Unbounded per-batch response size — re-introduces a scaled-down version of the SYSTEM_NO_ROLL risk class for a batch of large blobs | Same as A | Same as A | Same as A | Same as A, but a single oversized batch has no split recourse — fails outright more often |
| **C — one bounded `filter blob:none` request** | Wrong shape — `filter blob:none` returns commit+trees, deliberately excludes blob content; Stage-By-Filter's actual need here is precisely the blob payloads, not tree structure (already fetched by the existing `try_filtered_commit_fetch` pre-step) | `filter` (already used elsewhere) | O(1) | O(K) unchanged | N/A — this mode cannot deliver blob content at all | N/A | N/A | N/A | N/A | Would never resolve the caller's actual need; rejected |
| **D — exceptional branch recovery (`recovery_branch_full`/full non-thin refetch)** | Correct only as a last resort; as the *normal* top-up path it reproduces the exact 162,919-object defect (no want-list narrowing at all) | none beyond base capabilities | O(1) | O(K) unchanged | **None — this is the current defect's shape**, just without deepen-based ambiguity | Same risk class as the current defect | Same as today | None | Always available | Retained only as `ensure_available`'s own existing `upload_pack_by_commit` thin→self-contained→recovery cascade is a DIFFERENT concern (full-branch pull, not this top-up) — never used here |
| **E — no safe remote top-up available** | Structured failure, no data risk | N/A | 0 | O(K) local only | N/A | N/A | N/A | N/A | Already the exact behavior when capability is absent (raises, caller falls back) — this is not a competing candidate, it is candidate A's own built-in failure mode |

**Selected: Candidate A**, using the already-implemented, already-tested
adaptive batching/oversize-splitting machinery in
`zcl_abapgit_ortec_cold_init` rather than writing a new implementation.
Candidate B is rejected for lacking the byte bound (a real risk given the
incident's own root cause was a payload-size problem, not merely a
want-count problem). Candidate C is the wrong protocol primitive for this
need. Candidate D is retained only for its existing, unrelated role
(`ensure_available` never adopts it). Candidate E is candidate A's
already-built failure mode, not a fallback implementation of its own.

## 6. Selected target design

### 6.1 New public primitive (mechanical extraction, no new algorithm)

Add `zcl_abapgit_ortec_cold_init=>materialize_missing_batches` (27 chars),
a **PUBLIC CLASS-METHODS** that extracts exactly the adaptive-batching WHILE
loop body already proven correct inside `materialize_tip_snapshot` (client
init → `take_next_batch` → `materialize_batch` → `calculate_next_
batch_size` — **not** `chunk_missing_sha1s`, which is confirmed dead
production code today, called only from its own test class; the real
loop's chunking primitive is `take_next_batch`, an index-cursor slice of
an already-deduplicated list, per protocol review finding 1), with the
certification-specific steps removed:

```text
IMPORTING iv_url TYPE string
          iv_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
          it_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt
RAISING   zcx_abapgit_ortec_git
```

Behavior: deduplicate `it_sha1s` (reuse the existing private
`deduplicate_sha1s`); if empty, return with no HTTP call at all; otherwise
`init_materialize_client` once, then the SAME adaptive WHILE loop
`materialize_tip_snapshot` already runs (`take_next_batch` →
`materialize_batch` → `calculate_next_batch_size`), closing the client on
both success and failure exactly as `materialize_tip_snapshot` already
does. **No** `begin_attempt`, **no** `verify_ready_blobs`, **no**
`finalize_snapshot` call — this method's only side effect is object
persistence via `materialize_batch`'s existing `decode_streaming` call
(identical persistence shape to every other `upload_pack`-driven path).
`materialize_tip_snapshot` itself is refactored to call this new method for
its own batching loop (removing the duplicated loop body, not duplicating
logic in two places) — **zero behavior change** to `materialize_tip_
snapshot`'s own external contract (it still begins/verifies/finalizes
exactly as before; only its internal loop is now a call to the extracted
method).

`materialize_batch`, `init_materialize_client`, `take_next_batch`,
`calculate_next_batch_size`, `deduplicate_sha1s` stay **PRIVATE** — no
visibility change needed, since the new public entry point lives in the
SAME class and can call them directly.

### 6.2 `zcl_abapgit_ortec_missing_obj=>ensure_available` rewrite

Replace Step 2 and Step 3 (currently `upload_pack_by_commit` +
`store_objects`) with a single call:

```abap
TRY.
    zcl_abapgit_ortec_cold_init=>materialize_missing_batches(
      iv_url      = iv_url
      iv_repo_key = iv_repo_key
      it_sha1s    = lt_missing ).
  CATCH zcx_abapgit_ortec_git INTO DATA(lx_materialize).
    RAISE EXCEPTION lx_materialize.
ENDTRY.
```

Deliberately `RAISE EXCEPTION lx_materialize` unchanged (not re-wrapped
with a new "Missing-object fetch failed: ..." text as the old
`upload_pack_by_commit` catch did) — this **preserves `mv_unsupported_
capability`** end-to-end so a capability-absent failure is distinguishable
by every caller/test, satisfying the `capability_missing_rejected`
requirement without inventing a new exception field.

Step 1 (`get_missing_sha1s`) and Step 4 (`get_missing_sha1s` re-check) are
**unchanged** — `ensure_available`'s existing "local check → fetch → local
re-check" shape is preserved exactly; only the fetch mechanism inside Step
2 changes. `iv_commit` remains in the signature (both real call sites,
`build_files_from_rows` and `walk_prep=>topup_missing_blobs`, already pass
it; both are class-local diagnostic/interface-stability reasons to keep it
rather than force two unrelated call-site signature changes) but is **no
longer used to scope the fetch** — the class/method doc comment is updated
to say so explicitly, closing the "narrow the contract safely" requirement:
`it_sha1s` is documented as the object identities actually placed on the
wire, and is narrowed from "Object SHA1s" to "**blob** SHA1s the caller
needs to be present" (matching what `MATERIALIZE_BLOBS` mode is named and
designed for, and matching both real callers' actual usage — confirmed by
source read, neither caller ever passes a non-blob SHA1 today).

### 6.3 Why this does not reuse `ensure_available` "unchanged" (run-brief requirement)

`ensure_available`'s **external contract is unchanged** (same signature, same
two call sites, same four-step shape, same raise-on-still-missing
guarantee) — but its **internal fetch mechanism is replaced**, and its
**documented semantics are narrowed** (commit-wide → missing-set-wide, per
§6.2). This satisfies "narrow its contract safely" directly, while also
adding the new explicit bulk top-up API (`materialize_missing_batches`)
requested as an alternative — both are done, since the new API is what
actually performs the correctly-scoped work and `ensure_available` becomes
a thin, still-independently-useful wrapper around it (its own gate logic —
already-buffered short-circuit, URL/opt-in checks — has no equivalent in
the new lower-level primitive and must stay in `ensure_available`).

## 7. Exact request mode/wire constraints

- Mode: `zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-materialize_blobs`
  exclusively for this path (unchanged serializer, no new mode).
- Wants: exactly the batch's own SHA1s (1..`c_materialize_batch_max` = 1000
  per request) — never the target commit, never a ref tip.
- Haves: none (`materialize_blobs` never negotiates haves — unchanged
  serializer behavior, confirmed in `build_request`).
- Shallow/deepen: none — `materialize_blobs` never emits either token
  (confirmed in `build_request`; this mode was never part of the deepen=1
  workaround `ensure_available`'s current doc comment describes, so
  removing that workaround here does not reopen the earlier deepen=0
  incident — that fix's own scope, `upload_pack_by_commit`'s own deepen
  handling for OTHER modes, is untouched).
- Capability requirement: `allow-reachable-sha1-in-want` OR
  `allow-tip-sha1-in-want`, hard-enforced by the existing serializer
  (`build_request` raises `zcx_abapgit_ortec_git`, `mv_unsupported_
  capability = abap_true`, if neither is advertised) — no new capability
  logic needed.

## 8. Row and byte batching

Reused unchanged from `zcl_abapgit_ortec_cold_init` (already-approved
Package B design, `.memory/logs/variant_b_package_b_design.md` §10 per
that class's own doc references):

```text
c_batch_rows_initial      = 500
c_batch_rows_min          = 50
c_batch_rows_max          = 1000  (== c_materialize_batch_max, the wire hard cap)
c_max_batch_growth        = 2     (a successful batch may at most double the next)
c_target_response_bytes   = 16,777,216  (16 MiB, adaptive controller target)
c_max_batch_response_bytes = 26,214,400 (25 MiB, hard per-batch ceiling before split)
c_max_oversize_splits     = 10    (ceil(log2(1000)))
```

No new constants are introduced by this design; `materialize_missing_
batches` inherits these exactly, so its behavior for a 100-object batch, a
1000-object batch, and an oversized single batch is byte-for-byte identical
to `materialize_tip_snapshot`'s own already-reviewed behavior.

## 9. Oversized single-object behavior

Unchanged, reused: `decide_oversize_action` returns `RAISE` (never `SPLIT`)
when a batch has already been reduced to exactly one SHA1 and its response
still exceeds `c_max_batch_response_bytes` — a single pathologically large
blob fails with a clear, structured `zcx_abapgit_ortec_git` rather than
being silently accepted into memory unbounded, and rather than falling back
to a one-request-per-object pattern (there is no lower granularity than
one object). `ensure_available`'s caller (`build_files_from_rows`/`walk_
prep`) already treats any `ensure_available` raise as "no fast-path benefit,
fall back to the caller's own existing safe path" — so a solo oversized
blob degrades to the pre-ORTEC-fastpath behavior for that one file, not to
a crash.

## 10. Attempt/session/transaction behavior

No change to D2's attempt/lock/transaction model. `materialize_missing_
batches` never calls `zcl_abapgit_ortec_mat_state=>begin_attempt`, never
acquires `zcl_abapgit_ortec_pack_dec=>acquire_repo_lock`, and issues no
`COMMIT WORK` of its own — persistence happens entirely inside
`materialize_batch`'s existing `decode_streaming` call, which is the exact
same persistence primitive `upload_pack`/`complete_missing_object` already
use for non-certified, ad-hoc object top-ups. This matches `ensure_
available`'s current (pre-fix) behavior exactly: today's `upload_pack_by_
commit` → `upload_pack` → `decode_streaming` path also does not touch
attempt/lock state. No new transaction boundary is introduced or removed.

## 11. Cache and cleanup behavior

No new cache. `zcl_abapgit_ortec_obj_store`'s existing session cache
(`mt_cache`) is warmed exactly as it already is for any `decode_streaming`-
persisted object (unchanged). No new `ZAOG_OBJ_STORE` status value and no
new cleanup predicate. **Correction (protocol review §5/§6):** a
`MATERIALIZE_BLOBS` response is not guaranteed delta-free — `build_request`
never *requests* `thin-pack`/`ofs-delta`, so no *external* delta bases are
solicited, but the server may still encode returned objects as internal
REF_DELTA/OFS_DELTA entries for compression (the incident's own 162,919-
object pack had 132,963 such entries). Such rows transiently pass through
`decode_and_persist_streaming`'s existing `status = 'D'` split and
`resolve_streaming`'s promotion to `'R'`, exactly like any other
streaming-decoded pack — this is harmless and already handled correctly by
the shared D2 pipeline, but is not a case where `'D'` rows are structurally
impossible. This design adds no new status value, no new cleanup
predicate, and no write to `ZAOG_COMMIT_HIST`/`ZAOG_REPO_STATE` — it reuses
the identical, already-reviewed object-store persistence surface every
other `MATERIALIZE_BLOBS`/`upload_pack` caller already exercises. The
SYSTEM_NO_ROLL fix's own cleanup concerns (orphaned `'D'` rows,
`cleanup_incomplete`) are untouched and unaffected by this design.

## 12. Exact productive file and symbol scope

```text
src/ortec/git/zcl_abapgit_ortec_cold_init.clas.abap
  - ADD public CLASS-METHODS materialize_missing_batches (new)
  - REFACTOR materialize_tip_snapshot: replace its inline WHILE loop with
    one call to materialize_missing_batches( iv_url, iv_repo_key,
    it_sha1s = lt_missing ) — begin_attempt/verify_ready_blobs/
    finalize_snapshot calls stay exactly where they are, unchanged.
  - NO change to materialize_batch, init_materialize_client, chunk_missing_
    sha1s, decide_oversize_action, split_batch_in_half, take_next_batch,
    calculate_next_batch_size, deduplicate_sha1s, verify_batch_objects,
    may_publish_snapshot, finalize_snapshot, acquire_blobless_graph.

src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap
  - REWRITE ensure_available Step 2/3: replace the
    zcl_abapgit_git_transport=>upload_pack_by_commit + store_objects pair
    with one zcl_abapgit_ortec_cold_init=>materialize_missing_batches call,
    propagating its exception unchanged (no re-wrap).
  - UPDATE class-level and method-level ABAP Doc: narrow it_sha1s'
    documented semantics to "blob SHA1s"; remove the deepen=1/SYSTEM_NO_ROLL
    workaround paragraph (no longer applicable - MATERIALIZE_BLOBS never
    sends deepen); document that iv_commit is retained for interface
    stability/diagnostics only and no longer scopes the fetch.
  - NO signature change (both real call sites and all 3 existing "legacy
    aggregate class" gate tests keep compiling and passing unchanged).

Files explicitly NOT touched:
  - zcl_abapgit_ortec_fetch_req.clas.abap (serializer already correct/complete)
  - zcl_abapgit_ortec_fastpath.clas.abap (upload_pack/upload_pack_by_commit/
    complete_missing_object/try_filtered_commit_fetch all unchanged - this
    design does not touch the transport layer at all)
  - zcl_abapgit_ortec_pack_stream.clas.abap, zcl_abapgit_ortec_pack_dec.clas.abap,
    zcl_abapgit_ortec_delta.clas.abap (D1/D2-owned decode/resolve pipeline,
    confirmed sound, untouched)
  - zcl_abapgit_ortec_obj_store.clas.abap/.testclasses.abap (SYSTEM_NO_ROLL
    fix, already committed at 2111b288 - preserved, not touched again)
  - zcl_abapgit_ortec_obj_index.clas.abap, zcl_abapgit_ortec_walk_prep.clas.abap
    (both real ensure_available call sites - unchanged, benefit transparently)
  - zcl_abapgit_ortec_mat_state.clas.abap, zcl_abapgit_ortec_repo_state.clas.abap
    (certification/publication - untouched, this design never certifies)
  - any file under src/git/** (standard abapGit) - no standard hook needed
```

## 13. Exact class-local test plan (all names ≤ 30 characters)

New file `zcl_abapgit_ortec_missing_obj.clas.testclasses.abap` (does not
exist yet — confirmed by file search; the only current `ensure_available`
tests live in the legacy aggregate class `zcl_abapgit_ortec_git_tests
.clas.testclasses.abap`, which per the run brief must not receive new
tests). `zcl_abapgit_ortec_cold_init.clas.testclasses.abap` already exists
and hosts the extraction's own regression tests.

**Corrected per protocol review finding 1**: `chunk_missing_sha1s` is
confirmed dead production code (called only by its own test class, never by
`materialize_tip_snapshot`'s real loop). The tests below target
`take_next_batch`/`calculate_next_batch_size` (the methods the real
`materialize_missing_batches` code path actually runs) instead.

| Test | Host | Chars | What it proves | HTTP needed? |
| --- | --- | --- | --- | --- |
| `missing_set_deduplicated` | `zcl_abapgit_ortec_cold_init.clas.testclasses.abap` | 25 | `deduplicate_sha1s` (called once by `materialize_missing_batches` before the loop) collapses duplicate input SHA1s, preserving first-occurrence order | No — pure |
| `missing_set_row_bounded` | same | 24 | `take_next_batch` on a >1000-entry deduplicated list, called with `iv_max_rows = c_batch_rows_max`, never returns more than `c_materialize_batch_max` entries in one `et_batch`, and `ev_next_index` correctly advances past every returned row | No — pure |
| `missing_set_byte_bounded` | same | 25 | `decide_oversize_action` returns `SPLIT` for a >25 MiB response with batch_size > 1 | No — pure |
| `oversize_object_solo` | same | 21 | `decide_oversize_action` returns `RAISE` (never `SPLIT`) once batch_size = 1 and still oversized | No — pure |
| `no_request_per_object` | same | 22 | `calculate_next_batch_size` never degenerates below `c_batch_rows_min` (50) regardless of a tiny successful-response byte count, i.e. the adaptive controller cannot collapse to "1 object per request" | No — pure |
| `capability_missing_rejected` | `zcl_abapgit_ortec_fetch_req.clas.testclasses.abap` | 27 | ALREADY COVERED — confirmed existing test `materialize_missing_capa_raise` asserts `build_request(materialize_blobs, iv_server_caps without either want-capability)` raises with `mv_unsupported_capability = abap_true`; no duplicate added | No — pure |
| `capability_intersection` | same | 24 | NEW: `build_request` falls back to `allow-tip-sha1-in-want` when ONLY that capability (not `allow-reachable-sha1-in-want`) is advertised — the existing suite only covers "both advertised" (`materialize_wants_and_bounds`) and "neither advertised" (`materialize_missing_capa_raise`); this fills the one missing combination | No — pure |
| `topup_narrows_to_blobs` | `zcl_abapgit_ortec_missing_obj.clas.testclasses.abap` (new file) | 23 | `ensure_available`'s three existing no-network gate tests are ported into the new class-local file unchanged (already-buffered short-circuit, no-URL raise, opt-in-off raise) — confirms the rewrite preserves every pre-existing guarantee | No |
| `unexpected_extra_ignored` | same | 24 | pre-storing unrelated extra objects for the same repo does not affect a subsequent `ensure_available` no-fetch-needed short-circuit for a disjoint `it_sha1s` set | No |
| `attempt_cleanup_preserved` | same | 25 | a successful (no-fetch-needed) `ensure_available` call creates no `ZAOG_COMMIT_HIST`/`ZAOG_REPO_STATE` row — proves the rewritten method still never touches certification state | No |
| `missing_after_topup_raises` | same | 26 | **documented as NOT independently re-testable without a live HTTP mock** — this is Step 4's pre-existing, unchanged re-check logic; behavior-preserving by construction (no code in Step 4 changed). Recorded here as `NOT_APPLICABLE` (mirrors the D1/D2 precedent for genuinely HTTP-mock-blocked cases), not silently omitted. | N/A |
| `no_repo_wide_topup` | same | 18 | **documented as verified by source inspection, not by a live/mocked end-to-end call** — `materialize_missing_batches` never places `iv_commit` (or any value other than the caller's own `it_sha1s`) on the wire; confirmed by the `materialize_missing_batches`/`materialize_batch` source itself taking no commit parameter at all. Recorded as `NOT_APPLICABLE (structural)`, consistent with the same disclosed limitation pattern used throughout this project's prior D2 test-coverage notes. | N/A |
| `retry_is_bounded` | same | 17 | **documented as verified by source inspection**: `ensure_available` calls `materialize_missing_batches` exactly once (no loop, no retry-with-backoff around the whole Step 2); the adaptive batching inside is itself bounded by `take_next_batch`'s finite input size (the deduplicated `it_sha1s` list). Recorded as `NOT_APPLICABLE (structural)`. | N/A |
| `mostly_shared_cold_branch` | same | 25 | local proxy test: pre-store the "already shared" majority of a synthetic blob set for a repo, request `ensure_available` for a set containing both shared (present) and a few genuinely-missing SHA1s pointed at an unreachable URL, and assert the raised failure text's missing-count reflects only the true minority — proving `get_missing_sha1s`' existing bulk-subtract behavior (unchanged) correctly narrows K before any fetch is attempted, matching the "mostly shared" incident scenario's local-side shape | No — local DB only |

`missing_set_row_bounded`/`missing_set_byte_bounded`/`oversize_object_solo`/
`no_request_per_object` exercise **already-existing** `cold_init` private
pure helpers (`take_next_batch`, `decide_oversize_action`, `calculate_next_
batch_size`) that remain unchanged by this design — these tests strengthen
regression coverage for logic this design newly depends on via its one new
public entry point, without duplicating `materialize_tip_snapshot`'s own
existing test suite (verify no exact-name collision before adding; rename
with a `d2timeout_` prefix if any of the four already exist under a
different name).

## 14. Performance model (1 / 1,000 / 40,000 / 1,000,000 stored objects)

All figures are for one `ensure_available` call (one Stage-By-Filter
top-up), not repository-wide, matching the "K objects, not N" invariant.

| Stored objects (N) | Caller's missing set (K) | HTTP requests (after fix) | HTTP requests (before fix) | SQL calls | Rows/bytes fetched (after fix) | Rows/bytes fetched (before fix) |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 0 or 1 | 0 or 1 | O(1) local | K objects | up to the commit's full graph (could equal N even for N=1) |
| 1,000 | ~20 | 1 (fits in one 500-row initial batch) | 1 | O(1) local + O(K) bulk store | ~20 objects, proportional to K | up to 1,000 (N) |
| 40,000 | ~200 (a typical filtered path set) | 1 (fits in one adaptive batch) | 1 | O(1) local + O(K) bulk store | ~200 objects (K) | up to 40,000 (N) — **matches the run brief's mandated 40,000-scale scenario directly** |
| 1,000,000 | ~100 (the run brief's own "100 affected objects with 1,000,000 stored keys" model) | 1 (well within the 500-row initial batch) | 1, but pack payload scales toward N (measured incident case: 162,919 objects for one commit fetch) | O(1) local + O(K) bulk store | ~100 objects (K) | up to and beyond the measured incident's 162,919 — **this is the exact scenario that produced the TIME_OUT** |

The fix changes the "rows/bytes fetched" column from O(N) to O(K) in every
row; HTTP request COUNT stays O(1) per call in the common case and grows
only to O(ceil(K/adaptive_batch_rows)) for a K large enough to need more
than one adaptive batch (K in the tens-of-thousands range, not the typical
Stage-By-Filter case) — never O(K) individual requests, satisfying "no HTTP
request per object."

**Clarification (performance DESIGN_GATE finding M-1):** the table's
40,000-row models `N = 40,000` with a *typical* filtered `K ≈ 200` (one
HTTP request). The distinct case of `K` itself being 40,000 (e.g. an
unusually broad filter, or the run brief's own literal "40,000-object
synthetic path" scenario) requires `ceil(40000 / batch_rows)` sequential
POSTs on one HTTP client — tens of round-trips during the adaptive
controller's ramp-up (starting at `c_batch_rows_initial` = 500, doubling
per successful batch up to `c_batch_rows_max` = 1000), converging toward
~40 once the batch size saturates at 1000. This is expected, bounded
behavior (never `O(K)` individual requests, never unbounded per-batch
bytes) and is not a defect — it is disclosed explicitly here so a future
reader does not mistake the table's "1 HTTP request" row for a claim that
`K = 40,000` also completes in one request.

## 15. Exact IT8 reproduction/retest plan

Reuses the run brief's own required acceptance sequence verbatim (§ "IT8
acceptance test"), plus the SYSTEM_NO_ROLL fix's own still-pending retest:

```text
1. Import both this checkpoint's commit and 2111b288 into IT8 (if 2111b288
   is not yet imported, import it first - both must be present together).
2. Buffer several branches in the large repository (reproduces "several
   other branches already loaded").
3. Select a previously cold branch with mostly shared history (the exact
   branch/commit from the original incident, refs/heads/development/6.0.x
   at commit 81157b1448b4183f38403ec63caad1291a2226a4, is the most direct
   reproduction target if it is still in the same state).
4. Run Stage-By-Filter for a small filtered path set.
5. Confirm no SYSTEM_NO_ROLL (validates 2111b288 together with this fix).
6. Confirm no TIME_OUT (validates this design).
7. Confirm via SM50/ST05/SAT that the request/pack size is proportional to
   the filter's actual missing K, not the branch's full reachable N -
   measure and record: requested missing SHA1 count (from ensure_
   available's own Step 1 result, observable via a temporary breakpoint or
   the raised-exception text on a deliberately-broken URL test), HTTP
   request count, received pack bytes/object count, SQL statement count or
   trace shape, elapsed time, peak memory.
8. Confirm no singleton object-store SQL storm (no repeat of the "very many
   individual database accesses" SM50 observation).
9. Confirm no orphan I/D rows and no failed-attempt publication for this
   run (query ZAOG_OBJ_STORE/ZAOG_COMMIT_HIST for the target repo_key as in
   the original incident's Phase 3, comparing against a fresh baseline).
10. Confirm warm/cold/incremental behavior remains correct (existing
    Package C/D1/D2 functional checks - unaffected by this design, but
    re-verify as part of the same session since this is a shared-code-path
    change).
```

Do not mark `TIMEOUT-10`/the overall incident resolved until this exact
sequence succeeds on live IT8 with measured evidence, per the run brief.

## 16. Package D/Package E ownership impact

```text
This design's productive changes live in zcl_abapgit_ortec_missing_obj
(pre-Package-D utility, Package-E-adjacent per .memory/state.md's own
ENSURE_AVAILABLE flag) and zcl_abapgit_ortec_cold_init (Package B,
SAP_VALIDATED_COMPLETE). Neither file is owned by Package D1/D2's own
scope list (verified against both D1 and D2 implementation maps - neither
lists these two files), so this remains a CROSS_PACKAGE /
PACKAGE_E-pulled-forward fix, consistent with the incident artifact's own
§12 classification for this exact root cause. It does not reopen or modify
any Package D1 (delta resolution) or Package D2 (staged visibility/attempt
isolation) contract, and does not touch Package B's own
acquire_blobless_graph entry point or materialize_tip_snapshot's external
behavior (only its internal loop body is extracted, unchanged in effect).
Package E's own future "prevent normal certified current-tip consumers
from using ENSURE_AVAILABLE" invariant is unaffected either way - this fix
makes the METHOD safe to call at production scale; Package E's separate
concern (whether it SHOULD be called at all for a given consumer class) is
still open and explicitly out of this design's scope.
```

## 17. Rollback and compatibility plan

```text
Both changed methods keep their existing public signatures (materialize_
tip_snapshot: unchanged; ensure_available: unchanged). A revert is a plain
`git revert` of this checkpoint's one commit with no follow-on signature
fixes needed anywhere else in the codebase. Backward compatibility: every
existing caller of ensure_available/materialize_tip_snapshot keeps
compiling and behaving identically for the "nothing missing"/"capability
present, small K" cases already covered by existing tests; the only
observable behavior change is that a large-K top-up that used to fetch the
whole commit graph (and could time out/OOM) now fetches only the K objects
needed (and succeeds, or fails with a structured, capability-specific
error, in bounded time). No DDIC change, no migration, no data format
change - purely a request-shape and internal-call-graph change.
```
