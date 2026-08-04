# Progress Feedback Review — ORTEC Git Fetch Path

Scope: `zcl_abapgit_ortec_fastpath`, `zcl_abapgit_ortec_pack_stream`,
`zcl_abapgit_ortec_pack_dec`, `zcl_abapgit_ortec_fetch_req` (read-only),
`zif_abapgit_progress`/`zcl_abapgit_progress` (read-only, standard, not modified).

## 1. Call-chain map

```
zcl_abapgit_ortec_fastpath=>upload_pack_by_branch / upload_pack_by_commit   (orchestration entry)
  -> pull_by_branch                     (cache/resume check - branch entry only)
       -> zcl_abapgit_ortec_pack_raw=>find_active_session / resume_decode  (resume path)
       -> zcl_abapgit_ortec_obj_store=>get_reachable_objects              (Phase 3 reconstruction)
  -> zcl_abapgit_git_transport=>find_branch_ortec                        (HTTP connect, ref advertisement)
  -> zcl_abapgit_ortec_fetch_req=>parse_capabilities                     (parse caps, no HTTP/SQL)
  -> upload_pack (PRIVATE)                                               (per-tier orchestration)
       -> zcl_abapgit_ortec_have_policy=>get_certified_haves             (SQL, cheap, scoped)
       -> zcl_abapgit_ortec_fetch_req=>build_request                     (pure serializer)
       -> io_client->send_receive_close                                 (HTTP send+receive, synchronous/blocking)
       -> parse (pkt-line / side-band demux)                             (in-memory)
       -> zcl_abapgit_ortec_pack_dec=>peek_object_count                  (header peek, no SQL)
       -> [nothing-new] serve_cached_when_nothing_new                    (SQL: get_reachable_objects)
       -> [decode]  zcl_abapgit_ortec_pack_stream=>decode_streaming
             -> decode_and_persist_streaming   (object-by-object decode + batched persist, SQL every c_batch_size)
             -> resolve_streaming              (in-pack fixpoint passes, then bulk external-base load, then final pass)
             -> commit-extraction loop         (SQL: get_object per commit, usually 1)
       -> [fallback, only via try_filtered_commit_fetch] zcl_abapgit_ortec_pack_dec=>decode_and_persist
             -> resumable_decode               (per-object decode+persist, batched, delta resolution via zcl_abapgit_ortec_delta)
  -> persist_pull_result (post-pull hook, called by caller/porcelain, not inside upload_pack_by_branch itself)
       -> persist_missing_objects, certify_fetched_commit, update_after_fetch, COMMIT WORK
```

Retry cascade inside `upload_pack_by_branch`/`upload_pack_by_commit` (unchanged control flow):
`INCREMENTAL_THIN` -> (on failure) `INCREMENTAL_SELF_CONTAINED` -> (on retry-without-haves signal)
`RECOVERY_BRANCH_FULL`. Each tier opens a fresh HTTP client and calls `upload_pack` again.

## 2. Existing progress calls (inventory)

| Location | Total | Current | Timing | Note |
|---|---|---|---|---|
| `upload_pack_by_branch` (top) | `get_instance(1)` | 2 | before any work | text "Fetch remote files (Fastpath)" - misleading, fires even on a pure cache hit |
| `pull_by_branch` (Phase 1b resume success) | `get_instance(1)` (2nd call, same singleton) | 1 | after full resume+tree-walk, before return | resets throttle timer of the outer call |
| `pull_by_branch` (Phase 3 reconstruction) | `get_instance(1)` (3rd call) | 1 | after tree-walk, before return | same reset problem |
| `upload_pack` (nothing-new branch) | `get_instance(1)` (Nth call) | 1 | after `serve_cached_when_nothing_new` | completion-only |
| `upload_pack` (streaming/fallback success) | `get_instance(1)` (Nth call) | 1 | after full decode+resolve+commit-extract | completion-only, two variants (streaming vs fallback wording) |

`upload_pack_by_commit` has **no** progress calls at all today. Nothing inside
`decode_and_persist_streaming`/`resolve_streaming`/`decode_and_persist`/`resumable_decode`
reports progress today - the entire decode/persist/resolve body (the actual long-running,
CPU+DB-bound part for large repos) is currently silent. Every existing call is
**completion-only**, confirming observation 1/2 in the task brief.

**Confirmed defect (design constraint 6):** `zcl_abapgit_progress=>get_instance()` is a
process-global singleton (`gi_progress` CLASS-DATA). Every call to `get_instance(iv_total)`
invokes `set_total`, which unconditionally clears the throttle window
(`mv_cv_time_next`/`mv_cv_datum_next`) in addition to (re)setting `mv_total`. `pull_by_branch`
calling `get_instance(1)` a second/third time *after* `upload_pack_by_branch` already obtained
an instance does not corrupt data (both pass total=1), but it does silently reset the 2-second
display-throttle window set up by the outer caller - a real, if minor, instance of exactly the
"nested get_instance resets another caller's state" risk called out in the brief. With this
change's ownership model (see §4) this is eliminated by construction: `get_instance()` is
called at most once per top-level fetch attempt.

## 3. Phase matrix (new instrumentation)

| Class.Method | Phase text | Current value source | Total | Cadence | Why useful | Overhead |
|---|---|---|---|---|---|---|
| fastpath.pull_by_branch | `Git: checking local cache` | constant 1 | n/a | once, at entry (after switch check) | first user-visible sign of activity for the common resume/reconstruct path | 1 FM-gated call |
| fastpath.upload_pack_by_branch/_commit | `Git: requesting objects from remote (thin\|self-contained\|recovery attempt)` | constant 1 | n/a | once per retry tier actually entered (max 3) | makes silent retry escalation visible without changing control flow | 1 call per tier |
| fastpath.upload_pack (private) | `Git: preparing fetch request` | constant 1 | n/a | once, before build_request | | 1 call |
| fastpath.upload_pack | `Git: receiving remote response` (reported right before the blocking `send_receive_close`, since no incremental HTTP hook exists) | constant 1 | n/a | once | HTTP round-trip is the single largest unaccounted wall-clock block; we cannot subdivide it without touching standard `zcl_abapgit_http_client` (out of scope) | 1 call |
| fastpath.upload_pack | `Git: parsing server response` | constant 1 | n/a | once, after send_receive_close, before `parse()` | | 1 call |
| fastpath.upload_pack / pull_by_branch | `Git: completed (<n> objects, <duration>[, qualifier])` | `lines(rt_objects)`/duration timer (existing) | n/a | once, replaces/consolidates 5 existing ad hoc completion texts | preserves existing info, unifies vocabulary | 0 extra calls (rewords existing ones) |
| pack_stream.decode_and_persist_streaming | `Git: decoding pack object <n> of <total>` | `lv_uindex` | `lv_objects` (parsed pack header - truthful) | first object, last object, every `c_batch_size` (500) objects | truthful, bounded; aligns with existing batch-flush boundary (no new counting) | O(total/500) calls, each already-throttled by `zcl_abapgit_progress` |
| pack_stream.decode_and_persist_streaming | `Git: persisting decoded objects <n> of <total>` | `lv_uindex` | `lv_objects` | at each `flush_batch` call (mid-loop + final) | distinguishes "decoded in memory" from "durably flushed" per requirement B | same as above |
| pack_stream.decode_and_persist_streaming | `Git: validating pack` | `lv_objects` | n/a | once, before trailer SHA1 check | | 1 call |
| pack_stream.decode_and_persist_streaming | `Git: finalizing object store` | `lv_objects` | n/a | once, before the two promotion UPDATEs | | 1 call |
| pack_stream.resolve_streaming | `Git: resolving in-pack deltas (pass <p>, <remaining> remaining)` | pass counter (new local) | `remaining` = `count_unresolved(ct_meta)` (existing helper, O(n) in-memory, no SQL) | once per fixpoint pass (bounded by max chain depth, not object count) | | O(passes) calls, each O(n) in-memory count reused from an already-existing diagnostic helper |
| pack_stream.resolve_streaming | `Git: loading <count> external delta bases` | `lines(lt_external_bases)` (already computed, deduplicated) | `UNKNOWN` (not a percentage-bearing phase) | once, before `bulk_resolve_external_bases` | matches the exact dedup count already computed | 1 call |
| pack_stream.resolve_streaming | `Git: resolving external deltas (<remaining> remaining)` | `count_unresolved(ct_meta)` | `UNKNOWN` | once, before pass 2 (only if remaining > 0) | | 1 call, O(n) in-memory |
| pack_stream.decode_streaming | `Git: extracting commits` | `lines(lt_meta)` | n/a | once, before the commit-extraction loop | | 1 call |
| pack_dec.decode_and_persist | `Git: decoding pack object 0 of <total>` / completion | `lv_obj_count` | `lv_obj_count` | once at start, once at end | see §5 for why per-object cadence is deliberately NOT added here | 2 calls |

All "remaining"/"total" values are counters or table sizes already held in memory
(`lv_objects`, `lt_external_bases`, `ct_meta` size via the pre-existing `count_unresolved`
helper) - no new SQL, no new HTTP, no new object-store reads, no repository-wide scans.

## 4. Progress-ownership design

**Orchestration-owned lifecycle with an injected optional `REF TO zif_abapgit_progress`.**

- `zcl_abapgit_ortec_fastpath=>upload_pack_by_branch` and `=>upload_pack_by_commit` are the
  chosen **owners**: each calls `zcl_abapgit_progress=>get_instance(1)` **exactly once**, at
  the very top, and threads that single reference down through every call it makes for the
  rest of that one fetch attempt (`pull_by_branch`, every tier's `upload_pack` call).
- `pull_by_branch` gains an **OPTIONAL** `ii_progress` importing parameter. If the caller
  supplies one (the owner case), it is reused as-is and `get_instance` is **never** called
  again inside `pull_by_branch`. If not supplied (any other/direct caller, preserving 100%
  backward compatibility since the parameter is optional), `pull_by_branch` falls back to its
  own `get_instance(1)` call exactly as today - existing external callers keep their current
  behavior unchanged.
- `upload_pack` (private) gains the same optional `ii_progress` parameter and never calls
  `get_instance` itself anymore (previously it did, twice, on two different completion
  branches) - the diff removes both, replacing them with the injected reference.
- `zcl_abapgit_ortec_pack_stream=>decode_streaming` / `decode_and_persist_streaming` /
  `resolve_streaming` all gain the same optional `ii_progress` parameter, propagated
  end-to-end. None of them ever call `get_instance` - only the top-level orchestrator does.
- A tiny private static helper, `report_progress( ii_progress iv_current iv_text )`, is added
  independently to `zcl_abapgit_ortec_fastpath` and `zcl_abapgit_ortec_pack_stream` (duplicated
  ~8 lines rather than introducing a new shared class, to avoid any cross-class coupling for a
  single-purpose safety wrapper). It no-ops if `ii_progress` is not bound, and swallows
  `zcx_abapgit_exception` from `show()` in a `TRY/CATCH` with an empty handler - a progress
  *display* failure must never abort or alter a decode/resolve/fetch result (design constraint
  8: the display call itself is not part of "the original exception contract"; the real
  decode/resolve exceptions are never touched by this helper and propagate completely
  unchanged, verified by test `progress_does_not_swallow_failure` in §implementation).

This design satisfies constraint 1 (one clear owner, verified: exactly one `get_instance` call
per top-level fetch attempt after this change, versus up to 3 today), constraint 2 (fully
optional/behavior-neutral: every new parameter is OPTIONAL, unsupplied callers see zero
behavior change), and constraint 5 (bounded cadence riding on `zcl_abapgit_progress`'s own
already-existing 2-second real-time throttle inside `show()` - no new timer/SQL/scan is added
by this change).

## 5. Files and methods changed

- `zcl_abapgit_ortec_fastpath.clas.abap`: `pull_by_branch` (signature + 2 internal
  `get_instance` calls replaced), `upload_pack_by_branch`, `upload_pack_by_commit`,
  `upload_pack` (signature + phase calls + 2 internal `get_instance` calls removed), new
  private `report_progress`.
- `zcl_abapgit_ortec_pack_stream.clas.abap`: `decode_streaming`, `decode_and_persist_streaming`,
  `resolve_streaming` (all gain optional `ii_progress` + phase calls), new private
  `report_progress`.
- `zcl_abapgit_ortec_pack_dec.clas.abap`: `decode_and_persist` gains optional `ii_progress` +
  a start/completion report only (see below for why `resumable_decode` is NOT touched). New
  private `report_progress`.
- Testclasses: `zcl_abapgit_ortec_pack_stream.clas.testclasses.abap`,
  `zcl_abapgit_ortec_fastpath.clas.testclasses.abap` gain new, additional `LOCAL FRIENDS` test
  classes (no changes to any existing test method). No change to the legacy aggregate
  `zcl_abapgit_ortec_git_tests`.

### D. `zcl_abapgit_ortec_pack_dec` reachability finding

`decode_and_persist`/`resumable_decode` are **not fully dead**: `upload_pack`'s own streaming
failure path was already changed (2026-07-17 diagnostic commit, see the TODO comment still in
the source) to **re-raise instead of falling back** to the old decoder - so the old decoder is
unreachable from the *main* fetch path today. It remains reachable from exactly one other
call site: `try_filtered_commit_fetch`'s own inner `CATCH zcx_abapgit_ortec_git` fallback,
which is a best-effort, `filter blob:none` structural probe (commit+trees only, no blobs) used
by the branch picker/stage flow, not a "long-running fetch" in the sense this task targets.

Given that (a) it is reachable, so the brief's "if reachable, add equivalent feedback" applies,
but (b) `try_filtered_commit_fetch` itself has and needs no progress-owning caller today, and
(c) `resumable_decode` is a large, delta-resolution-heavy, historically crash-sensitive
(SYSTEM_NO_ROLL) method per its own extensive in-source postmortem comments, this review's
disposition is: add the optional `ii_progress` parameter and a **start/completion-only** report
to the public `decode_and_persist` entry point (consistent ownership model, zero risk to the
crash-sensitive inner loop), but deliberately **do not** thread it into `resumable_decode`'s
per-object loop, and **do not** wire any reference into it from `try_filtered_commit_fetch`
(which has no owner reference to give it). This keeps the seam available for a future real
caller while adding zero live instrumentation cost or risk to the one fallback path that
exists today - documented here rather than guessed at silently.

## 6. Confirmation of unchanged shape

- **SQL call shape**: no new `SELECT`/`INSERT`/`UPDATE`/`MODIFY`/`DELETE` statements were
  added anywhere. All new "remaining"/"count" values reuse in-memory tables/counters already
  computed by the surrounding code (`lv_objects`, `lt_external_bases`,
  `count_unresolved(ct_meta)` - a pre-existing private helper already used for diagnostics).
- **HTTP call shape**: no new HTTP calls. Progress text is emitted immediately before/after the
  existing single `send_receive_close`/client-creation calls; nothing subdivides or retries the
  HTTP call itself.
- **Transaction ownership**: no `COMMIT WORK`/`ROLLBACK WORK` statement was added, moved, or
  removed. All existing commit points in `decode_and_persist_streaming`, `resolve_streaming`,
  and `persist_pull_result` are untouched.
- **Protocol bytes**: `build_request`/`build_upload_pack_buffer`/`parse` are not modified;
  want/have/shallow/deepen/filter line generation and side-band parsing are byte-for-byte
  unchanged.
- **Locks/retry/fallback control flow**: the thin -> self-contained -> recovery cascade's
  `TRY`/`CATCH` structure, `is_retry_without_haves` checks, and `zcl_abapgit_ortec_pack_dec`
  repo-lock acquire/release logic are unchanged; only new `report_progress` calls are inserted
  between existing statements.

No stop condition applies: the existing `zif_abapgit_progress` API supports this without any
change to standard abapGit UI infrastructure, the current source matched the stated call chain
closely enough to insert cleanly, and no signature change is user-visible to any external,
unscoped consumer (every new parameter is OPTIONAL).
