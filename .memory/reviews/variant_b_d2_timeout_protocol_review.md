# Protocol/persistence review — Variant B D2 TIMEOUT fix design (Candidate A)

```text
PACKET=COMPACT_HANDOFF_V1
TASK=D2-TIMEOUT-PROTOCOL-REVIEW
BASELINE=2111b2887cc4fbf2ee481f753fd4af2c3e5085c4
REVIEWED_DESIGN=.memory/logs/variant_b_d2_timeout_fix_design.md (Candidate A)
STATUS=REVIEW_COMPLETE
```

This is an independent, read-only review. No productive source was changed.
All findings below are backed by direct re-reads of current source in
SOURCE_SCOPE (plus `zcx_abapgit_ortec_git` for exception-hierarchy facts
needed to evaluate finding §2 below).

## 1. Extraction fidelity (materialize_tip_snapshot -> materialize_missing_batches)

Verified directly against `zcl_abapgit_ortec_cold_init.clas.abap` lines
392-563 (`materialize_tip_snapshot`) and 918-997 (`take_next_batch`,
`calculate_next_batch_size`).

- Confirmed: `materialize_tip_snapshot`'s adaptive loop is
  `deduplicate_sha1s` -> WHILE (`take_next_batch` -> `materialize_batch` ->
  `calculate_next_batch_size`), wrapped in one `TRY`/`CATCH
  zcx_abapgit_ortec_git` that closes `lo_client` on both the failure path
  and the normal-completion path. `begin_attempt` (mat_state),
  `verify_ready_blobs` (obj_store), `may_publish_snapshot`,
  `finalize_snapshot`, and `COMMIT WORK` are all textually OUTSIDE this
  loop/TRY block. The design's §6.1 extraction boundary is drawn in
  exactly the right place — extracting the loop leaves 100% of the
  certification/publication logic in `materialize_tip_snapshot`, unchanged.
- **Design-document inaccuracy (not a code defect):** §13's test-plan table
  claims `chunk_missing_sha1s` tests (`missing_set_deduplicated`,
  `missing_set_row_bounded`) "already cover `materialize_missing_batches`'s
  own batching." This is incorrect. `chunk_missing_sha1s` (lines 565-593)
  is **not called anywhere in production code** — grep confirms its only
  caller in the entire workspace is its own test class
  (`zcl_abapgit_ortec_cold_init.clas.testclasses.abap` lines 278, 299).
  `materialize_tip_snapshot`'s actual loop uses `take_next_batch` +
  `calculate_next_batch_size`, a structurally different algorithm (index-
  cursor slicing of an already-deduplicated list with an *adaptive* row
  count driven by response bytes, vs. `chunk_missing_sha1s`'s *fixed*
  `c_materialize_batch_max`-sized chunking of a table it deduplicates
  itself via a hashed side table). Testing `chunk_missing_sha1s` proves
  nothing about the code path `materialize_missing_batches` will actually
  run. **Classified MAJOR** — the design's own §6.1 prose is correct (it
  says "take_next_batch -> calculate_next_batch_size"), only §13's test
  attribution is wrong; this is a self-contradiction within the same
  document that should be fixed before implementation, or the proposed
  tests will give false confidence.
- **Minor edge-case behavior delta (not a regression):** today, `IF
  lt_missing IS NOT INITIAL` (the raw, pre-dedup `get_missing_sha1s`
  result) gates whether `init_materialize_client` runs at all; dedup only
  happens just before the loop. If `lt_missing` somehow contained only
  blank-string entries (not achievable via `get_missing_sha1s`'s real
  contract, but not statically impossible), today's code would open+close
  an HTTP client with zero batches; per §6.1, `materialize_missing_batches`
  dedups its own `it_sha1s` first and returns before any HTTP call if the
  deduplicated set is empty. This is a strict improvement, not a
  regression, and not reachable via either real caller (`build_files_
  from_rows`, `topup_missing_blobs` both source `it_sha1s` from
  `get_missing_sha1s`/`get_tip_blob_sha1s`, which do not emit blanks).
  No action required.

## 2. Exception-wrap removal / `mv_unsupported_capability` survival

This was checked by tracing the actual exception class hierarchy, not
assumed:

- `zcx_abapgit_ortec_git` is declared `INHERITING FROM cx_static_check`
  (verified directly), **not** a subtype of `zcx_abapgit_exception`. This
  matters because both `zcl_abapgit_ortec_fetch_req=>build_request`'s own
  wrapping `CATCH zcx_abapgit_exception INTO lx_pkt_error` and
  `zcl_abapgit_ortec_cold_init=>materialize_batch`'s wrapping `CATCH
  zcx_abapgit_exception INTO lx_error` **do not** match a
  `zcx_abapgit_ortec_git` instance at runtime — so `raise_unsupported_
  capability`'s exception (raised inside `build_request`'s `WHEN
  cs_fetch_mode-materialize_blobs` branch, itself lexically inside that
  method's outer TRY) passes through **both** of those catch blocks
  unmodified and uncaught, carrying `mv_unsupported_capability = abap_true`
  intact all the way out of `materialize_batch`.
- Under the OLD `ensure_available` (Step 2 = `upload_pack_by_commit`), the
  call is routed through `zcl_abapgit_git_transport` (a standard, non-ORTEC
  abapGit class), whose failures are normalized to plain
  `zcx_abapgit_exception` before `ensure_available`'s own `CATCH
  zcx_abapgit_exception` wraps them — so the capability flag was **already
  unrecoverable** through that call path today, independent of this
  design.
- Under the NEW design, `ensure_available` calls
  `zcl_abapgit_ortec_cold_init=>materialize_missing_batches` directly (an
  ORTEC class raising `zcx_abapgit_ortec_git` natively), and the design's
  §6.2 rewrite catches `zcx_abapgit_ortec_git` (not the supertype) and
  re-raises the SAME instance unchanged. Combined with the hierarchy fact
  above, this genuinely delivers `mv_unsupported_capability = abap_true` to
  `ensure_available`'s caller for the first time — the design's stated goal
  is correctly achieved, not just asserted.
- Caller-impact check (both real call sites read in full):
  - `zcl_abapgit_ortec_obj_index=>build_files_from_rows` wraps its
    `ensure_available` call in `CATCH zcx_abapgit_ortec_git.` with an
    **empty handler** (falls through to the pre-existing `get_objects`
    miss-handling) — it inspects neither text nor `mv_unsupported_
    capability`. Unaffected by the wrap removal or by the text-narrowing
    that will occur once `it_sha1s`'s doc comment changes.
  - `zcl_abapgit_ortec_walk_prep=>topup_missing_blobs` (and its own caller
    `prewarm`) does not catch at all — it lets `zcx_abapgit_ortec_git`
    propagate raw. No further caller up that chain was found to pattern-
    match this exception's text.
  - The 3 existing gate tests in `ltcl_missing_objects`
    (`zcl_abapgit_ortec_git_tests.clas.testclasses.abap`) all use bare
    `CATCH zcx_abapgit_ortec_git.` with no assertion on `get_text( )` or
    `mv_unsupported_capability` — all three keep passing regardless of the
    Step 2/3 rewrap removal.
  - A workspace grep for the literal removed-wrap texts ("Missing-object
    fetch failed", "Missing-object persist failed") found no other
    consumer. The one text a sibling design (`variant_b_package_e_
    design.md`) references verbatim — `"<n> object(s) still missing after
    negotiated fetch"` — is `ensure_available`'s **Step 4** message, which
    this design does not touch.
- **No blocking or major finding here.** This is the strongest part of the
  design and the non-obvious exception-hierarchy fact is worth recording
  for future reviewers (see repo-memory note recommendation, not written
  per `STATE_WRITE_ALLOWED=no`).

## 3. MATERIALIZE_BLOBS wire correctness / capability gate

Verified directly in `zcl_abapgit_ortec_fetch_req=>build_request`'s `WHEN
cs_fetch_mode-materialize_blobs` branch:

- Want-count validated against `c_materialize_batch_max` (1000) before any
  buffer is built.
- Capability check (`iv_server_caps CS c_cap_reachable_want` OR `CS
  c_cap_tip_want`) executes **before** `build_want_lines` is ever called —
  a missing capability raises `raise_unsupported_capability` and no bytes
  are assembled, let alone sent. Hard-enforced, not advisory.
- `rs_request-have_count = 0` is hardcoded for this branch; no `build_
  have_lines` call exists in it. Confirmed independently in
  `zcl_abapgit_ortec_fastpath=>upload_pack`: certified-haves resolution
  (`get_certified_haves`) is gated by `IF iv_mode = incremental_thin OR
  iv_mode = incremental_self_contained` — `materialize_blobs` never enters
  that branch, so `lt_ortec_haves` stays empty structurally, not just by
  convention.
- No `deepen`/`shallow` token is emitted anywhere in the
  `materialize_blobs` branch (confirmed by reading the whole branch body —
  it only ever appends want-lines, `'0000'`, and `'0009done'`).

**Verdict: protocol-correct as designed, matches the design's §7 claims
exactly.**

## 4. Non-reintroduction of the deepen=0/empty-haves incident

`zcl_abapgit_ortec_missing_obj=>ensure_available`'s current deepen=1
workaround is scoped to its Step 2 call to `zcl_abapgit_git_transport=>
upload_pack_by_commit` (a wholly different serializer/transport path than
`zcl_abapgit_ortec_fetch_req=>build_request`'s `materialize_blobs` branch).
Since `materialize_blobs` structurally never emits `deepen`/`shallow`/
`have` lines (§3 above) and the design removes `ensure_available`'s only
call to `upload_pack_by_commit` entirely (replacing it, not routing through
it), the deepen=1 workaround's own target incident class (empty-haves +
deepen=0 => "send full history from the beginning" wire shorthand) cannot
recur through this new call path — there is no `deepen`/`have`
negotiation surface left for it to occur on. Confirmed no other file in
SOURCE_SCOPE re-introduces a call to `upload_pack_by_commit` for this flow.

## 5. Persistence/idempotency risk from the new call path

- `materialize_batch`'s persistence side effect is exactly
  `zcl_abapgit_ortec_pack_stream=>decode_streaming` — the same primitive
  every other fetch mode (`upload_pack`, `complete_missing_object`, and
  `materialize_tip_snapshot`'s own existing loop) already uses. This
  design introduces no new persistence primitive or commit boundary.
- Per `.memory/logs/variant_b_package_d_design.md` §1.3 (already-
  documented, D2-owned finding), `decode_and_persist_streaming` commits
  each pack's promotion durably before `resolve_streaming` runs. A
  mid-WHILE-loop failure in `materialize_missing_batches` therefore leaves
  earlier, already-succeeded batches durably committed (correct, wanted
  behavior — no partial rollback of good work) and the failing batch's own
  rows at `status IN ('I','D')`, which `get_missing_sha1s`/`get_objects`
  (both hard-filtered to `status = 'R'`, per the same design doc) correctly
  continue to report as missing — a subsequent retry cannot observe a false
  "present" positive.
- The one known gap — orphaned `'I'`/`'D'` rows surviving an **uncaught
  hard abort** (TIME_OUT/SYSTEM_NO_ROLL, not a caught ABAP exception) — is
  pre-existing, already tracked in
  `.memory/incidents/variant_b_d2_it8_system_no_roll_timeout.md` §7, and is
  not created or worsened by this design; if anything, bounding each fetch
  to the caller's true K-sized missing set makes hitting that abort
  scenario markedly less likely for this call path than today's N-sized
  `upload_pack_by_commit` fetch.
- **Minor documentation inaccuracy (§11 of the design):** the design states
  a materialized blob "is a self-contained fetch with no delta-base
  ambiguity requiring the `'D'` staging status." This overstates it:
  `build_request`'s `materialize_blobs` branch never *requests*
  `thin-pack`/`ofs-delta` capabilities (so no *externally*-based deltas are
  solicited), but nothing prevents the server from still encoding objects
  in the returned pack as internal `REF_DELTA`/`OFS_DELTA` entries for
  compression (exactly as the incident's own 162,919-object pack showed —
  132,963 of those were unresolved deltas). Such rows **will** transiently
  pass through `decode_and_persist_streaming`'s existing `status = 'D'`
  split and `resolve_streaming`'s promotion to `'R'`, same as any other
  streaming-decoded pack. This is harmless (the shared D2 pipeline already
  handles it correctly) but the design's justification text should not
  claim `'D'` rows are structurally impossible here — recommend correcting
  the sentence to avoid a future reader assuming a guarantee that doesn't
  hold.

## 6. Confirmed untouched: D1 external-base resolution, D2 attempt isolation, Package C F/C certification

- `materialize_missing_batches`'s extracted scope (per §1) contains no call
  to `zcl_abapgit_ortec_mat_state=>begin_attempt`/`mark_full_complete`/
  `publish_snapshot_complete`, no call to
  `zcl_abapgit_ortec_repo_state=>prepare_full_snapshot`, and no `COMMIT
  WORK` of its own — all four remain exclusively in `materialize_tip_
  snapshot`, outside the extracted loop (confirmed by direct line-range
  inspection, §1). No `ZAOG_COMMIT_HIST`/`ZAOG_REPO_STATE` write occurs on
  this call path.
- D1's bulk external-base fixpoint (Phase 1.5, owned by
  `zcl_abapgit_ortec_delta`/`zcl_abapgit_ortec_pack_stream`) is not in
  SOURCE_SCOPE and this design does not call into it differently than any
  other `decode_streaming` caller already does — no change in shape or
  frequency of that resolution for this path.
- **Clarification on the literal "never writes zaog_obj_store status='D'/
  'I' rows" phrasing in the task brief:** this is not literally true and
  should not be certified as such — `decode_streaming`'s own internal
  pipeline (shared, unchanged, D2-owned) necessarily writes transient
  `status = 'I'` rows during raw persist and `status = 'D'` rows for any
  delta-encoded entries in the response (§5 above), exactly as every other
  fetch mode already does. What genuinely holds, and is the substantively
  important guarantee, is that this design adds **no new** status value,
  **no new** cleanup predicate, and **no** write to the certification
  tables (`ZAOG_COMMIT_HIST`/`ZAOG_REPO_STATE`) — it reuses the identical,
  already-reviewed object-store persistence surface every other
  `MATERIALIZE_BLOBS`/`upload_pack` caller already exercises.

## 7. Row/byte/HTTP batching sanity check

`materialize_missing_batches` inherits `c_batch_rows_initial` (500),
`c_batch_rows_min` (50), `c_batch_rows_max`/`c_materialize_batch_max`
(1000), `c_max_batch_growth` (2), `c_target_response_bytes` (16 MiB),
`c_max_batch_response_bytes` (25 MiB), `c_max_oversize_splits` (10) — all
verified present and unmodified in `zcl_abapgit_ortec_cold_init`'s public
constants section. HTTP request count is `O(ceil(K / adaptive_rows))`,
never `O(1)` for large `K` and never proportional to `N` (unlike today's
one-HTTP-call-but-whole-commit-graph shape) — this is the exact fix the
incident calls for. SQL remains `O(K)` bulk (Steps 1/4 unchanged). No new
`FOR ALL ENTRIES`/per-object SQL is introduced; `materialize_batch`'s
persistence is unchanged.

## Findings summary

| # | Severity | Finding |
| - | -------- | ------- |
| 1 | MAJOR | §13's test-plan misattributes `chunk_missing_sha1s` coverage to `materialize_missing_batches`'s real algorithm (`take_next_batch`/`calculate_next_batch_size`); `chunk_missing_sha1s` is dead production code today. Revise the test plan to target the actually-reused methods (directly, or end-to-end via `materialize_missing_batches`) before implementation. |
| 2 | MINOR | §11's claim that `'D'`-status rows cannot occur for `MATERIALIZE_BLOBS` fetches is inaccurate — internal REF_DELTA/OFS_DELTA compression can still occur; harmless (shared pipeline handles it) but should be corrected in the doc. |
| 3 | MINOR | Two method/test name character counts in the design doc are off by one (`materialize_missing_batches` is 27 chars not 28; `capability_missing_rejected` is 27 not 28) — both still safely under the 30-char ABAP object/method-name limit, no action required beyond doc accuracy. |
| 4 | MINOR | Extracting the empty-after-dedup check ahead of client creation is a (harmless, unreachable-in-practice) behavior improvement vs. today, not a regression — noted for completeness only. |

No blocking findings. The core protocol design (§3/§4), the exception-flag
preservation mechanism (§2, verified against actual class hierarchy rather
than assumed), the certification/attempt-isolation boundary (§6), and the
row/byte batching reuse (§7) are all confirmed correct by direct source
inspection.

## Verdict

**APPROVE_WITH_MINOR_REVISIONS**

Condition: fix the MAJOR test-plan misattribution (finding 1) — retarget or
add tests that actually exercise `take_next_batch`/`calculate_next_batch_
size`/`materialize_batch` (the real code `materialize_missing_batches` will
run), either directly or via an end-to-end `materialize_missing_batches`
call — before or alongside implementation. The three MINOR findings are
documentation-accuracy items and do not need to block implementation start.
