# Performance IMPLEMENTATION_AUDIT — Variant B D2 TIME_OUT fix

```text
PACKET=COMPACT_HANDOFF_V1
TASK=D2-TIMEOUT-PERFORMANCE-IMPLEMENTATION-AUDIT
MODE=IMPLEMENTATION_AUDIT
BASELINE=2111b2887cc4fbf2ee481f753fd4af2c3e5085c4 (uncommitted working-tree diff on top)
STATUS=AUDIT_COMPLETE
```

Independent, read-only audit. `git diff` run directly against the exact
SOURCE_SCOPE files; full current content of all five files also read to
verify class/METHOD boundaries. No productive source was changed.

## 1. Diff verified against source (not the design/scan prose)

`git status --short` confirms the working-tree diff is scoped to exactly:
`zcl_abapgit_ortec_cold_init.clas.abap`, `.clas.testclasses.abap`,
`zcl_abapgit_ortec_fetch_req.clas.testclasses.abap`,
`zcl_abapgit_ortec_missing_obj.clas.abap`, `.clas.xml`, plus the new file
`zcl_abapgit_ortec_missing_obj.clas.testclasses.abap`. No file outside
SOURCE_SCOPE is modified (`git status --short` shows only `.memory/**`
untracked additions besides these).

**Structural integrity (full-file read, not just hunks):**
- `materialize_tip_snapshot` (lines 427-525): ends with exactly one
  `finalize_snapshot(...)` call followed by exactly one `COMMIT WORK.` and
  one `ENDMETHOD.` — confirmed by direct read. Its own `begin_attempt`,
  `verify_ready_blobs`, `may_publish_snapshot`, `finalize_snapshot` calls
  are all intact, outside the extracted loop, in original order.
- `materialize_missing_batches` (lines 527-624): fully separate, complete
  `METHOD`/`ENDMETHOD` block. Contains dedup → empty-return short-circuit →
  `TRY` (client init → reset budget → adaptive WHILE loop:
  `take_next_batch` → `materialize_batch` → `calculate_next_batch_size`) →
  `CATCH zcx_abapgit_ortec_git` (closes client, re-raises unchanged) →
  post-`ENDTRY` client close on success. **Zero** calls to
  `begin_attempt`/`mark_full_complete`/`verify_ready_blobs`/
  `finalize_snapshot`/`prepare_full_snapshot`, **zero** `COMMIT WORK` —
  confirmed by full-body read, not grep sampling.
- `zcl_abapgit_ortec_missing_obj=>ensure_available`: Step 2/3 collapsed
  into one direct call to `materialize_missing_batches`, exception
  propagated with no `TRY`/`CATCH`/re-wrap at all (stronger than the
  design's own §6.2 proposal, which still wrapped `CATCH
  zcx_abapgit_ortec_git` + `RAISE EXCEPTION` — the implementation
  simplified this to a bare call, which is behaviorally identical for an
  unhandled-exception-type raise and preserves `mv_unsupported_capability`
  exactly as required). Steps 1 and 3 (local `get_missing_sha1s`)
  unchanged verbatim.
- One incidental item already self-corrected during implementation per the
  scan log (§ Findings, "trailing COMMIT WORK... had to be carefully
  re-attached") — independently re-verified here as fixed: exactly one
  `COMMIT WORK.` exists in the whole diff, inside `materialize_tip_
  snapshot` only.

## 2/3. SQL and HTTP quantification (K vs. N)

| Scenario | HTTP requests | SQL calls |
| --- | --- | --- |
| K=1 (any N) | 1 info/refs + 1 MATERIALIZE_BLOBS POST (batch of 1) | O(1) chunked (`get_missing_sha1s` ×2, Steps 1/3) |
| K=1,000 | 1 info/refs + 1–3 POSTs (adaptive start 500, doubles) | O(K/chunk) bulk, ×2 |
| K=40,000 | 1 info/refs + tens of sequential POSTs (ramps 500→1000, converges to ~40 once saturated; more during ramp-up) | O(K/chunk) bulk, ×2 — unaffected by N |
| K=100 vs. N=1,000,000 | 1 info/refs + 1 POST (fits initial 500-row batch) | O(1) chunk, ×2 — `obj_sha1 IN (...) AND repo_key = ...`, never scans the other 999,900 rows |

All four scale with `K` only; none scale with `N`. Confirmed directly
against `zcl_abapgit_ortec_obj_store` presence-only SQL (`get_missing_
sha1s`/`get_present_sha1s`, unchanged by this diff) and `take_next_batch`'s
index-cursor slicing (bounded by `c_batch_rows_max`=1000/wire hard cap).

## 4. No per-object SQL/HTTP introduced

Confirmed: `materialize_missing_batches` issues zero SQL of its own — its
only SQL exposure is `materialize_batch`'s existing `decode_streaming`
persistence, bulk/unchanged. HTTP is batched via `take_next_batch`
(≤1000 SHA1s/POST), never one request per object. No new loop wraps a
singleton call.

## 5. Row/byte bounds inherited unchanged

`c_batch_rows_initial`=500, `c_batch_rows_min`=50, `c_batch_rows_max`=1000,
`c_max_batch_growth`=2, `c_target_response_bytes`=16 MiB,
`c_max_batch_response_bytes`=25 MiB, `c_max_oversize_splits`=10 — all
declared once in the class's `PUBLIC SECTION` (untouched by the diff) and
referenced identically by both `materialize_tip_snapshot`'s (now removed)
inline loop and the new `materialize_missing_batches`. No redefinition, no
bypass, no new constant introduced.

## 6. Transaction/attempt behavior confirmed unchanged in shape and order

`materialize_missing_batches` calls no `mat_state`/`repo_state` method and
issues no `COMMIT WORK` (§1). `materialize_tip_snapshot`'s own sequence —
`begin_attempt` → (loop, now delegated) → `verify_ready_blobs` →
`may_publish_snapshot` → `finalize_snapshot` → `COMMIT WORK` — is
byte-for-byte the same statement order as before the extraction.

## 7. SYSTEM_NO_ROLL fix untouched

`git status --short` / `git diff --stat` show no change to
`zcl_abapgit_ortec_obj_store.clas.abap` or its testclasses in this
working-tree diff — the 2111b288 fix (`get_reachable_objects`) is
completely unaffected.

## 8. D1 / Package C untouched

No file under D1's scope (`zcl_abapgit_ortec_delta.clas.abap`,
`zcl_abapgit_ortec_pack_stream.clas.abap`) or Package C's scope
(`zcl_abapgit_ortec_mat_state.clas.abap`,
`zcl_abapgit_ortec_repo_state.clas.abap`) appears anywhere in this diff —
confirmed via `git diff --stat` against those paths (no output).

## Mandatory scale scenarios

- Small (1–20 objects): estimated only, consistent with existing unit
  tests (`materialize_missing_empty`, `topup_narrows_to_blobs`,
  `mostly_shared_cold_branch` — 22-entry input, 2 genuinely missing).
- Medium (5,000+ objects, multiple batches): **estimated, not executed**
  — no live/mocked HTTP round-trip exists in this test infrastructure
  (documented pre-existing limitation, consistent with every prior D1/D2
  scan/audit in this project).
- Large (40,000+): **estimated, not executed** — same limitation.
- Shared branches (95–98%): partially exercised by
  `mostly_shared_cold_branch` (20/22 pre-buffered) at unit-test scale only;
  not measured at production scale.
- Incremental store (~100 affected / ~1,000,000 stored): **estimated**
  (§2/§3 table), not measured — no live IT8 retest was run in this audit.
- Interrupted attempt and retry: covered by design's `attempt_cleanup_
  preserved` test (no-fetch path only); a genuine mid-batch abort/retry is
  **NOT_APPLICABLE (documented)** per the new test file's own honestly-
  disclosed placeholders (`missing_after_topup_raises`, `no_repo_wide_
  topup`, `retry_is_bounded`).

All non-executed scenarios are explicitly marked estimated/not-applicable
above — none are presented as measured.

## Findings

No blocking or major findings.

| ID | Severity | Finding |
| --- | --- | --- |
| F-1 | MINOR (doc-only) | `materialize_batch`'s class-doc comment still states "Client ownership and cleanup belong exclusively to MATERIALIZE_TIP_SNAPSHOT" — now inaccurate since `materialize_missing_batches` is an equal, independent client owner. No behavioral impact (both methods correctly close the client on every path, verified in §1); recommend updating the comment to name both owners. |
| F-2 | MINOR (disclosure) | Consistent with the performance-design-gate's own M-1: the K=40,000 case requires tens of sequential HTTP round-trips within one dialog step (ramp-up from 500 toward the 1000 cap). Correct and bounded, not a defect — carried forward for visibility, no code change required. |

Both findings are non-blocking and match findings already disclosed in the
prior design/protocol reviews (no new regression introduced by
implementation).

## Verdict

**PASS**

The implementation is a faithful, behavior-preserving extraction exactly as
designed and previously approved (`APPROVE_WITH_MINOR_REVISIONS` at both
protocol and performance DESIGN_GATE). `ensure_available`'s new call path
is strictly K-bounded (never N-bounded) for SQL and HTTP across all four
mandated scale points, introduces no per-object SQL/HTTP, inherits all
row/byte bounds unmodified, and leaves the certification/attempt/commit
boundary, the SYSTEM_NO_ROLL fix, and D1/Package C entirely untouched.

## SAP validation closeout

```text
STATUS=SAP_VALIDATED_RESOLVED
PACKAGE_D_D2_VALIDATED_HEAD=733bb30799886ef8659be7e293b82c3ccfcebbdd
```

Live IT8 retest confirmed `TIME_OUT_REPRODUCED=NO`. The follow-up SAT trace
(`.memory/incidents/variant_b_d2_sat_warm_to_cold_o4h8794.md`) independently
confirms zero `materialize_missing_batches`/`ensure_available` cost on the
warm/already-materialized case - this audit's PASS verdict is validated
live, not only statically.
