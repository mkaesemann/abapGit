# Package D1 — implementation map

```text
PACKET=COMPACT_HANDOFF_V1
TASK=D1-IMPLEMENTATION
BASELINE=5a1171f24f0fe0664eeaa3a832fee1bc7076a2e3
STATUS=SAP_VALIDATED_COMPLETE
VALIDATED_HEAD=73cb519a
SAP_SYSTEM=IT8
ACTIVATION=PASS
SYNTAX=PASS
ABAP_UNIT=PASS
ATC=PASS
WARM_BRANCH=PASS
COLD_BRANCH=PASS
SLIN_WARNINGS=NONE
D1_BLOCKERS=NONE
D2_STATUS=AUTHORIZED_NOT_STARTED
```

Baseline verification: `git rev-parse HEAD` = `5a1171f2...` (D0 design finalize,
memory-only on top of `5e540354`). `git status --short` clean. `git show
--stat` confirmed the one commit after `5e540354` touches only `.memory/**`.
Used as the D1 implementation baseline per the run brief's rule 5.

## Files changed

- `src/ortec/git/zcl_abapgit_ortec_delta.clas.abap` — new PUBLIC
  `bulk_resolve_external_bases`; Phase 1.5 inserted into `resolve_all`
  between the existing Phase 1 DO-loop and Phase 2 final pass; two
  PRIVATE test-only call counters (`gv_bulk_load_calls`,
  `gv_thin_fetch_calls`) incremented at the exact bulk-call and
  on-demand-fallback call sites in `bulk_resolve_external_bases`/
  `resolve_one`.
- `src/ortec/git/zcl_abapgit_ortec_delta.clas.xml` — add
  `WITH_UNIT_TESTS = X`.
- `src/ortec/git/zcl_abapgit_ortec_delta.clas.testclasses.abap` — NEW file,
  `ltcl_delta` local-friend test class, mandatory D1 test list.
- `src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap` — `resumable_decode`:
  remove the manual `SELECT ... FOR ALL ENTRIES` external-base prefetch
  block (now superseded by `resolve_all`'s internal Phase 1.5); capture
  `lv_original_count` before calling `resolve_all`; replace the
  `lt_base_shas` SHA1-based skip in the persist loop with an
  index-threshold check (`sy-tabix > lv_original_count`), since Phase 1.5
  appends merged external bases strictly after every originally-parsed
  object, exactly mirroring the removed code's own invariant.
- `src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap` — `resolve_streaming`:
  insert Phase 1.5 (bulk-collect undeclared REF_DELTA bases not yet in
  `ct_sha_idx`, call the shared helper, merge each loaded base as a new
  `ty_meta` row + `ct_sha_idx` entry + base-cache warm) between Pass 1 and
  the existing `preload_external_bases` call; catch
  `zcx_abapgit_exception` from the shared helper and re-raise
  `zcx_abapgit_ortec_git` with `iv_retry_without_haves = abap_true`
  (preserves the existing recovery-tier contract verified by
  `missing_base_no_http_retry`); `decode_streaming`: guard the
  `WHERE obj_type = commit` sparse-extraction loop with an
  originally-decoded-row-count threshold so a Phase-1.5-merged external
  base that happens to be `obj_type = commit` (e.g. a thin REF_DELTA
  commit) is never mistaken for this pack's own new commit; one PRIVATE
  test-only counter `gv_thin_fetch_calls` incremented at
  `resolve_one_meta`'s external-base on-demand fallback (`get_base_bytes`/
  `get_object` branch).
- `src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.testclasses.abap` —
  extend `ltcl_pack_stream` with D1-relevant streaming-adapter tests only
  (per design §16 placement rule); add `LOCAL FRIENDS` line for the counter.

No DDIC change. No `zcl_abapgit_ortec_obj_store` change (D2 owns
`get_staged_delta_objects`). No D2 status/attempt/lock/publication change.

## SQL shape

- `bulk_resolve_external_bases`: exactly one
  `zcl_abapgit_ortec_obj_store=>get_objects(iv_bulk_fetch = abap_true)`
  call per invocation, only when its deduplicated input set is non-empty.
  `get_objects` itself raises on any missing SHA1 (existing, unchanged
  contract) — completeness validation is free, no extra query.
- Non-streaming (`resolve_all`): at most one `bulk_resolve_external_bases`
  call per `resolve_all` invocation (i.e. per pack), replacing the old
  manual `SELECT ... FOR ALL ENTRIES` (also exactly one call) — net SQL
  call count for external-base loading is unchanged (still exactly 1),
  only the implementation is now shared/validated.
- Streaming (`resolve_streaming`): at most one `bulk_resolve_external_bases`
  call per `resolve_streaming` invocation — replaces what was previously
  zero-to-K individual `get_object`/`get_base_bytes` DB calls (one per
  still-external base after cache-warming) with a single bulk call.
- Zero SQL/DB calls during either resolver's Phase 1 in-pack fixpoint
  (unchanged, already true).
- Zero per-base SQL calls in either resolver's final pass after the Phase
  1.5 merge (previously guaranteed for non-streaming; newly guaranteed for
  streaming — this is the actual D1 fix).

## HTTP shape

No HTTP calls anywhere in D1 scope (unchanged — delta resolution never
issues HTTP; the existing `iv_retry_without_haves` signal is preserved
verbatim for the caller's own cascade, out of D1 scope).

## Row / byte bounds

- Bounded by K = distinct external REF_DELTA bases needed by ONE pack,
  never by repository size N (per Package D design §18 scaling table).
- `get_objects(iv_bulk_fetch = abap_true)` is deliberately unchunked
  (existing, documented INV-B-13 exception) — safe here because the input
  is always pack-bounded (K), never repository-wide.

## Oversized-base behavior

Unchanged — a single oversized base is read/rejected exactly per the
existing `get_object`/`get_objects`/`read_object_rows` contract (no new
size-based branching introduced by D1).

## Lookup complexity

O(1) per merged base: non-streaming via `ct_tabix_by_index` (HASHED,
unique key `obj_index`, existing type); streaming via `ct_sha_idx`
(HASHED, unique key `sha1`, existing type). No new O(n) scan introduced.

## Maximum simultaneous payload copies

One transient `lt_loaded_bases`/`rt_objects` table (size K, pack-bounded)
held during the merge step, then absorbed into `ct_objects`/`ct_meta` —
no second full-pack copy. Matches existing Phase 2/on-demand-fetch memory
shape.

## Cache scope / invalidation

Unchanged — `zcl_abapgit_ortec_base_cache` (256 MiB LRU, process-global,
content-addressed) is warmed by Phase 1.5's streaming merge exactly as
`preload_external_bases` already does; no new cache, no new invalidation
rule.

## Transaction behavior

No `COMMIT WORK`/`ROLLBACK WORK` in `bulk_resolve_external_bases`, the
Phase 1.5 merge blocks, or any D1-touched read path. `resolve_all` never
committed before and still doesn't. `resolve_streaming`'s existing single
`COMMIT WORK` at its own end is unmoved and unchanged in scope.

## Expected shape at scale (design §18 scaling table, reconfirmed for D1)

| Stored objects (N) | External bases needed (K, one pack) | SQL calls (bulk load) |
| --- | --- | --- |
| 1 | 0 or 1 | 0 or 1 |
| 1,000 | typically < 20 | 1 |
| 40,000 | typically < 20 | 1 |
| 1,000,000 | ~100 (incremental fetch) | 1 |
| 100 affected objects / 1,000,000 stored keys | bounded by the ~100 declared external bases in the incoming pack | 1 (never scans the 1,000,000 stored rows) |

## Concurrent commit reconciliation (35be4c65 et al.)

`preload_delta_rows`/`preload_external_bases` (commit `35be4c65`) are
preserved unmodified and called exactly where they are today — Phase 1.5
is inserted as an additional step, not a replacement, per
`.memory/logs/variant_b_package_d_concurrent_commit_impact.md`. After
Phase 1.5 merges a base into `ct_sha_idx`, `preload_external_bases`'s own
candidate-collection loop finds it already indexed and skips it (no
duplicate bulk read — its `READ TABLE it_sha_idx ... WITH TABLE KEY sha1`
guard already exists unchanged).

## Completion note

All planned edits landed exactly as designed above; no deviation from the
approved D0 design was required. `zcl_abapgit_ortec_pack_dec.clas.abap`'s
refactor (delete manual prefetch block, `lv_original_count` threshold skip)
and `zcl_abapgit_ortec_pack_stream.clas.abap`'s Phase 1.5 insertion +
`decode_streaming` commit-filter guard + `gv_thin_fetch_calls` counter were
implemented as specified. `get_errors` (real ADT-based syntax check) reports
zero errors on all 5 changed/new files. A full-repo `abaplint` run was
executed; every finding on the touched files is pre-existing baseline noise
from a config mismatch (targets an old ABAP version, doesn't resolve custom
`ZCL_ABAPGIT_ORTEC_*` types) — confirmed by running the identical config
against an untouched sibling file (`zcl_abapgit_ortec_fastpath.clas.abap`),
which independently shows 38 findings of the same kinds. No new
abaplint-detectable issue class was introduced by this diff.

Not yet performed (explicitly out of scope for this implementation pass,
required before D1 can be marked SAP_VALIDATED_COMPLETE): live SAP
syntax/activation check, ABAP Unit execution (no local runner available),
`ortec-abapgit-performance-scan`/`ortec-abapgit-performance-review`
`IMPLEMENTATION_AUDIT`, and `ortec-abapgit-regression`.

## SAP validation closeout

Owner-executed live SAP validation in system IT8 on commit `73cb519a`
confirmed: ACTIVATION=PASS, SYNTAX=PASS, ABAP_UNIT=PASS, ATC=PASS,
WARM_BRANCH=PASS, COLD_BRANCH=PASS, SLIN_WARNINGS=NONE (the 3
`ZCX_ABAPGIT_EXCEPTION is not caught or declared` warnings on
`zcl_abapgit_ortec_pack_dec=>peek_object_count` and
`zcl_abapgit_ortec_delta=>skip_size_header` were fixed in commit `73cb519a`
and confirmed cleared on retest). D1_BLOCKERS=NONE. D2_STATUS is now
AUTHORIZED_NOT_STARTED.

The false `Local MODIFIED`/`Remote MODIFIED` indicator observed in a large
repository is a non-D1 follow-up, classified `PACKAGE_E_CONSUMER_COHERENCE`
and does not block this closeout (warm/cold branch acquisition succeed, ABAP
Unit/ATC are clean, and no actual content delta is present). Full causal-chain
evidence: [.memory/logs/variant_b_package_d_d1_modified_status_triage.md](.memory/logs/variant_b_package_d_d1_modified_status_triage.md).

D1 is closed as `SAP_VALIDATED_COMPLETE`. See
[.memory/logs/regression_variant_b_package_d_d1.md](.memory/logs/regression_variant_b_package_d_d1.md)
for the full owner evidence record.

