# Variant B Package B — B2+B3 checkpoint handoff

Status: `IMPLEMENTED_PENDING_SAP_VALIDATION`

## Baseline

- Continues from B1 `SAP_VALIDATED_COMPLETE`, HEAD `85533762168fd2509ca469acd79c75b8e8b62279`.
- Checkpoint plan: `B2_PLUS_B3` combined (per approved design §9 — B2 has no
  independent productive caller).

## Implemented

- `ZCL_ABAPGIT_ORTEC_OBJ_STORE=>GET_TIP_BLOB_SHA1S` (B2): iterative bounded
  commit→tree walk collecting unique reachable blob SHA1s, no payload reads.
- `ZCL_ABAPGIT_ORTEC_COLD_INIT=>MATERIALIZE_TIP_SNAPSHOT` (B3, public entry
  point) plus private helpers `MATERIALIZE_BATCH`, `CHUNK_MISSING_SHA1S`,
  `DECIDE_OVERSIZE_ACTION`, `SPLIT_BATCH_IN_HALF`, `VERIFY_BATCH_OBJECTS`,
  `MAY_PUBLISH_SNAPSHOT` (public), new constants `C_MAX_BATCH_RESPONSE_BYTES`
  (25 MiB), `C_MAX_OVERSIZE_SPLITS` (7), new types `TY_SHA1_BATCH_TT`,
  `TY_OVERSIZE_ACTION`/`CS_OVERSIZE_ACTION`.
- No productive caller wired yet (Package C scope), matching B1's precedent.

## Tests added

- `zcl_abapgit_ortec_git_tests.clas.testclasses.abap` (`ltcl_obj_store`): 11
  new `TIP_BLOBS_*` tests for `GET_TIP_BLOB_SHA1S` — single root, nested
  dirs, dedup-tree-once, dedup-blob-once, missing-tree raises, non-READY-tree
  raises, wrong-type-tree raises, no-payload-read proof, empty-blob valid,
  >1000-wide frontier chunking, no graph/snapshot certificate side effect.
  Matches design §"Acceptance scenario / mandatory test coverage map" B2
  row (`get_tip_blob_sha1s`, §9).
- `zcl_abapgit_ortec_cold_init.clas.testclasses.abap` (`ltcl_cold_init`, now
  `LOCAL FRIENDS` of the main class): 11 new tests for
  `MATERIALIZE_TIP_SNAPSHOT` and its pure helpers — all-present needs no
  HTTP, idempotent re-run, batch dedup, batch-size limit, oversize-byte-limit
  split decision, oversize-repeatable-split-then-raise, verify-missing
  raises, verify-extra-ignored, verify-wrong-type raises, verify-absent
  -content raises, may-publish-false-on-still-missing. Matches design's B3
  row (`materialize_tip_snapshot`, §11: subtraction, batch chunking §10,
  partial-batch non-publication guard §4.4/§12).
  - The capability/mode/bounds scenarios for `MATERIALIZE_BLOBS` are already
    covered by existing Slice 2 `zcl_abapgit_ortec_fetch_req` tests
    (`materialize_wants_and_bounds`, `materialize_missing_capa_raise`,
    `materialize_over_max_raises`, `materialize_empty_raises`) — no
    duplicate test added, per the design's own acceptance-map guidance.
  - No-regression coverage is verified by the existing `ltcl_*` suites for
    `fetch_req`, `pack_stream`, completeness, and `base_cache` remaining
    unmodified, not a dedicated new test method.
  - "Missing" vs "SHA-invalid content" are proven via the identical
    `VERIFY_BATCH_OBJECTS`/`get_objects`-not-found mechanism — an
    intentional collapse, since Git's content-addressed SHA1 means both
    failure modes manifest identically as "the wanted SHA1 is absent".

## Documented implementation decision

`VERIFY_BATCH_OBJECTS` uses `get_objects` (not a second `get_missing_sha1s`
call) for per-batch verification, bounded to the batch size (≤100), to also
satisfy the "wrong-type raises" acceptance criterion. See
`.memory/reviews/perf_audit_variant_b_b2b3.md` for the full bounded-cost
justification — not a DESIGN_GATE-triggering change.

## Static/local validation

- `get_errors` clean on all four changed files.
- All new method and test method names verified ≤ 30 characters.

## Deferred boundaries (unchanged)

- Package C: productive branch orchestration, cold/warm decision, final have policy.
- Package D1: generalized bulk external delta-base resolution.
- Package D2: final attempt/transaction isolation.
- Package E: legacy cleanup.

## Next action

- Owner-executed SAP validation (IT8 import, activation, ABAP Unit, ATC) —
  `SAP_VALIDATION=PENDING`. Do not claim PASS before owner evidence.
- On PASS: update `.memory/state.md` Package B status to
  `B2+B3: SAP_VALIDATED_COMPLETE` and start Package C planning.
