# Variant B Package D regression baseline

Baseline commit: 29199f629773c676e0eaa2f3a006f5167d304ae8
Scope: read-only inventory of Package D regression obligations from the existing ORTEC ABAP Unit suite.

## Acceptance ID D1-1: cold branch init / snapshot materialization

- Existing evidence: `zcl_abapgit_ortec_cold_init.clas.testclasses.abap`
- Methods:
  - `empty_repo_key_raises`
  - `empty_tip_commit_raises`
  - `graph_response_ceiling_value`
  - `all_present_needs_no_http`
  - `snapshot_idempotent`
  - `chunk_missing_dedups`
  - `chunk_missing_batch_limit`
  - `oversize_action_byte_limit`
  - `oversize_action_repeatable`
  - `verify_batch_missing_raises`
  - `verify_batch_extra_ignored`
  - `verify_batch_wrong_type`
  - `verify_batch_absent_content`
  - `may_publish_false_missing`
  - `finalize_writes_full_state`
- Assessment: Adequate baseline for D1. The suite already covers guard clauses, bounded batching, oversize handling, object verification, and publication gating for cold init.

## Acceptance ID D1-2: warm-unchanged / incremental certification behavior

- Existing evidence: `zcl_abapgit_ortec_have_policy.clas.testclasses.abap`
- Methods:
  - `full_certified_have_eligible`
  - `graph_only_have_ineligible`
  - `uncertified_have_ineligible`
  - `classify_warm_unchanged`
  - `classify_incremental_update`
  - `classify_cold_branch`
  - `backfill_skips_never_seen`
  - `backfill_completes_locally`
  - `backfill_incomplete_no_publish`
  - `backfill_repeat_idempotent`
- Assessment: Adequate baseline for D1. The suite already covers the policy split between warm-unchanged, incremental, and cold-branch cases, plus backfill behavior and publication gating.

## Acceptance ID D1-3: materialization state / attempt isolation / publication invariant

- Existing evidence: `zcl_abapgit_ortec_mat_state.clas.testclasses.abap`
- Methods:
  - `begin_attempt_*` style lifecycle methods
  - `mark_graph_complete_*`
  - `publish_snapshot_*`
  - `mark_full_complete_*`
  - `invalidate_commit_*`
  - `clean_attempts_*`
  - `publish_requires_full`
  - `full_can_publish`
  - `publish_keeps_repo_meta`
  - `no_c_snapshot_without_f`
- Assessment: Adequate baseline for D1. These methods directly cover the materialization state machine, publication gating, and cleanup semantics relevant to attempt isolation.

## Acceptance ID D1-4: fastpath certification and object persistence

- Existing evidence: `zcl_abapgit_ortec_fastpath.clas.testclasses.abap`
- Methods:
  - `certify_closure_incomplete`
  - `certify_missing_blob`
  - `certify_full_publishes`
  - `certify_idempotent_repeat`
  - `persist_stores_new_objects`
  - `persist_skips_existing_obj`
- Assessment: Adequate baseline for D1. The suite already covers certification completeness, missing-blob handling, publication, idempotency, and persistence semantics.

## Acceptance ID D1-5: explicit fetch request modes and capabilities

- Existing evidence: `zcl_abapgit_ortec_fetch_req.clas.testclasses.abap`
- Methods:
  - `blobless_requires_filter_want`
  - `blobless_no_haves_no_deepen`
  - `blobless_missing_filter_raises`
  - `thin_wants_and_haves`
  - `thin_advertised_and_haves_used`
  - `thin_no_haves_no_thin_capa`
  - `self_contained_uses_haves`
  - `materialize_wants_and_bounds`
  - `materialize_missing_capa_raise`
  - `recovery_minimal_and_no_haves`
- Assessment: Adequate baseline for D1. This is the clearest existing coverage for explicit fetch-mode serialization and bounded recovery behavior.

## Acceptance ID D1-6: promised-vs-current-tip missing blobs / materialization semantics

- Existing evidence: `zcl_abapgit_ortec_porcelain.clas.testclasses.abap`
- Methods:
  - `one_blob_one_path`
  - `same_blob_two_paths`
  - `dup_blob_obj_once`
  - `unrelated_blob_skip`
  - `non_blob_obj_skip`
  - `blob_data_sha_kept`
- Assessment: Partial baseline for D1. These tests cover blob materialization and deduplication, but they do not explicitly name the “promised-vs-current-tip missing blob” scenario as a dedicated regression case.

## Acceptance ID D1-7: bounded retry / recovery cascade and cache clear

- Existing evidence: `zcl_abapgit_ortec_cold_init.clas.testclasses.abap`, `zcl_abapgit_ortec_fetch_req.clas.testclasses.abap`, `zcl_abapgit_ortec_obj_store.clas.testclasses.abap`
- Methods:
  - `chunk_missing_batch_limit`
  - `oversize_action_byte_limit`
  - `oversize_action_repeatable`
  - `recovery_minimal_and_no_haves`
  - `active_repo_key_fallback`
- Assessment: Partial baseline for D1. There is bounded-batch and recovery coverage, but the specific “cache clear” and full recovery-cascade behaviors are not represented as a single dedicated regression test in the current inventory.

## Acceptance ID D1-8: ORTEC disabled fallback

- Existing evidence: `zcl_abapgit_ortec_obj_store.clas.testclasses.abap`
- Methods:
  - `active_repo_key_fallback`
- Assessment: Gap for D1. The current inventory shows fallback semantics at the repository-key/store layer, but no dedicated explicit “ORTEC disabled fallback” regression test was found in the inspected coverage.

## Acceptance ID D1-9: delta identity / chain / REF / OFS / external base / missing base

- Existing evidence: `zcl_abapgit_ortec_git_tests.clas.testclasses.abap`
- Methods:
  - `ref_chain_resolves`
  - `ofs_chain_resolves`
  - `external_thin_base_resolves`
  - `two_thin_bases_do_not_collide`
  - `missing_base_raises`
  - `missing_base_no_http_retry`
  - `base_after_dependent`
  - `resolve_after_prior_in_pass`
  - `chain_onto_later_unresolved`
- Assessment: Adequate baseline for D1. The suite already covers REF/OFS chains, external-base resolution, and missing-base failure paths.

## Acceptance ID D2-1: final attempt / session isolation / transaction isolation

- Existing evidence: `zcl_abapgit_ortec_pack_dec.clas.testclasses.abap`
- Methods:
  - `decode_populates_all`
  - `decode_from_pack`
  - `resume_after_partial`
  - `resume_no_session`
  - `cleanup_after_decode_failure`
  - `prefetch_bases_do_not_collide`
  - `peek_object_count_cases`
- Assessment: Strong baseline for D2. These tests explicitly exercise crash-resume behavior, cleanup on failure, and persisted session state without leaving orphaned rows behind.

## Acceptance ID D2-2: branch switch / re-entry safety

- Existing evidence: `zcl_abapgit_ortec_obj_store.clas.testclasses.abap`, `zcl_abapgit_ortec_mat_state.clas.testclasses.abap`
- Methods:
  - `active_repo_key_fallback`
  - `publish_requires_full`
  - `full_can_publish`
  - `publish_keeps_repo_meta`
- Assessment: Partial baseline for D2. There is evidence for repo-key fallback and publication state, but no single dedicated branch-switch regression test was found in the inspected methods.

## Acceptance ID D2-3: F/C publication invariant

- Existing evidence: `zcl_abapgit_ortec_mat_state.clas.testclasses.abap`, `zcl_abapgit_ortec_fastpath.clas.testclasses.abap`
- Methods:
  - `publish_requires_full`
  - `full_can_publish`
  - `certify_full_publishes`
  - `certify_closure_incomplete`
- Assessment: Adequate baseline for D2. The suite already covers publication gating and the difference between incomplete and fully certified closure.

## Acceptance ID D2-4: promised-vs-current-tip missing blob after retry

- Existing evidence: `zcl_abapgit_ortec_porcelain.clas.testclasses.abap`, `zcl_abapgit_ortec_pack_dec.clas.testclasses.abap`
- Methods:
  - `blob_data_sha_kept`
  - `cleanup_after_decode_failure`
  - `prefetch_bases_do_not_collide`
- Assessment: Partial baseline for D2. The current tests cover materialization and cleanup, but they do not explicitly assert the “promised blob missing after a retry/session restart” scenario as a dedicated regression case.

## Acceptance ID D2-5: bounded external-delta-base recovery / no silent fallback

- Existing evidence: `zcl_abapgit_ortec_git_tests.clas.testclasses.abap`, `zcl_abapgit_ortec_pack_dec.clas.testclasses.abap`
- Methods:
  - `external_thin_base_resolves`
  - `two_thin_bases_do_not_collide`
  - `missing_base_raises`
  - `missing_base_no_http_retry`
  - `prefetch_bases_do_not_collide`
- Assessment: Adequate baseline for D2. The suite explicitly checks missing-base handling and the absence of a silent fallback when a base cannot be resolved.

## D1 scenario matrix

| Scenario | Existing coverage | Baseline status |
| --- | --- | --- |
| Cold branch init | `empty_repo_key_raises`, `empty_tip_commit_raises`, `snapshot_idempotent` | Covered |
| Warm unchanged | `classify_warm_unchanged` | Covered |
| Incremental update | `classify_incremental_update` | Covered |
| Bounded batch / recovery | `chunk_missing_batch_limit`, `oversize_action_byte_limit`, `recovery_minimal_and_no_haves` | Covered |
| Publication gate | `may_publish_false_missing`, `publish_requires_full`, `certify_full_publishes` | Covered |
| Missing blob / promised-vs-current-tip | `blob_data_sha_kept` | Partial |
| External delta base resolution | `external_thin_base_resolves`, `two_thin_bases_do_not_collide`, `missing_base_raises` | Covered |
| ORTEC disabled fallback | none found in inspected methods | Gap |

## D2 scenario matrix

| Scenario | Existing coverage | Baseline status |
| --- | --- | --- |
| Final attempt / resume | `decode_populates_all`, `decode_from_pack`, `resume_after_partial`, `resume_no_session` | Covered |
| Session cleanup after failure | `cleanup_after_decode_failure` | Covered |
| Branch switch / re-entry | `active_repo_key_fallback` | Partial |
| F/C publication invariant | `publish_requires_full`, `full_can_publish`, `certify_full_publishes` | Covered |
| Missing blob after retry | `cleanup_after_decode_failure`, `blob_data_sha_kept` | Partial |
| Bounded external-base recovery | `missing_base_no_http_retry`, `prefetch_bases_do_not_collide` | Covered |

## Method-name length audit

- No method names in the inspected Package D evidence set exceeded 30 characters.
- The existing coverage is therefore consistent with the current ABAP naming limit for the methods that were inspected.

## Overall gap count

- D1 gaps: 1 explicit gap (ORTEC disabled fallback) and 1 partial area (promised-vs-current-tip missing blob semantics).
- D2 gaps: 1 explicit gap (branch switch / re-entry safety) and 1 partial area (promised-vs-current-tip missing blob after retry).
