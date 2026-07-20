# Variant B current-source reconciliation

Status: Slice 0 complete, read-only; no productive ABAP code edited.
Date: 2026-07-20
Scope: current productive source in the ORTEC fast-path, pack decode/streaming, fetch-negotiation, repo-state, object-store, walk, missing-object, and porcelain layers plus the directly used ZAOG persistence tables.

## Executive summary

The current source already has the right skeleton for Variant B: one repository-scoped object store, repository/branch state, a pack decoder, a streaming decode path, and a commit-completeness gate for haves. The gaps are not about missing concepts; they are about contract and invariants. The live flow still mixes incomplete graph state with branch state, still uses progressive deepen and retry-without-haves as a correctness mechanism, still allows some recovery paths to re-enter the network one object at a time, and still treats branch rows as if they imply object readiness even when the graph has not been verified.

In short: the implementation is a strong starting point for an ADAPT effort, but it is not yet aligned with the Variant B owner contract.

## Component classification

- zcl_abapgit_ortec_fastpath — ADAPT. The orchestration entry points already exist, but the live flow still uses thin/non-thin/progressive tiers and a `force_full`-style recovery contract instead of explicit fetch modes and an orchestrator-owned publication boundary.
- zcl_abapgit_ortec_pack_stream — ADAPT. The streaming decode and delta-resolution model is close to the required shape, but the result contract is still implicit and the hot path still performs external-base recovery and network repair in a way that is too loose for Variant B.
- zcl_abapgit_ortec_pack_dec — ADAPT. The pack decoder already provides resume and persistence hooks, but it still treats decode/persist as a low-level operation with a lot of side effects and no typed materialization certificate contract.
- zcl_abapgit_ortec_fetch_neg — ADAPT. The completeness gate exists and is an important improvement over earlier code, but the live have-set still derives from commit-history and fetch-pointer state rather than a first-class materialization certificate.
- zcl_abapgit_ortec_repo_state — ADAPT. The repository/branch state exists and is already split from the object payloads, but it still stores fetch pointers and shallow/deepen metadata that are not sufficient as a correctness certificate.
- zcl_abapgit_ortec_obj_store — ADAPT. The repository-shared object store is already the right core abstraction, but it needs stronger explicit repo-key handling, presence-vs-read semantics, and a more disciplined readiness contract.
- zcl_abapgit_ortec_walk_prep / zcl_abapgit_ortec_missing_obj / zcl_abapgit_ortec_porcelain — ADAPT. These are already relevant to graph walking and missing-object handling, but they need to be aligned with a certified graph/snapshot model rather than the old “best effort” assumptions.
- zcl_abapgit_git_transport — ADAPT. The transport layer already exposes the connection and request assembly hooks, but a single validated request serializer and capability gate are still missing.
- ZAOG_* tables — REUSE_WITH_ADAPT. Existing tables such as ZAOG_OBJ_STORE, ZAOG_REPO_STATE, ZAOG_COMMIT_HIST, ZAOG_PACK_IDX, and ZAOG_PACK_META already provide the persistence core; they need new state fields or a new materialization-state table rather than a brand-new storage model.

## Requirement mapping to current source

### 1) One physical SHA-addressed object store per repository
- Existing implementation: zcl_abapgit_ortec_obj_store and the repository-scoped ZAOG_OBJ_STORE table.
- Current evidence: store_object/store_objects/get_object/get_objects/get_reachable_objects/get_reachable_sha1s all key by repository plus object SHA.
- Verdict: largely present and already aligned; this is a REUSE/ADAPT point, not a new design.

### 2) Branch/ref state separate from object payloads
- Existing implementation: zcl_abapgit_ortec_repo_state and ZAOG_REPO_STATE for branch/ref pointers, versus ZAOG_OBJ_STORE for payloads.
- Current evidence: repo-state stores branch name, remote URL, current/fetch commit, shallow/deepen fields; object payloads live in ZAOG_OBJ_STORE.
- Verdict: present structurally, but the state still mixes branch pointer semantics with object-readiness claims and therefore needs adaptation.

### 3) Cold unknown-branch access uses blobless graph acquisition with no deepen
- Existing implementation: try_filtered_commit_fetch, fetch_tip_commits, and the upload-pack request builder.
- Current evidence: the code can issue a filtered `filter blob:none` request and a one-commit want request, but the main branch fast-path still uses a generic deepen-based request builder when haves are empty. The current builder still sends `deepen` whenever no haves are available.
- Verdict: partially present; the owner requirement is not yet implemented as a first-class explicit mode and the current flow still uses a deepen-based cold-start path.

### 4) Bulk materialization of only current-tip blobs
- Existing implementation: no single shard-level API yet for “current-tip blobs only.” The current walk code can traverse trees and collect blob SHAs, but the live path still treats a pull as a broader object fetch and does not publish a branch-snapshot materialization boundary.
- Current evidence: zcl_abapgit_ortec_obj_store=>get_reachable_objects and zcl_abapgit_ortec_walk_prep/porcelain are the closest existing pieces.
- Verdict: NEW/ADAPT. A typed blob-set materialization API is still missing.

### 5) Repository-wide reuse of commits, trees, and unchanged blobs
- Existing implementation: object store plus repo-scoped persistence and the object-store cache.
- Current evidence: the store is repo-scoped and can serve objects by SHA across branches; the current code already benefits from shared storage and a per-repo cache.
- Verdict: mostly present; the design still needs a certificate-based view of what is “ready” and reusable.

### 6) Thin/OFS deltas only when every required base is provably available
- Existing implementation: zcl_abapgit_ortec_pack_stream and zcl_abapgit_ortec_pack_dec plus the completeness gate in zcl_abapgit_ortec_fetch_neg.
- Current evidence: the fetch-negotiation layer now tries to filter to verified-complete haves, and the streaming pack decoder has explicit base-resolution and external-base recovery logic.
- Verdict: partially present, but the current implementation still allows rescue-type recovery and does not make the completeness certificate the only gate for thin/OFS usage.

### 7) No progressive deepen as correctness or final recovery
- Existing implementation: zcl_abapgit_ortec_fastpath has progressive-deepen constants, first/next helper methods, and a bounded retry cascade.
- Current evidence: upload_pack_by_branch/upload_pack_by_commit both implement thin → non-thin → progressive retry tiers, and build_upload_pack_buffer still uses `deepen` when haves are empty.
- Verdict: directly contradicted by the current live flow. This is one of the clearest mismatches to Variant B.

### 8) Missing/unknown/promised data must never be treated as remote deletion
- Existing implementation: the object-store state model has explicit object-state constants such as `NOT_BUFFERED`, `UNKNOWN_NEEDS_FETCH`, `CONFIRMED_ABSENT`, and `CORRUPT_OR_INCOMPLETE`.
- Current evidence: zcl_abapgit_ortec_obj_store=>cs_object_state documents the intent clearly; the current implementation still uses conventional presence checks and does not yet expose this as a first-class contract for the fetch orchestrator.
- Verdict: partially present in the object-store model, but not yet enforced end to end by the fetch orchestration.

### 9) Resolve in-pack dependencies before classifying a delta base as external
- Existing implementation: zcl_abapgit_ortec_pack_stream=>resolve_streaming and resolve_one_meta.
- Current evidence: the code explicitly performs repeated ascending sweeps and then tries external bases. That is the correct direction, although it still involves a later repair phase for external bases.
- Verdict: partly implemented and mostly aligned.

### 10) External bases are deduplicated and bulk-loaded
- Existing implementation: zcl_abapgit_ortec_pack_stream has a base-cache and a thin-pack completion route, but external-base acquisition is still effectively a recovery path and not yet a bulk, attempt-scoped dependency load.
- Current evidence: get_base_bytes uses a base cache and may trigger complete_missing_base; the current design is still too per-base for Variant B.
- Verdict: not yet aligned with the owner requirement.

### 11) No SQL/HTTP request per object, tree node, blob, or delta base
- Existing implementation: current code already does bulk object reads in the object-store helpers and uses set-based work where possible, but the hot path still contains repair sequences that can produce one-shot network fetches for missing bases.
- Current evidence: complete_missing_base/complete_missing_object and the per-base get_base_bytes path remain live hotspots.
- Verdict: current source still violates this invariant in the repair path and therefore needs a structural rewrite.

### 12) Incremental work scales with required objects K, not all repository objects N
- Existing implementation: object-store reads are bulk-based, and the streaming decoder batches DB writes; the current implementation is already stronger than the old one-object-at-a-time decoder.
- Current evidence: the streaming path uses batch sizes of 500 and bulk persistence helpers; the object-store helpers batch reads with a package size of 1000.
- Verdict: partly achieved, but still undermined by the per-base completion repair.

### 13) Failed attempts publish no ready objects, branch state, or certificates
- Existing implementation: the pack-streaming path cleans up incomplete rows and the repo-state update is a later publication point, but there is still no explicit materialization-attempt state that owns publication.
- Current evidence: the current code uses low-level persistence and then updates repo state later, without a single orchestrator-owned boundary.
- Verdict: NEW. This is a real architectural gap.

### 14) Request only capabilities advertised by the server
- Existing implementation: the request builder and the fetch-tip/filtered-fetch helpers inspect capability advertisement strings.
- Current evidence: capability extraction is still string-based and a single request serializer is missing. The code also has independent request builders for different paths.
- Verdict: partially present; not yet a single validated serializer.

## Live occurrences of the requested anti-patterns

### Progressive-deepen recovery
- zcl_abapgit_ortec_fastpath=>upload_pack_by_branch and upload_pack_by_commit.
- Evidence: the methods implement thin → non-thin → progressive retry; constants c_progressive_start_min, c_progressive_widen_factor, c_progressive_max_deepen, and c_progressive_max_steps are live.
- Impact: this is explicitly superseded by the Variant B requirement and must be removed from the live flow.

### force_full that can emit deepen
- zcl_abapgit_ortec_fastpath=>build_upload_pack_buffer.
- Evidence: the builder still sends `deepen` when `it_ortec_haves` is initial, and the method comments explicitly state that the current design still uses the caller-supplied deepen value even under a force-full-style request.
- Impact: this breaks the owner’s explicit-mode contract.

### Recursive or per-object completion repair
- zcl_abapgit_ortec_pack_stream=>get_base_bytes and zcl_abapgit_ortec_pack_stream=>complete_missing_base.
- Evidence: missing delta bases trigger `complete_missing_base`, which calls `complete_missing_object` in the fastpath layer; this is functionally a one-object-at-a-time repair path.
- Impact: this is the clearest example of a non-Variant-B repair loop.

### Uncertified or shallow-ambiguous bare haves
- zcl_abapgit_ortec_fetch_neg=>get_have_commits and zcl_abapgit_ortec_repo_state=>get_complete_commits.
- Evidence: the current logic can assemble candidate haves from commit history and branch-state fetch pointers before any materialization certificate is used as the sole authority.
- Impact: this is a correctness hole for the have-eligibility contract.

### Silent standard decode fallback
- zcl_abapgit_ortec_fastpath=>try_filtered_commit_fetch.
- Evidence: this method catches ORTEC decode failure and falls back to zcl_abapgit_ortec_pack_dec=>decode_and_persist on the same pack bytes.
- Impact: the current code still allows a fallback path that reuses a different decoder without a structured reason or explicit recovery mode.

### Blank repo-key fallback
- zcl_abapgit_ortec_obj_store=>get_object.
- Evidence: the method accepts an optional repo key and uses a cached active repo key if the incoming value is blank. The class also has `set_active_repo_key` for this purpose.
- Impact: this is a direct violation of the owner’s requirement to pass repository keys explicitly.

### SQL/HTTP calls from hot loops
- zcl_abapgit_ortec_pack_stream=>get_base_bytes and zcl_abapgit_ortec_pack_stream=>resolve_one_meta.
- Evidence: external-base resolution and completion repair still go back to the object store and the network from the delta hot path instead of using a batch/attempt-scoped dependency load.
- Impact: this is not acceptable for the final Variant B implementation.

### Repository-wide reads used for incremental work
- zcl_abapgit_ortec_obj_store=>populate_cache and get_all_objects.
- Evidence: populate_cache reads all objects for a repository into the session cache; get_all_objects does the same for a repo-wide materialization path.
- Impact: this is the wrong scale for incremental fetch work and should not be the basis for the Variant B hot path.

### Payload reads used for presence checks
- zcl_abapgit_ortec_obj_store=>get_reachable_objects and get_object.
- Evidence: the full walk reads object payloads to build tree/blob content, while the current completeness helper get_reachable_sha1s explicitly avoids payload reads for a reason. The gap is that presence and content are still not separated cleanly in the higher-level orchestration.
- Impact: the future design needs a stronger presence-only contract for graph verification.

### Unbounded XSTRING copies / full-response materialization
- zcl_abapgit_ortec_fastpath=>upload_pack and zcl_abapgit_ortec_fastpath=>parse.
- Evidence: the current fastpath currently receives the entire response into an XSTRING before parsing and then decodes it from that intermediate buffer.
- Impact: Variant B needs explicit memory-gated behavior and bounded payload handling rather than a fully materialized response object for recovery paths.

### Low-level COMMIT WORK
- zcl_abapgit_ortec_pack_dec=>decode_and_persist.
- Evidence: the current decoder commits after storing the raw pack and after creating the session, and the streaming path uses low-level commit semantics around persistence.
- Impact: this is too coupled to transport/persistence mechanics; the new design needs an orchestrator-owned publication boundary.

## Drift against the external review findings

The current source is already partially aligned with some of the review findings and directly contradicts others:

- F-01 (have eligibility is not one atomic invariant) — partially addressed in the fetch-negotiation helper, but still not enforced by the materialization contract.
- F-02 (non-thin is not equivalent to a full fetch) — current code still uses a thin/non-thin/progressive cascade and therefore still overstates what the second retry means.
- F-03 (force_full semantics are not centrally enforced) — current code still has request-builder logic that can emit deepen in the no-haves case; this is still a live inconsistency.
- F-04 (REF_DELTA bases cannot be decided during raw pre-scan) — the current pack-streaming model is structurally aware of this and is directionally correct.
- F-05 (external-base acquisition is one-by-one) — still live and still a clear performance/correctness problem.
- F-06 (singleton global base cache is not repository-scoped) — still a design risk; current code uses a cache object but the contract is still too global for a strict Variant B model.
- F-07 (blank repo-key fallback is unsafe) — still live in the object-store getter.
- F-08/F-09/F-10 (identity mapping and package-index assumptions) — the current code has already moved toward more explicit metadata handling in the streaming path, but the final design still needs a stronger, immutable metadata model and a separate external-base map.
- F-11/F-12/F-13/F-14/F-15/F-16/F-17 — the current source already demonstrates some partial hardening, but these are still design risks or explicit gaps relative to the owner’s non-negotiable invariants.

## Bottom line for Slice 1+

The current implementation already has most of the building blocks for Variant B, but the live contract is still wrong in the places that matter most:

1. the fetch flow is still a retry-and-recover orchestration rather than an explicit-mode serializer;
2. the completeness and materialization state is not yet first-class and certificate-based;
3. the recovery path still uses per-object repair and a weak publication boundary; and
4. the current branch-state model is still too close to the old fetch-pointer model to be trusted as a correctness certificate.

That means the next design slice should be built as an ADAPT-and-replace effort around the existing repo-scoped object store, the existing packet decode pipeline, and the existing branch-state tables, not as a completely new subsystem.
