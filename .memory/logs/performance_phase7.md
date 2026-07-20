# Phase 7 performance audit (static, large-repo review)

Date: 2026-07-12
Scope: full Ortec opt-rework across phases 1, 3, 4, 4b, 5a, 5b.1, 5b.2, 6, and the obj_store/repo_state crash fix.
Method: static audit only; no live large-repo benchmark environment was available, so this report evaluates whether the implemented mechanisms are wired in a way that can plausibly deliver the claimed latency win.

## Executive summary

The static evidence is broadly consistent with the Phase 7 latency target for a warm, cache-populated repository. The main filtered Stage/Diff path is now routed through a single bulk index lookup plus one bulk object-store fetch, and the old per-object loop style that would have scaled badly on large repos has been removed from the audited hot path.

The only meaningful caution is that the thin-pack/OFS_DELTA win is intentionally conservative. It should become reachable on a repeat-fetch from a repo whose commit history and object store are already complete, but it will remain off for cold or partially populated repos. That is a safe correctness choice, not a broken implementation.

## 1) Secondary-index usage for ZAOG_OBJ_STORE / ZAOG_PACK_IDX

Classification: Confirmed-fine

Evidence:
- The new ZAOG_OBJ_STORE index STA on (REPO_KEY, STATUS) is aligned with the bulk status-based reads in [src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap): `populate_cache`, `get_all_objects`, `get_known_commits`, and the bulk read helper `read_object_rows` all filter by `repo_key` and `status = 'R'` in a way that can use the STA prefix. These are not full-table scans by design.
- The new ZAOG_PACK_IDX index SHA on (REPO_KEY, OBJ_SHA1) is aligned with the delta-base completeness query in [src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap) and the pack-decoder resume path in [src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap): both use a repository-scoped object-SHA lookup for a set of candidates, which is exactly the shape the SHA index is meant to support.
- I did not find a query shape in the Ortec classes that filters by `(REPO_KEY, STATUS)` or `(REPO_KEY, OBJ_SHA1)` without the relevant prefix fields and would still be forced to scan the whole table because of a different predicate shape. The few remaining queries that include `OBJ_SHA1` in addition to `STATUS` are still keyed by the repository and a direct object identity, and the primary key remains the obvious path for single-object existence checks; that does not negate the new index for the bulk status-scoped reads.

Conclusion:
- The new indexes are wired to the query shapes they were intended to accelerate, and there is no evidence of a mismatched index design in the audited code.

## 2) Remaining N+1 / per-object round-trips in the filtered Stage/Diff/Patch/fetch hot path

Classification: Confirmed-fine

Evidence:
- The Phase 6 bulk-prefetch fix in [src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap](src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap) is now a real bulk operation: the resume path reads all previously decoded pack-index rows into one internal table, then issues one bulk `SELECT` against ZAOG_OBJ_STORE with `FOR ALL ENTRIES`, followed by a hashed-table lookup. This removes the old per-object `SELECT SINGLE` loop pattern in the resume path.
- The filtered Stage/Diff path now flows through [src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap), [src/ortec/git/zcl_abapgit_ortec_git_facade.clas.abap](src/ortec/git/zcl_abapgit_ortec_git_facade.clas.abap), and [src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap): it performs one index query for the filter rows, then one bulk object-store fetch for the relevant blobs, and only then computes status with the already-resolved remote set.
- The missing-object collector in [src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap](src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap) is also batch-oriented and does not loop over individual object-store lookups.
- I did not find a second hidden per-object `SELECT SINGLE` loop in the other hot-path-adjacent classes named in the request. The remaining `SELECT SINGLE` forms in [src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap) and [src/ortec/git/zcl_abapgit_ortec_repo_state.clas.abap](src/ortec/git/zcl_abapgit_ortec_repo_state.clas.abap) are single-object convenience reads, not looped hot-path work.

Conclusion:
- The known bulk-prefetch fix appears to be the only hot-path N+1 issue that was still present, and it is now addressed.

## 3) Bulk collection is truly bulk

Classification: Confirmed-fine

Evidence:
- [src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap](src/ortec/git/zcl_abapgit_ortec_missing_obj.clas.abap) calls `zcl_abapgit_ortec_obj_store=>get_missing_sha1s` once, then performs one negotiated fetch and one bulk persist. There is no per-object fetch loop in this path.
- [src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap) implements `get_missing_sha1s`, `get_objects`, and `get_reachable_objects` through chunked set-based reads. The object-store reader `read_object_rows` is the shared batched helper and is used by all three methods.
- The completeness gate in [src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap) also uses the same bulk pattern for `has_dangling_delta_base`, so the Phase 6 index addition is helping the actual set-based delta-base check rather than only the earlier completeness-gate concept.
- The index additions are relevant to these bulk shapes because the bulk object-store reads still carry the `repo_key` and `status = 'R'` prefix, and the pack-index delta-base check carries the `repo_key` and `obj_sha1` prefix.

Conclusion:
- The bulk collectors are genuinely bulk and are not accidentally falling back to per-object lookups. The new indexes are relevant to the real query shapes used by the bulk collectors.

## 4) Thin-pack / OFS_DELTA bandwidth win reachability

Classification: Minor-concern

Evidence:
- The thin-pack capability is only advertised when [src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap](src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap) receives a non-empty set of verified haves from [src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap](src/ortec/git/zcl_abapgit_ortec_fetch_neg.clas.abap). That gate is intentionally conservative.
- The gate is not dead. It can become true on a warm repeated fetch because `get_have_commits` can return commits from `ZAOG_COMMIT_HIST` or `ZAOG_REPO_STATE`, and `is_commit_complete` checks the index-ready marker, the reachable object set, and the absence of dangling delta bases.
- The caveat is that the gate will stay empty on a cold, partially populated, or partially indexed repo. In that case the code correctly falls back to the non-thin path rather than risking an unsafe delta response. That is a safe default, but it means the thin-pack win is conditional on successful prior materialisation.

Conclusion:
- The thin-pack win is reachable in a realistic repeat-fetch scenario, but it is not guaranteed. The conservative gate is appropriate for correctness, and the main performance implication is that the benefit is conditional rather than universal.

## 5) Overall filtered Stage/Diff path

Classification: Confirmed-fine

Evidence:
- The Stage and Diff hooks now resolve remote files through a single facade call in [src/repo/stage/zcl_abapgit_stage_logic.clas.abap](src/repo/stage/zcl_abapgit_stage_logic.clas.abap) and [src/ui/pages/diff/zcl_abapgit_gui_page_diff_base.clas.abap](src/ui/pages/diff/zcl_abapgit_gui_page_diff_base.clas.abap). Both call the Ortec facade and then pass the already-resolved remote set into [src/repo/zcl_abapgit_repo_status.clas.abap](src/repo/zcl_abapgit_repo_status.clas.abap) via the optional `it_remote` parameter, so status is computed in one pass and the code does not re-read the remote file list for each file.
- The facade itself delegates to [src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap), which resolves the repo key and commit and then calls [src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap). That method uses one set-based index lookup and one bulk object-store load for the relevant blobs; the work grows with the filtered set size, not with the size of the whole repository.
- There is no remaining per-file remote round-trip in this path. The only fallback is the standard full remote-file resolution when the Ortec preconditions are not met, which is a safe fallback rather than an accidental O(n) loop.

Conclusion:
- The filtered Stage/Diff path is now structurally consistent with the intended large-repo win: one index lookup, one bulk object-store fetch, and one status pass, with no per-file remote re-fetch.

## 6) New admin report overhead

Classification: Confirmed-fine

Evidence:
- The admin overview is only reachable from its own report entry point in [src/ortec/git/zabapgit_ortec_cache_admin.prog.abap](src/ortec/git/zabapgit_ortec_cache_admin.prog.abap), not from the Stage/Diff/Patch/fetch hot path.
- The backing implementation in [src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.abap](src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.abap) uses a few aggregate queries grouped by `repo_key` over the cache tables, which is reasonable for an admin tool and does not involve a full-table object-store read before aggregation.
- The report is intentionally off the hot path and is not part of the runtime Stage/Diff/Patch fetch workflow, so it will not perturb the large-repo latency path.

Conclusion:
- The admin report is lightweight enough for its intended role and does not appear to create a new hot-path bottleneck.

## Overall judgment

The static evidence is consistent with the claimed performance improvement, and it is stronger than the earlier state because the hot path now appears to be dominated by a small number of set-based operations rather than repeated object-by-object work.

In practical terms:
- For a warm repo whose object store, index marker, and commit history are already populated, the audited path looks like it can plausibly deliver the Phase 7 win because the expensive per-object selection and per-file remote re-fetch patterns are gone.
- For a cold or partially populated repo, the system will still fall back safely, and the thin-pack path will remain inactive until completeness is established. That is the correct trade-off for a correctness-first implementation.
- Because no live large-repo benchmark was available, this is a judgment based on wiring and algorithmic shape rather than measured latency. The evidence is sufficient to say the implementation is directionally consistent with the target, but not sufficient to prove the >=50% / >=2x threshold in a real environment.

No genuine fixable bottleneck was found in the audited hot path. The only remaining concern is the conservative thin-pack gate, which is a correctness-safe behavior and not an obvious implementation bug.
