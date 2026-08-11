# Performance Scan

## Scope
- Topic: OBJ_PERF_FINAL partial ZAOG_OBJ_INDEX / ZAOG_OBJ_COVER / ZAOG_OBJ_PIDX path
- Slice: 1/1b/1c/1d/2/3 demand-driven partial index
- Entry methods: ensure_filtered_coverage, walk_filtered, invalidate_commit_index, select_partial_rows_for_filter, compute_context_hash, get_coverage, write_coverage, clear_repo, get_remote_files_for_stage
- Files inspected: [src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap), [src/ortec/git/zcl_abapgit_ortec_obj_cover.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_cover.clas.abap), [src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.abap](src/ortec/git/zcl_abapgit_ortec_cache_admin.clas.abap), [src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap](src/ortec/git/zcl_abapgit_ortec_filter_walk.clas.abap), [src/ortec/git/zaog_obj_cover.tabl.xml](src/ortec/git/zaog_obj_cover.tabl.xml), [src/ortec/git/zaog_obj_pidx.tabl.xml](src/ortec/git/zaog_obj_pidx.tabl.xml), [src/ortec/git/zaog_obj_index.tabl.xml](src/ortec/git/zaog_obj_index.tabl.xml)
- Expected cardinality: filtered walk over a caller-supplied object set K, with a full commit-tree walk over the repository graph F and bounded per-call filter chunks of 5000 rows

## Summary
- Verdict: MINOR_FINDINGS
- Estimated SQL shape: bounded and chunked; the new filtered read/write paths use fixed 5000-row chunks for caller-supplied filter sets, and the complete-mode index path remains write-batched at 30000 rows
- Estimated HTTP shape: none in the inspected scope; the filtered path is implemented via the ORTEC object-store and pack helpers, not a per-object network loop
- Estimated memory risk: LOW to MEDIUM; the main risk is a repeated linear membership test inside the tree walk rather than a materialization or XSTRING growth issue

## Findings

### PS-001
- Severity: MINOR
- File/class/method: [src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap](src/ortec/git/zcl_abapgit_ortec_obj_index.clas.abap) / ZCL_ABAPGIT_ORTEC_OBJ_INDEX / walk_filtered
- Evidence: The tree-walk loop checks whether each discovered file node belongs to the caller-supplied filter by using a linear internal-table membership test via line_exists on the full it_filter list inside the per-node loop. This is a repeated scan over the filter set for every visited tree node.
- Hidden call chain: walk_filtered -> decode_tree -> per-node file handling -> line_exists(it_filter[...])
- Multiplicity: One membership test per discovered file node during the commit-tree walk; scales with the number of traversed tree nodes and the size of the caller filter set.
- Scaling variable: FRONTIERS
- Why it matters: The design goal for the filtered walk is a bounded, caller-scoped path with near-constant filter membership checks, but this implementation reintroduces a per-node linear scan that can grow as O(F × K) for large trees and larger filter sets.
- Required review: Replace the repeated line_exists test with a hashed lookup structure keyed by object type and object name before the tree walk begins, then use that lookup for the per-node match check.

## Unverified paths
- No live SAP execution or runtime profiling was available for this scan; the assessment is based on the static implementation in the allowed source scope only.

## Evidence limits
- The scan did not inspect unrelated repository code outside the explicitly allowed scope and did not attempt runtime validation of large-repository behavior.
