# Performance Scan

## Scope
- Topic: variant-b-partial-clone
- Slice: Package D2
- Entry methods: zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming, zcl_abapgit_ortec_obj_store=>get_staged_delta_objects, zcl_abapgit_ortec_pack_dec=>resume_decode/resumable_decode, zcl_abapgit_ortec_fastpath=>pull_by_branch/persist_pull_result, zcl_abapgit_ortec_porcelain=>pull_by_branch
- Files inspected: src/ortec/git/zcl_abapgit_ortec_obj_store.clas.abap, src/ortec/git/zcl_abapgit_ortec_pack_stream.clas.abap, src/ortec/git/zcl_abapgit_ortec_pack_dec.clas.abap, src/ortec/git/zcl_abapgit_ortec_pack_raw.clas.abap, src/ortec/git/zcl_abapgit_ortec_fastpath.clas.abap, src/ortec/git/zcl_abapgit_ortec_porcelain.clas.abap, src/ortec/git/zaog_obj_store.tabl.xml, src/ortec/git/zaog_fetch_sess.tabl.xml, src/ortec/git/zaog_pack_meta.tabl.xml
- Expected cardinality: one decode session / one publication unit per pull, with SHA1 lists bounded to the current pack or current object set rather than repository-wide traversal

## Summary
- Verdict: CLEAN
- Estimated SQL shape: bounded package reads for SHA1 sets; status promotion and attempt-id correlation are implemented as set-based updates on the current pack scope, not per-object SQL inside a loop
- Estimated HTTP shape: no new HTTP requests were introduced by the D2 changes; the new repo-lock spans do not cover any transport call
- Estimated memory risk: low; the new staged-delta read is chunked and the lock spans are limited to the intended resume/persist call sequence

## Findings
- No findings in the inspected D2 scope.

## Unverified paths
- None; this is a static scan only.

## Evidence limits
- No runtime execution, SQL trace, or network trace was performed; conclusions are based on the inspected source and diff only.
