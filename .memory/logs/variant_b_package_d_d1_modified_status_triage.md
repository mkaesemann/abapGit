# D1 SAP Closeout: False MODIFIED-status Triage

## Reported symptom

Owner report after live SAP validation of commit `8bfca36beffd27f405034426d917556fc7959564`:
"Many files are shown as Local and Remote MODIFIED even though no actual delta exists."

All other owner-executed validation passed: Activation/syntax PASS (except 3 SLIN
warnings addressed separately), targeted ABAP Unit PASS, ATC PASS (except the same
3 SLIN warnings), warm-branch functional test PASS, cold-branch functional test PASS.

## Method

Static source-evidence-based causal-chain trace only. No live SAP reproduction was
performed or is available in this environment; this classification is based entirely
on reading the actual productive call chain from D1's changed methods through to the
standard Stage/Diff status comparison, not on runtime observation of the anomaly.

## Call chain traced

1. `zcl_abapgit_ortec_pack_stream=>decode_streaming` (Package D1's PRIMARY/default
   decode path, used by `zcl_abapgit_ortec_fastpath=>upload_pack`): its promote loop
   already guarded `IF sy-tabix > lv_original_count. CONTINUE.` **before** D1's changes
   (confirmed via `git diff 7403a639..8bfca36b --name-status`: this file is not part of
   either D1 commit's diff at all). This path never had the external-base leak bug.
2. `zcl_abapgit_ortec_pack_dec=>resumable_decode` (D1's narrow crash-resume-only path,
   reached only via `zcl_abapgit_ortec_fastpath=>pull_by_branch`'s Phase-1 resume and
   `zcl_abapgit_ortec_pack_dec=>resume_decode`): the pre-correction leak
   (returning merged external delta-base objects in `rt_objects`) was confined to the
   **transient, in-memory return value**. The DB **persist** loop inside the same
   method already carried its own correct `IF sy-tabix > lv_original_count. CONTINUE.`
   guard from D1's original commit onward - persisted `zaog_obj_store` rows were never
   affected by the leak, only what a caller of `resumable_decode`/`resume_decode`
   received back in memory.
3. The one caller that surfaces `resumable_decode`'s return value to an outer result
   (`zcl_abapgit_ortec_fastpath=>pull_by_branch`, `rs_result-objects = lt_resumed.`)
   is reachable only through the crash-resume path - not the default fetch path
   (`upload_pack`, which always uses `decode_streaming`, never `decode_and_persist`;
   that fallback is currently disabled via an explicit re-raise per an existing
   2026-07-17 TODO in `zcl_abapgit_ortec_fastpath`).
4. Standard Stage/Diff status resolution never consumes this in-memory
   `rt_objects`/`rs_result-objects` value at all. The real chain is:
   `zcl_abapgit_ortec_git_facade=>resolve_filtered_remote` →
   `zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage` →
   `zcl_abapgit_ortec_obj_index=>get_files_for_filter` →
   `build_files_from_rows`, which fetches blob content exclusively via
   `zcl_abapgit_ortec_obj_store=>get_objects(iv_bulk_fetch = abap_true)` keyed by
   `blob_sha1` values read back from the **persisted** `zaog_obj_index` table - a
   completely independent, content-addressed read path that never touches any
   decode call's return value.

## Conclusion

The pre-correction `resumable_decode` return-value leak (fixed by the owner's
`8bfca36b` commit) could only ever have affected a caller's in-memory object count on
the narrow crash-resume path; it never wrote incorrect data to `zaog_obj_store`, and
the file-content-serving path used for standard Stage/Diff MODIFIED-status comparison
(`obj_index=>build_files_from_rows`) reads exclusively from persisted, content-addressed
storage independent of any decode-return-value bug. No source-level mechanism was
found by which any D1-owned method (`zcl_abapgit_ortec_delta`, `zcl_abapgit_ortec_pack_dec`,
`zcl_abapgit_ortec_pack_stream`) could cause a blob's *content* or a commit's *tree*
mapping to be persisted incorrectly.

The false-MODIFIED pattern matches a stale/inconsistent per-commit snapshot or index
state instead - squarely the domain of `zcl_abapgit_ortec_obj_index`'s `zaog_obj_index`
snapshot mechanism (its `$IDX/__READY__` completeness marker, STRICT vs RELAXED
absent-strictness modes, and retry-once-after-rebuild pattern), which is explicitly
outside D1's scope (delta/pack-decode/pack-stream layer only).

**Classification: PACKAGE_E_CONSUMER_COHERENCE**

## Acceptance criterion for the future Package E investigation

- Reproduce with a repository exhibiting the anomaly and capture `zaog_obj_index` rows
  for the affected commit alongside the actual working-tree/remote file SHA1s.
- Confirm whether `is_index_ready`'s STRICT/RELAXED mode or the `$IDX/__READY__` marker
  was satisfied for a snapshot that was not actually complete (e.g. a snapshot built
  from a commit reached via the crash-resume path, before D1's correction, whose
  in-memory object count briefly appeared inflated - even though nothing was persisted
  incorrectly, a downstream consumer that keyed completeness off object *count* rather
  than persisted content could have been misled by the pre-correction behavior).
- Confirm resolution by verifying the false-MODIFIED status clears with no `zaog_obj_index`
  rebuild required once the correct root cause is fixed.

## Limits of this evidence

This is a static-evidence classification, not a measured reproduction. No live SAP
session was used to reproduce the anomaly or inspect actual `zaog_obj_index` row
content in this environment. If a live reproduction later contradicts this chain
(e.g. proves `zaog_obj_index` rows themselves derive from a decode return value this
trace missed), this classification must be revised.
