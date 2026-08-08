# Performance Scan

## Scope
- Topic: SER-SLICE-4 adaptive batch prefetch providers for TABL/PROG/FUGR
- Slice: new/changed code only
- Entry methods: prepare_tabl, prepare_fugr, get_tabl_i18n, get_tabl_extras, extract_for_batch_tabl/prog/fugr, inject_batch_from_buffer_tabl/prog/fugr, clear_tabl_cache/prog_cache/fugr_cache, before_dispatch, sum_provider_buffer_bytes, dispatch_batch
- Files inspected:
  - src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap
  - src/objects/tabl/zcl_abapgit_object_tabl.clas.abap
  - src/objects/zcl_abapgit_object_fugr.clas.abap
  - src/objects/zcl_abapgit_object_prog.clas.abap
  - src/ortec/serial/core/zcl_abapgit_ortec_ser_orch.clas.abap
  - src/ortec/serial/rfc/zabapgit_ortec_serial.fugr.z_abapgit_ortec_ser_batch.abap
- Expected cardinality: large batch of repository objects

## Summary
- Verdict: FINDINGS
- Estimated SQL shape: bulk FOR ALL ENTRIES in prepare_tabl/prepare_fugr; no new per-object SQL loop was found in the batch-provider path.
- Estimated HTTP shape: none in the inspected slice.
- Estimated memory risk: medium; the main risk is a new O(N^2) validation path in batch injection.

## Findings

### PS-001
- Severity: MAJOR
- File/class/method: src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap / ZCL_ABAPGIT_ORTEC_SER_PREF_EXT / inject_batch_from_buffer_tabl, inject_batch_from_buffer_prog, inject_batch_from_buffer_fugr
- Description: The new batch injectors validate payload-to-entry correlation by doing READ TABLE on a standard lt_p_entries table inside loops over lt_extras/lt_text/lt_prog/lt_areat/lt_enlfdir/lt_func. That creates an O(N^2) linear-scan pattern for each batch and will scale poorly as batch size grows.
- Hidden call chain: before_dispatch -> extract_for_batch_* -> inject_batch_from_buffer_*
- Multiplicity: O(K^2) in the number of batch entries for the correlation validation step.
- Scaling variable: K
- Why it matters: This is the main new static performance risk in the slice and it is exactly the kind of pattern that becomes expensive once batches contain many objects.
- Required review: Consider hashing or sorting the entry table before the correlation checks, or replacing the repeated linear lookups with a keyed lookup structure.
- New regression: Yes, introduced by this slice.

### PS-002
- Severity: MINOR
- File/class/method: src/ortec/serial/zcl_abapgit_ortec_ser_pref_ext.clas.abap / ZCL_ABAPGIT_ORTEC_SER_PREF_EXT / extract_for_batch_tabl, extract_for_batch_prog, extract_for_batch_fugr
- Description: The new extractors compute each entry's actual-byte admission value by calling extract_for_object( ) and xstrlen( ) inside the batch loop. That repeats per-object full-buffer work for every batch member just to size the envelope, which is extra work that can dominate large-batch cost even though the code remains functionally correct.
- Hidden call chain: before_dispatch -> extract_for_batch_* -> extract_for_object
- Multiplicity: One full-object extraction and byte-size calculation per batch entry.
- Scaling variable: K
- Why it matters: It adds repeated payload work to the batch path and can become a meaningful cost center when many objects are batched together.
- Required review: If byte admission is needed, consider whether a lighter-weight size proxy or a cached size value can be used instead of materializing a full object buffer per object.
- New regression: Yes, introduced by this slice.

## Unverified paths
- No live SAT/ST05 or real batch execution data was available in this scan, so the findings above are static-risk observations rather than measured runtime bottlenecks.

## Evidence limits
- This scan was intentionally limited to the supplied scope and directly invoked methods; no full-repository scan or live runtime validation was performed.
