# WAPA payload discovery

## Verified call chain
- WAPA object entry: ZCL_ABAPGIT_OBJECT_WAPA=>zif_abapgit_object~serialize routes to ZCL_ABAPGIT_ORTEC_WAPA=>serialize when the switch is active.
- Batch path: Z_ABAPGIT_ORTEC_SER_BATCH exports the full serialization result as FILES_XSTRING, and ZCL_ABAPGIT_ORTEC_SER_ORCH=>MERGE_INTO_MT_FILES imports and merges it back into the run.
- Dominant payload origin: page content materialization in ZCL_ABAPGIT_ORTEC_WAPA=>ADD_PAGE_CONTENT_FILE creates the large bytes, then the batch layer transports and merges them.

## Cost centers
- Serializer CPU: page-content generation, page details/parameter assembly, XML/XSTRING conversion, and repeated page materialization.
- DB: page/parameter reads from O2PAGDIR/O2PAGCON/O2PAGPAR/O2PAGPART/O2PAGEVH and related context reads.
- XML/XSTRING copy: content conversion and transport of the FILES_XSTRING payload.
- RFC payload: batch export/import of the full serialization result.
- Merge: per-file append into the orchestrator run accumulator.

## Hard limits already present
- Raw-prefetch rows: capped at 20,000 rows; larger results fall back from the fast path.
- Raw-prefetch decoded output: capped at 20,971,520 bytes; larger decoded output stops the fast path.
- No final-output cap in ZCL_ABAPGIT_ORTEC_WAPA itself; the 791 MB figure is an observed output size, not a code-enforced ceiling.

## Cache viability
- No WAPA-specific persisted cache exists in the current source scope.
- The serializer is required to preserve byte-for-byte parity with the legacy path, so any cache would need a proven change identity and output-parity guarantee before it could be treated as safe.

## Recommended next evidence
- Breakpoint ADD_PAGE_CONTENT_FILE for per-page sizes.
- Breakpoint around batch FILES_XSTRING export/import.
- Breakpoint around raw-prefetch success/fallback and cap-hit conditions.
