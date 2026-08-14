# Performance Scan

## Scope
- Topic: WAPA Option 1 post-repair performance scan
- Slice: Production `SERIALIZE` -> `RAW_PREFETCH_AND_READ` -> `READ_RAW_MANIFEST` / `READ_RAW_ROWS` / `DECODE_RAW_PAGE` / `SERIALIZE_REFERENCE_RANGE` -> `READ_PAGE`
- Entry methods: `SERIALIZE`, `RAW_PREFETCH_AND_READ`
- Files inspected: active IT8 main source `ZCL_ABAPGIT_ORTEC_WAPA` only
- Expected cardinality: 1,000-page groups; depth-5 bounded split; acceptance up to 40,000 pages; `/O4H/COMPANION` 11,307 pages

## Summary
- Verdict: CLEAN
- Estimated SQL shape: Three raw-helper SELECTs for an admitted range (manifest, payload, verification); rejected ranges split contiguously through depth 5; terminal reference fallback performs one unchanged `READ_PAGE` per page and no raw-helper SELECTs.
- Estimated HTTP shape: None in the scoped path.
- Estimated memory risk: Prior production duplicate-row-copy and growing-XSTRING blockers are closed in the active dispatch. Runtime SAT proof of kernel IMPORT transient ownership remains outside this static scan.

## Findings

### PS-001
- Severity: CLOSED
- File/class/method: `ZCL_ABAPGIT_ORTEC_WAPA` / production `RAW_PREFETCH_AND_READ` -> `DECODE_RAW_PAGE`
- Evidence: `SERIALIZE` calls `RAW_PREFETCH_AND_READ` directly for each 1,000-page group. `DECODE_RAW_PAGE` traverses `ct_rows` with `USING KEY by_page`; it does not copy/sort the complete admitted row table. The former `ASSEMBLE_AND_DECODE` still contains the old copy/sort implementation, but its only caller is `TRY_RAW_PREFETCH`, which is not called by `SERIALIZE` and is unreachable from the production entry in this class.
- Multiplicity: One page-local keyed traversal per page on the admitted production range.
- Scaling variable: N and physical payload bytes.
- Why it matters: The active production path no longer creates the prior second whole-range row table or repeats whole-range sorting; PS-001 is not present in the production path.
- Required review: None for this static finding. Keep the stale private helper out of production dispatch or remove it in a separate cleanup task.

### PS-002
- Severity: CLOSED
- File/class/method: `ZCL_ABAPGIT_ORTEC_WAPA` / `DECODE_RAW_PAGE`
- Evidence: Each page uses `lt_fragments` to collect fragment XSTRINGs and performs one `CONCATENATE LINES OF lt_fragments INTO lv_buffer IN BYTE MODE` per logical-key boundary, rather than concatenating the growing destination once per physical row. No incremental growing-XSTRING concatenate exists in the active `RAW_PREFETCH_AND_READ` production path.
- Multiplicity: One final assembly concatenate per logical PAGE/EVHNDL/TYPES key in the current page.
- Scaling variable: byte batches.
- Why it matters: The prior per-fragment quadratic concatenate shape is absent from the active production decoder.
- Required review: None for this static finding. Runtime memory measurement remains an acceptance gate.

### INV-01
- Severity: VERIFIED
- File/class/method: `ZCL_ABAPGIT_ORTEC_WAPA` / `RAW_PREFETCH_AND_READ`
- Evidence: `SERIALIZE` batches pages at `c_raw_prefetch_initial_pages = 1000`; rejected ranges split at `lines( it_pages ) / 2` while depth is below 5; depth-5 ranges call `SERIALIZE_REFERENCE_RANGE`. The fallback method contains no raw admission, split, or recursive fallback call.

### INV-02
- Severity: VERIFIED
- File/class/method: `ZCL_ABAPGIT_ORTEC_WAPA` / `DECODE_RAW_PAGE`
- Evidence: `TY_RAW_ROW_TT` declares sorted secondary key `by_page` on `pagekey objtype srtf2`; the decoder loops using that key. `DELETE ct_rows WHERE pagekey = iv_pagekey` consumes the current page rows before the method returns, and callers free the remaining row/manifest tables on fallback or completion.

## Unverified paths
- Runtime SAT evidence for the exact kernel IMPORT transient and peak application-owned page payload was not available.
- Acceptance execution for the 40,000-page fixture and `/O4H/COMPANION` zero-fallback requirement was outside this static main-source scan.
- The source still contains the private legacy `TRY_RAW_PREFETCH` / `ASSEMBLE_AND_DECODE` implementation; no production `SERIALIZE` call reaches it, but its dead-code cleanup was not performed.

## Evidence limits
- Active source existence and line content were confirmed from IT8 ADT reads of `ZCL_ABAPGIT_ORTEC_WAPA` main source (1,633 lines).
- No productive ABAP/DDIC source was edited. No state, archive, diagram, or unrelated repository path was inspected.
- Static verdict only: `CLEAN` means no scoped production PS-001/PS-002 blocker or new blocking pattern was found; it is not production-performance approval.