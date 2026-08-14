# Performance Scan

## Scope
- Topic: WAPA_PAYLOAD_PERF Option 1
- Slice: WAPA raw prefetch, bounded split, decode, and terminal reference fallback
- Entry methods: `SERIALIZE` -> `RAW_PREFETCH_AND_READ` -> `READ_RAW_MANIFEST` / `READ_RAW_ROWS` / `DECODE_RAW_PAGE` / `SERIALIZE_REFERENCE_RANGE` -> `READ_PAGE`
- Files inspected: active ADT source `ZCL_ABAPGIT_ORTEC_WAPA` main source only
- Expected cardinality: groups of up to 1,000 pages; acceptance fixture 40,000 pages; `/O4H/COMPANION` 11,307 pages

## Summary
- Verdict: FINDINGS
- Estimated SQL shape: Healthy admitted group performs 3 raw-helper SELECTs; rejected groups perform metadata probes while splitting; terminal reference fallback performs 1..3 unchanged cluster imports per page.
- Estimated HTTP shape: No HTTP/aRFC calls in the scoped path.
- Estimated memory risk: Blocking duplicate raw-row/page-scale copies and potentially quadratic XSTRING growth during assembly.

## Findings

### PS-001
- Severity: BLOCKING
- File/class/method: `ZCL_ABAPGIT_ORTEC_WAPA` / `ASSEMBLE_AND_DECODE`
- Evidence: `lt_sorted = it_rows` copies the complete admitted payload table, then `SORT lt_sorted`; `TY_RAW_ROW_TT` is a standard table with default key.
- Hidden call chain: `RAW_PREFETCH_AND_READ` -> `READ_RAW_ROWS` -> `DECODE_RAW_PAGE` -> `ASSEMBLE_AND_DECODE`.
- Multiplicity: Once per admitted range, plus once per page from `DECODE_RAW_PAGE`.
- Scaling variable: N and physical payload bytes.
- Why it matters: The approved lifetime requires one admitted raw range plus one decoded page. This creates a second whole-range row table and repeats sorting for every page, violating WAPA-INV-03 and the approved no-whole-row-copy decoder design.
- Required review: Replace the full-table copy/sort path with the approved keyed, page-local consuming traversal and verify peak memory at 40,000-page acceptance.

### PS-002
- Severity: BLOCKING
- File/class/method: `ZCL_ABAPGIT_ORTEC_WAPA` / `ASSEMBLE_AND_DECODE`
- Evidence: Each fragment is appended with `CONCATENATE <ls_buffer>-buffer lv_chunk ... IN BYTE MODE` inside the physical-row loop.
- Hidden call chain: `RAW_PREFETCH_AND_READ` -> `ASSEMBLE_AND_DECODE` and again through `DECODE_RAW_PAGE`.
- Multiplicity: Once per physical O2PAGCON row; potentially many rows per logical key.
- Scaling variable: byte batches.
- Why it matters: Repeated growing-XSTRING concatenation can copy the accumulated buffer on every fragment, producing quadratic time and transient memory amplification for large logical payloads.
- Required review: Measure and replace with the approved bounded assembly strategy; include a large multi-fragment key fixture and peak-memory evidence.

### PS-003
- Severity: MAJOR
- File/class/method: `ZCL_ABAPGIT_ORTEC_WAPA` / `DECODE_RAW_PAGE`
- Evidence: `LOOP AT ct_rows ... APPEND <ls_row> TO lt_page_rows` copies all current-page rows, then `DELETE ct_rows WHERE pagekey = iv_pagekey`; `ASSEMBLE_AND_DECODE` subsequently copies `lt_page_rows` again into `lt_sorted`.
- Hidden call chain: `RAW_PREFETCH_AND_READ` page loop -> `DECODE_RAW_PAGE` -> `ASSEMBLE_AND_DECODE`.
- Multiplicity: Once per requested non-controller page in an admitted range.
- Scaling variable: N and page payload bytes.
- Why it matters: Current-page CLUSTD rows remain in the admitted table until after a second page-local copy is built, contrary to the required delete-after-append lifetime. This increases simultaneous payload ownership and undermines WAPA-INV-11.
- Required review: Confirm row lifetime during IMPORT with SAT or equivalent runtime evidence; ensure current-page source rows are consumed and freed before decoding/import.

### PS-004
- Severity: MAJOR
- File/class/method: `ZCL_ABAPGIT_ORTEC_WAPA` / `READ_RAW_MANIFEST`
- Evidence: Manifest validation checks only negative SRTF2 and CLUSTR range. It does not explicitly validate SRTF2 contiguity, duplicate rows, mandatory PAGE presence, or non-contiguous requested results before admission.
- Hidden call chain: `RAW_PREFETCH_AND_READ` admission at every recursive node.
- Multiplicity: One metadata probe per attempted node; up to the bounded split-tree ceiling.
- Scaling variable: FRONTIERS and N.
- Why it matters: Malformed or incomplete manifests can be admitted and only fail later in payload/decode, weakening rejected-parent payload avoidance and the required admission contract. Duplicate insertion into the unique manifest key may also raise instead of producing the specified anomaly fallback.
- Required review: Implement or verify explicit manifest structural validation and make every anomaly follow the bounded split/terminal-reference rule without an uncontrolled exception.

### PS-005
- Severity: MAJOR
- File/class/method: `ZCL_ABAPGIT_ORTEC_WAPA` / `SERIALIZE`
- Evidence: `GV_RAW_PREFETCH_HITS` or `GV_RAW_PREFETCH_FALLBACKS` is incremented after each 1,000-page group. A mixed request can increment both counters, and increments are plain additions rather than saturating operations.
- Hidden call chain: `SERIALIZE` group loop -> `RAW_PREFETCH_AND_READ`.
- Multiplicity: Once per group, not once per serialize call.
- Scaling variable: N / group count.
- Why it matters: This violates the stated counter contract and makes acceptance diagnostics unreliable for mixed success/fallback requests; overflow can also wrap counters.
- Required review: Reconcile counters at serialize scope, guarantee exactly one of hit/fallback per request, and apply saturation to all counters.

## Unverified paths
- Active-server re-read independent of the ADT editor buffer was not available in this scan tool set; source evidence is from the current active ADT workspace source.
- Runtime SAT proof for decoded-page ownership, kernel IMPORT transient count, and `/O4H/COMPANION` zero-fallback acceptance was not available.
- Test include and 40,000-page acceptance execution were outside the requested main-source-only scope.

## Evidence limits
- No productive source, DDIC object, state file, archive, diagram, or repository-wide file was edited or inspected.
- Reference `READ_PAGE` cluster-import multiplicity is unchanged and only classified here as the documented fallback path.