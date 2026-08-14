# SMIM / SSFO post-WAPA performance discovery

Task: `SMIM_SSFO_PERFORMANCE_DISCOVERY_20260814`  
Baseline: IT8 active source, 2026-08-14  
Scope: named SMIM, SSFO, and serializer-prefetch methods only; no productive changes.

## Evidence and remaining per-object work

### SMIM

Observed: 1,987 objects, 195,868 ms, 15.4 MB. This is about 98.6 ms/object and 7.9 KB/object on average. The reported 1-3 KB objects taking 8-9 seconds are therefore dominated by fixed work or contention, not payload volume.

`ZIF_ABAPGIT_OBJECT~SERIALIZE` still performs, for every non-folder object:

- `CL_MIME_REPOSITORY_API=>IF_MR_API~GET` for the content XSTRING.
- `GET_FILENAME_AND_MIMETYPE`, which calls `SKWF_LOIO_ALL_PHIOS_GET` for the PHIO list and scans it.
- `SKWF_NMSPC_IO_ADDRESS_GET` through `GET_URL_FOR_IO`.
- XML/property assembly and one `mo_files->add` even when the content is small.

The active prefetch extension removes the `SMIMLOIO` singleton SELECT and the language-specific `SMIMPHF` SELECT when its cache hits. It does not prefetch MIME content, PHIO enumeration, or URL construction. `GET_SMIM_LOIO` and `GET_SMIM_PHF` are hashed-table reads, so their lookup cost is unlikely to explain multi-second small-object cases.

### SSFO

Observed: 1 object, 36,713 ms, 3.05 MB. The serializer loads the Smart Form and calls `XML_DOWNLOAD`, then walks the complete DOM. For matching source nodes it copies every child into an `abaptxt255` table and emits separate files; `CODE` additionally computes a path hash by walking ancestors and hashing the path.

After that traversal it performs two further complete-document iterator passes in `FIX_IDS`, then a complete-document pass in `SORT_TEXTS`. `SORT_TEXTS` additionally converts every `T_CAPTION` child row into `stxfobjt`, sorts it, and writes values back through DOM calls. Final `get_root_element`/`set_raw` materializes the resulting document.

## Ranked optimization candidates

### 1. SMIM: prove and reduce MIME API fixed cost (highest likely aggregate upside)

First candidate is not direct-table content replacement. Measure whether `IF_MR_API~GET` or the surrounding MIME repository call stack accounts for the 8-9 second small-object cases. If confirmed, investigate a supported batch/read API or a provider-level content prefetch that preserves MIME repository authorization, version/language, folder, and binary semantics. Do not bypass the repository tables without proof of semantic equivalence.

Risk: highest correctness risk. MIME repository APIs may enforce permissions, storage indirection, content versions, and error behavior not represented by `SMIMLOIO`/`SMIMPHF` alone. Any optimization must retain exact `mo_files` bytes and not-found/error parity.

### 2. SMIM: prefetch PHIO enumeration and URL inputs

Extend the existing prefetch only after trace evidence shows `SKWF_LOIO_ALL_PHIOS_GET` or `SKWF_NMSPC_IO_ADDRESS_GET` is material. Cache the exact API inputs/outputs, not a guessed URL format, and keep the API fallback for misses.

Risk: medium. Multiple PHIOs, language fallback, folders, and URL naming semantics need parity tests. This is a smaller likely win than content retrieval but safer than replacing MIME content access.

### 3. SSFO: isolate `XML_DOWNLOAD` before changing DOM passes

If `XML_DOWNLOAD` dominates, optimize at the Smart Form export boundary or cache only where object identity and invalidation are provable. If the DOM work dominates, consider a single-pass combined traversal for source extraction and ID normalization, but preserve node ordering and exact XML output.

Risk: high regression surface. `FIX_IDS` intentionally preserves ID/IDREF relationships, and `SORT_TEXTS` canonicalizes captions. Combining passes can change mutation order or output parity.

### 4. SSFO: avoid repeated DOM scans only with parity fixtures

The two `FIX_IDS` passes and the later `SORT_TEXTS` pass are concrete repeated traversal costs. A narrowly designed traversal/cache of node names, attributes, and parent paths could reduce iterator and virtual-method overhead. This is worthwhile only if SAT attributes show DOM calls/iterators are dominant after `XML_DOWNLOAD`.

Risk: high. DOM node mutation while iterating, namespace filtering, missing IDs, and caption-field insertion are observable. Require byte-for-byte serialized XML and extracted-file parity, not only successful activation.

### 5. SSFO: reduce allocation in `SERIALIZE_SOURCES`

`SERIALIZE_SOURCES` builds a fresh internal table of every source line before `mo_files->add_abap`, and `get_hash_for_path` recomputes an ancestor path for each `CODE` node. A per-document path cache or a streaming-compatible collector may help, but only if source extraction is visible in SAT.

Risk: medium. File ordering, line values, empty lines, and generated `iv_extra` names must remain identical.

## Focused SAT traces required

Use non-aggregated SAT traces with the serializer as the measured unit and retain call hierarchy plus DB/HTTP/kernel time. Run cold and warm variants where relevant; record object ID, payload bytes, total time, gross time, and memory.

1. **SMIM small slow case:** one 1-3 KB object known to take 8-9 s; trace `SERIALIZE` through `GET_URL_FOR_IO`, `SKWF_NMSPC_IO_ADDRESS_GET`, `CL_MIME_REPOSITORY_API~GET`, `SKWF_LOIO_ALL_PHIOS_GET`, `GET_FILENAME_AND_MIMETYPE`, `SMIMLOIO`, and `SMIMPHF`. Repeat the same object warm, then one ordinary median object and one larger object.
2. **SMIM batch shape:** trace a bounded run of 10-20 representative objects with per-call aggregation disabled or call-position detail sufficient to distinguish fixed API cost from payload scaling. Confirm whether time is in MIME content retrieval, metadata/FMs, XML/property work, or framework dispatch.
3. **SSFO single object:** trace `SERIALIZE` with separate call-tree attribution for `lo_sf->load`, `xml_download`, the main iterator loop, `SERIALIZE_SOURCES`, `GET_HASH_FOR_PATH`, `FIX_IDS`, `SORT_TEXTS`, and `io_xml->set_raw`. Capture DOM call counts and memory peaks if SAT exposes them.
4. **SSFO phase split:** if one trace is too coarse, use focused traces around `XML_DOWNLOAD`, `FIX_IDS`, and `SORT_TEXTS` with the same form and unchanged runtime conditions. This distinguishes export cost from canonicalization cost before any redesign.

## Total-upside prioritization

SMIM is the first target by aggregate evidence: 195.9 s across 1,987 objects means even a 20% reduction is roughly 39 s for this sample, while a fixed 10 ms/object reduction is roughly 20 s. SSFO is the first target by single-object severity: 36.7 s for one object, so it deserves immediate investigation if it is on a frequently used path, but its total batch upside depends on its object count and recurrence.

The allowed state artifact does not contain the numeric TABL/CLAS/FUGR/TTYP totals referenced by the task. Consequently, no defensible numeric ranking against those four families can be made here. The comparison gate is: prioritize SMIM ahead of a family only when its measured reducible time exceeds that family’s supplied total or when its 8-9 s fixed-cost tail is a user-visible outlier; prioritize SSFO ahead of them only when its object frequency or critical-path placement makes the 36.7 s singleton consequential. No productive optimization should start until those totals are reattached to the handoff or the owner supplies them.

## Recommendation

Run the four focused SAT exercises first. If SMIM confirms MIME API fixed cost, pursue a semantics-preserving provider/API optimization; if not, pursue PHIO enumeration/URL prefetch. For SSFO, do not merge DOM passes or alter XML export until the phase split proves the repeated walks are material and parity fixtures exist. No candidate is implementation-ready from the current evidence alone.