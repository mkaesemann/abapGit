# Variant B Package D — delta discovery

## 1) REF_DELTA / OFS_DELTA call chains

### Non-streaming path (pack decoder)
- `zcl_abapgit_ortec_pack_dec=>resumable_decode` parses each object, builds:
  - `lt_offset_map` for pack-offset → object-index lookups
  - `lt_ofs_meta` for every `ofs_d` entry with its resolved base offset
- After the parse/decode pass, it calls `zcl_abapgit_ortec_delta=>resolve_all(...)`.
- `resolve_all` performs two phases:
  1. repeated ascending sweeps with `iv_allow_thin_fetch = abap_false` to resolve only bases already reconstructable from the current pack
  2. one final ascending sweep with thin fetch enabled, so truly external bases can be loaded from `zaog_obj_store`
- `resolve_one` handles the actual per-delta work:
  - `ref_d`: finds an already-resolved base in the pack by its declared base SHA1, or falls back to `zcl_abapgit_ortec_obj_store=>get_object(...)` for a thin base
  - `ofs_d`: resolves its base from `lt_ofs_meta` + `lt_offset_map`, then recursively resolves that base if needed
  - in both cases it calls `zcl_abapgit_ortec_delta=>apply(...)` to reconstruct the result

### Streaming path (pack stream)
- `zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming` decodes one object at a time, persists each object under a temp key, and leaves delta rows unresolved for a later phase.
- `zcl_abapgit_ortec_pack_stream=>resolve_streaming` then runs the equivalent two-pass fixpoint:
  1. repeated in-pack-only sweeps (`iv_allow_thin_fetch = abap_false`)
  2. one final pass that allows object-store fetches and raises on truly missing bases
- `resolve_one_meta` is the streaming analogue of `resolve_one`:
  - `ref_d`: checks the in-memory SHA index first; if absent it uses `get_base_bytes(...)`
  - `ofs_d`: resolves the base via its `base_offset` using the in-memory offset index
  - it recursively resolves the base if the base is itself unresolved, then calls `zcl_abapgit_ortec_delta=>apply(...)`
- `decode_streaming` routes through `decode_and_persist_streaming` + `resolve_streaming` and returns only the sparse commit-object set expected by the higher layer.

## 2) Declared base identity vs resolved base payload

### Non-streaming representation
- For `ref_d`, the unresolved row’s `sha1` field is overloaded to carry the declared base SHA1, while the row’s `data` still contains the raw delta bytes.
- After resolution, the same row is promoted in place:
  - `type` becomes the resolved base type
  - `data` becomes the reconstructed payload
  - `sha1` becomes the final content SHA1
- For `ofs_d`, the declared base is not stored as a SHA1 in the object row itself. Instead, the decoder records it in `ty_ofs_meta` via `base_offset`, and the base is resolved from the pack-offset map.

### Streaming representation
- `ty_meta` separates the concepts cleanly:
  - `delta_base` stores the declared REF_DELTA base SHA1
  - `base_offset` stores the declared OFS_DELTA base offset
  - `sha1` remains blank until the delta is resolved
  - `temp_key` is the temp object-store key for the raw delta bytes
- Once resolved, `sha1`, `obj_type`, and `is_resolved` are updated, and the resolved payload is persisted under the final SHA1.

## 3) Recursion / fixpoint behavior and DB or HTTP calls inside per-delta resolution

### Recursion / fixpoint
- Both resolvers use a bounded recursive chain walk with `c_max_chain_depth = 64` in `zcl_abapgit_ortec_delta`.
- The non-streaming resolver uses a fixpoint-driven sweep strategy: repeated passes continue until no new delta resolves in a pass, which lets a chain of deltas resolve even when the base appears later in the pack.
- The streaming resolver uses the same two-pass strategy with an explicit in-pack pass followed by a final pass that allows external-base lookup.

### DB / HTTP calls during per-delta resolution
- Non-streaming:
  - per-delta resolution can issue a DB lookup via `zcl_abapgit_ortec_obj_store=>get_object(...)` when the base is external or thin
  - no HTTP calls are made by the delta resolver itself
- Streaming:
  - `get_base_bytes(...)` first checks the in-memory base cache, then calls `zcl_abapgit_ortec_obj_store=>get_object(...)` if absent
  - the current implementation does not issue any HTTP fetch during missing-base repair; `complete_missing_base(...)` is a permanent no-op

## 4) How external bases are loaded via base_cache / obj_store

### Non-streaming path
- Before final delta resolution, `resumable_decode` collects the SHA1s of all `ref_d` bases that are not already present in the current pack.
- It performs one bulk `SELECT ... FOR ALL ENTRIES` from `zaog_obj_store` for those bases and appends the fetched objects into `rt_objects` as synthetic objects with a real, unique `index`.
- That is a bulk external-base load step, not a per-delta loop.
- If a base is still not found after that, `resolve_one` falls back to a per-base `get_object(...)` lookup.

### Streaming path
- External bases are looked up in a cache-first, per-base manner:
  1. `zcl_abapgit_ortec_base_cache=>get_instance(...)`
  2. `has(...)` / `get(...)` from the LRU cache
  3. if absent, `zcl_abapgit_ortec_obj_store=>get_object(...)`
- The cache exists to avoid repeated DB reads when several deltas share the same base in the same pass.

## 5) Existing bounded recovery tier for missing bases

- Non-streaming: there is no dedicated bounded recovery tier in the current code path. Missing bases eventually raise `Delta base not found, <sha1>` after the final pass.
- Streaming: a recovery method exists as `complete_missing_base(...)`, but it is currently permanently disabled and always returns `abap_false` without calling the fastpath or issuing HTTP. Missing bases therefore escalate to an exception with `iv_retry_without_haves = abap_true`, so the caller can decide on a broader retry, rather than performing a per-object repair.
- The code also carries `c_max_completion_attempts = 20` and `gv_completion_attempts` for a future bulk-resolution design, but they are not currently used.

## 6) Memory / XSTRING copy patterns for base payloads

- `zcl_abapgit_ortec_delta=>apply(...)` reconstructs the result incrementally using byte-wise concatenation from the base bytes and delta instructions; it does not keep an extra full-pack copy in memory.
- `zcl_abapgit_ortec_base_cache` stores full `xstring` payloads in its LRU entries and uses a byte budget (`c_budget_bytes = 268435456`) to bound memory usage.
- The streaming resolver avoids holding more than one resolved delta payload in memory at a time; it reads the raw delta bytes from the object store, applies them, persists the result, and clears the local result variable.
- The non-streaming decoder also bounds temporary memory by clearing intermediate `xstring` variables after trailer hashing and by using batched persistence rather than keeping a second full copy of every object around.

## 7) Existing ABAP Unit test methods touching REF / OFS / mixed chains, missing or corrupt bases, and external-base loads

### REF / external-base coverage
- `ltcl_pack_stream=>ref_delta_stays_unresolved`
  - exercises a `ref_d` object with an external base stored in `zaog_obj_store`
  - asserts that the row remains unresolved after the streaming scan and retains its declared base SHA1 and temp key

### Missing / corrupt-base / failure cleanup
- `ltcl_pack_decoder=>cleanup_after_decode_failure`
  - corrupts the trailing pack SHA1 and verifies the non-streaming decoder cleans up temp rows and bookkeeping
- `ltcl_pack_stream=>corrupt_trailer_no_rows`
  - corrupts the trailing pack SHA1 and verifies the streaming decoder removes all incomplete rows after failure

### External-base load coverage
- `ltcl_pack_decoder=>prefetch_bases_do_not_collide`
  - validates the bulk prefetch of two distinct external delta bases and ensures they do not collide in the index bookkeeping

### OFS / mixed REF+OFS chain coverage
- No dedicated ABAP Unit test methods for `ofs_d` or mixed `ref_d`/`ofs_d` chains were found in the scanned classes.
