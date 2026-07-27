# Performance scan: Variant B / Package D1 (generalized bounded external delta-base resolution)

Mode: static call-chain scan (mandatory performance gate, senior implementation
agent). No dedicated `ortec-abapgit-performance-scan` subagent was available in
this session's agent roster (only `ortec-abapgit-implementation-junior` and
`ortec-abapgit-regression` are registered) - this scan was performed directly
by the senior implementation agent instead of being delegated, per the mode's
fallback expectation that the gate itself is mandatory regardless of delegate
availability.

## Scope

Exact D1 call chain scanned:

- `zcl_abapgit_ortec_delta=>resolve_all`
- `zcl_abapgit_ortec_delta=>resolve_one`
- `zcl_abapgit_ortec_delta=>bulk_resolve_external_bases` (shared helper)
- `zcl_abapgit_ortec_pack_dec=>resumable_decode`
- `zcl_abapgit_ortec_pack_stream=>resolve_streaming`
- `zcl_abapgit_ortec_pack_stream=>resolve_one_meta`
- `zcl_abapgit_ortec_pack_stream=>preload_delta_rows`
- `zcl_abapgit_ortec_pack_stream=>preload_external_bases`
- `zcl_abapgit_ortec_obj_store=>get_objects` / `get_object` (object-store APIs
  D1 calls into)

## SQL-call shape

- `bulk_resolve_external_bases` issues **exactly one** call to
  `zcl_abapgit_ortec_obj_store=>get_objects( iv_bulk_fetch = abap_true )` per
  `resolve_all`/`resolve_streaming` invocation (i.e. once per pack, at most
  once per decode/resume attempt) - never once per external base.
- Inside `get_objects`, the `iv_bulk_fetch = abap_true` branch deliberately
  does **not** chunk at `c_select_package_size` (unlike the non-bulk branch,
  and unlike `get_present_sha1s`/`has_dangling_delta_base` elsewhere in the
  same class) - it builds one `lt_package` RANGE table from every missing
  SHA1 and calls `read_object_rows` **once**, which issues **one** `SELECT *
  FROM zaog_obj_store ... WHERE obj_sha1 IN lr_sha1s AND status = 'R'`.
  - This is intentional and matches the approved design ("one bulk call"
    invariant, verified by `bulk_base_one_call_only`/`no_sql_in_pack_phase`).
  - It is safe at scale because ABAP OpenSQL's DB interface transparently
    splits an oversized `IN`-range into multiple physical statements to the
    underlying database (a long-standing ABAP kernel behavior, not specific
    to this code) - so no single external-base set, however large, can
    produce an SQL statement that is rejected by the DB for having too many
    `IN` predicates. The `lt_package`/RANGE table construction is O(N)
    ABAP-side memory/CPU (N = distinct external bases in this pack), never
    O(N²).
  - Realistic upper bound on N: a single pack's own external-base fan-out is
    bounded by that pack's total delta-object count (never the whole
    repository), which for a normal incremental fetch is small (tens to a
    few hundred), and even for an unusually large single pack is still
    orders of magnitude below the repository's total stored-object count.
- `preload_delta_rows`/`preload_external_bases` (streaming side) reuse the
  same shared `bulk_resolve_external_bases` helper and the streaming LRU
  base-cache; guards (`ct_sha_idx`/cache membership checks) prevent
  re-reading any payload already resolved in phase 1.5, so streaming issues
  the same "one bulk call per pack" shape, not one per delta.
- `resolve_one`'s dead-path thin-fetch fallback (`get_object`, singular) is
  the only per-object DB call in this chain, and it is unreachable in the
  normal path once phase 1.5 has run (proved by `no_sql_in_pack_phase`/
  `no_thin_fetch_for_ext_base`, both asserting the corresponding call
  counter stays at zero).

## HTTP-call shape

- D1 introduces no new HTTP call sites. External-base resolution reads
  exclusively from the persistent object store (`zaog_obj_store`), which is
  already populated by prior fetch/pull attempts - it never triggers a new
  network round trip. Confirmed by direct reading of
  `bulk_resolve_external_bases` and `resolve_streaming`: neither contains an
  HTTP client reference.

## Row / byte batch limits

- Bulk fetch: one unbounded (per-pack) row set via a single `SELECT`, sized
  to the pack's own external-base fan-out (see above) - not batched by rows,
  but bounded by the physical pack's own delta count, which is itself
  bounded by the server's pack-generation limits, not by repository size.
- Object payloads returned by `get_objects` are the same `obj_data` XSTRING
  column already used everywhere else in this class - no new payload-size
  handling was introduced by D1; existing byte-budget/streaming safeguards
  in the surrounding decode paths are unchanged.

## Hidden singleton APIs in loops

- None found. `bulk_resolve_external_bases` and `get_objects` are each
  called at most once per pack per resolver (`resolve_all`/
  `resolve_streaming`), never from inside a per-object loop.
- `resolve_one`'s recursive OFS-chain call is bounded by
  `c_max_chain_depth = 64` (raises past that), and only ever touches
  in-memory `ct_objects`/`ct_tabix_by_index` - no DB/HTTP call inside the
  recursion itself.

## Maximum simultaneously held payloads

- `lt_loaded_bases`/`rt_objects` in `bulk_resolve_external_bases` hold one
  in-memory copy of every distinct external base's payload for the
  *current pack* only (freed once `resolve_all`/`resolve_streaming` for
  that pack completes) - bounded by the same per-pack external-base fan-out
  as the SQL shape above, not by total repository size.
- The streaming side additionally holds these in the LRU base cache
  (bounded eviction, per existing Package B/C invariants - unchanged by
  D1).

## Large-repository acceptance scenario (definition, evaluated in the
## implementation audit below)

1. **1 object, 0 external bases** - `resolve_all`/`resolve_streaming` must
   not call `bulk_resolve_external_bases` at all (empty
   `lt_external_bases`), zero SQL beyond whatever the surrounding
   decode/promote path already issues.
2. **1,000 objects, ~50 external bases** (typical incremental fetch) - one
   bulk `get_objects` call for the ~50 distinct bases, regardless of how
   many of the 1,000 objects reference them.
3. **40,000 objects in one pack, all external REF_DELTA bases** (worst
   case, unrealistic for a real git fetch but a valid stress bound) - still
   exactly one bulk `get_objects` call; the `IN`-range is large but ABAP
   OpenSQL splits it transparently at the DB layer.
4. **~100 objects affected out of a 1,000,000-row `zaog_obj_store` table**
   - the `WHERE obj_sha1 IN (...) AND status = 'R'` predicate is indexed on
     `repo_key`+`obj_sha1` (existing primary/secondary key, unchanged by
     D1); reading ~100 targeted rows out of 1,000,000 is a normal indexed
     lookup, not a table scan - no new index requirement introduced by D1.

## Verdict

Static call-shape evidence is consistent with the approved D1 design and the
mandatory performance gate: no per-object SQL, no per-object HTTP, exactly
one bulk external-base load per pack per resolver, and no unbounded
in-memory accumulation beyond one pack's own external-base set. This scan
found **no blocking or major performance findings**. One documented,
non-blocking design characteristic (the bulk-fetch branch is intentionally
un-chunked, relying on OpenSQL's transparent `IN`-list splitting) is carried
forward into the implementation audit below for an explicit verdict.
