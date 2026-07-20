---
name: git-protocol
description: Git protocol, pack, delta, and branch semantics
---

# Git protocol skill

Use this when analyzing or implementing remote Git behavior.

Protocol rules:
- Remote refs/branch tips are source of truth; local cache is only a cache.
- Discover refs/capabilities before requesting capabilities.
- Use have/want negotiation where supported.
- Do not request server capabilities that were not advertised.
- Request `thin-pack` only if the Ortec code can thicken packs by resolving all bases.
- Handle `OBJ_REF_DELTA` and `OBJ_OFS_DELTA` explicitly.
- If a delta base is missing, persist a missing-base state and fetch bases in bulk before exposing the object as loaded.
- Treat branch-specific knowledge and SHA-addressed object reuse separately.

Filtered staging strategy:
- Resolve branch tip and root tree.
- Walk only required path prefixes.
- Collect missing commit/tree/blob/base objects.
- Fetch in bulk/minimal requests.
- Persist and index first; status calculation consumes the local store on retry.

### Partial clone rules

- Partial clone and shallow clone solve different problems.
- `filter blob:none` may omit blob payloads but must not truncate commit history.
- `deepen N` is a shallow-history mechanism and must never be used as a
  completeness certificate.
- Intentionally omitted/promised blobs must be distinguishable from unexpected
  missing commits, trees, or delta bases.
- A branch graph certificate may be complete while historical blobs remain
  promised.
- A branch snapshot certificate is complete only when every blob reachable from
  the selected tip tree is locally READY and hash-verified.
- Do not fetch arbitrary blob/tree SHA wants unless the server advertised the
  applicable capability.
- If arbitrary wants are unavailable, use a bounded bulk strategy; never fall
  back to one request per object.
- A cold-branch blobless fetch should normally use:
  - no haves;
  - no shallow lines;
  - no deepen;
  - filter blob:none;
  - no thin-pack.
- An unfiltered no-have, no-deepen, non-thin branch-tip fetch is recovery, not
  the normal partial-clone path.
