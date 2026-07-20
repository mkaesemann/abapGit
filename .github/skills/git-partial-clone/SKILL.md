---
name: git-partial-clone
description: Blobless partial-clone and branch snapshot materialization rules
---

## Git partial-clone skill

Use for cold branch initialization, partial clone, promisor objects, branch
snapshot materialization, and large-repository recovery.

### Target model

- One physical SHA-addressed object store per repository.
- Branch/ref state is separate from object payloads.
- Unknown branch initialization fetches the unbounded commit/tree graph using
  `filter blob:none`.
- Current-tip blobs are materialized in bounded bulk requests.
- Shared objects are reused across all branches.
- Historical promised blobs may remain absent.
- Current-tip referenced blobs may not remain absent after snapshot completion.

### Forbidden substitutes

- Progressive deepen as a completeness strategy.
- Numeric deepen values treated as full history.
- Physical object stores per branch.
- One HTTP request per missing object.
- One SQL request per delta base.
- Full all-refs repository clone as the normal initialization path.
- Bare haves from uncertified or shallow-ambiguous commits.

### Fetch modes

- INITIAL_BRANCH_BLOBLESS
- INCREMENTAL_THIN
- INCREMENTAL_SELF_CONTAINED
- MATERIALIZE_BLOBS
- RECOVERY_BRANCH_FULL

Every implementation or review must validate the exact wire invariants of the
selected mode.

### Completeness levels

- GRAPH_COMPLETE:
  requested commit and complete tree closure are present; historical blobs may
  be promised.
- SNAPSHOT_COMPLETE:
  every blob referenced by the selected tip tree is present and verified.
- FULL_COMPLETE:
  all required reachable objects including historical blobs are present.

Never treat these levels as interchangeable.

### Scaling rules

- Blob wants and DB reads are row- and byte-bounded.
- No SQL in recursive tree walking.
- No network request per object.
- Delta external bases are deduplicated and bulk-loaded.
- Full branch recovery must be protected by an explicit memory-risk gate while
  HTTP responses are materialized as one XSTRING.

For all implementation slices, also apply:
`.github/skills/abap-performance-patterns/SKILL.md`
