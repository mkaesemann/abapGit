---
name: persistence-schema
description: Persistent Git object, pack, delta, and path indexing design
---

# Persistence/schema skill

Analyze and design around:
- `zaog_obj_store`
- `zaog_obj_index`
- `zaog_pack_idx`
- `zaog_pack_meta`
- `zaog_raw_pack`
- related `zaog_*` tables.

Goals:
- Bulk lookup by SHA1/object IDs.
- Pack -> object and object -> pack/offset mapping.
- Delta dependency tracking and base resolution status.
- Lazy tree-entry and path indexes for filtered staging.
- Shared object reuse across branches by SHA1.
- Branch/ref metadata kept separate from SHA-addressed object content.
- Sparse local state queryable without scanning all packs.

Schema changes are allowed, but every schema change proposal must include:
- purpose,
- exact table/index change,
- migration/backfill plan,
- compatibility risk,
- performance impact,
- rollback/clear-cache approach.
