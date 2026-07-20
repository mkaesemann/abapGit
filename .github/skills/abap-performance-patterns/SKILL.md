---
name: abap-performance-patterns
description: Mandatory ABAP database, batching, graph-walk, buffering, XSTRING, and
  large-repository performance rules. Bulk DB access and table-key patterns for large-scale
  ABAP (abapGit object stores/caches)
---

# ABAP performance patterns

Use this skill for every implementation or review that can process repository
objects, pack entries, commits, trees, blobs, paths, work items, or other
collections that may contain hundreds or thousands of rows.

Performance is part of functional correctness for production-scale processing.
An implementation that is logically correct but performs one SQL or HTTP
interaction per object is not complete.

## 1. Mandatory performance design before coding

Before implementing a potentially large-scale path, write a short performance
map containing:

- expected production cardinality;
- number of SQL statements per operation;
- number of HTTP requests per operation;
- maximum simultaneously held payload bytes;
- table lookup complexity;
- batching dimensions;
- transaction boundaries;
- cache invalidation frequency;
- expected behavior for 1, 1,000, 40,000, and 1,000,000 objects.

If these values cannot be estimated from the current design, the design is not
ready for implementation.

Do not postpone bulk behavior to a later optimization phase when production
cardinality is already known to be large.

## 2. Database round-trip rules

Never perform `SELECT`, `SELECT SINGLE`, `INSERT`, `MODIFY`, `UPDATE`, or
`DELETE` inside a loop that can process repository-scale collections.

This includes:

- Git objects;
- pack entries;
- delta bases;
- tree nodes;
- blobs;
- paths;
- index rows;
- missing-object lists;
- materialization certificates.

Forbidden shape:

```abap
LOOP AT lt_sha1s INTO DATA(lv_sha1).
  SELECT SINGLE *
    FROM zaog_obj_store
    WHERE repo_key = @iv_repo_key
      AND obj_sha1 = @lv_sha1
    INTO @DATA(ls_object).
ENDLOOP.
```

Required shape:

1. collect and deduplicate all required keys;
2. divide the key set into bounded packages;
3. issue one set-based statement per package;
4. build a hashed lookup table;
5. process the result entirely in memory.

Use:

- `SELECT ... FOR ALL ENTRIES` with a mandatory non-empty guard;
- range tables or `IN @range`;
- `MODIFY ... FROM TABLE`;
- `INSERT ... FROM TABLE`;
- set-based `DELETE`;
- database joins where the result cardinality remains controlled.

Always preserve all original key predicates when converting a singleton query
to a bulk query.

## 3. No hidden singleton APIs in hot loops

A convenience method such as `get_object`, `has_object`, `load_tree`, or
`get_base` may internally execute SQL.

Do not call such a method inside a repository-scale loop merely because SQL is
not visible at the call site.

For every hot loop, inspect the complete called method chain and classify each
call as:

- memory-only;
- one-time SQL;
- batched SQL;
- singleton SQL;
- HTTP/network;
- cache invalidation;
- transaction control.

Singleton methods may exist for cold or administrative paths, but graph,
tree-walk, delta, fetch, status, and materialization paths must use bulk APIs.

## 4. Batch by rows and bytes

A fixed row count alone is insufficient for blobs, trees, packs, or XSTRING
payloads.

Every payload batch must have both:

- maximum row count;
- maximum total expected or actual bytes.

Use a configurable structure such as:

```text
max_rows
max_bytes
max_single_object_bytes
```

If one object exceeds `max_bytes`, process it through an explicit oversized
single-object path. Do not combine it with other payloads.

A default row package size such as 500 may be used only when combined with a
byte budget or when the rows contain metadata without payload XSTRINGs.

## 5. Separate presence, metadata, and payload APIs

Do not load an object's XSTRING payload merely to check whether it exists.

Provide separate APIs for:

```text
PRESENCE
- repository key
- SHA
- readiness/status

METADATA
- object type
- object size
- pack/attempt identity
- materialization state
- payload omitted

PAYLOAD
- object bytes
- loaded only when required
```

Completeness checks, have eligibility, branch materialization checks, and
missing-set calculations must use presence or metadata APIs.

Blob payloads may be loaded only by the consumer that actually requires the
bytes.

## 6. Graph and tree traversal

Do not execute SQL or HTTP from recursive tree-walk or delta-resolver methods.

Required graph pattern:

1. initialize a frontier of IDs;
2. bulk-load the complete frontier;
3. parse nodes in memory;
4. add unseen child IDs to the next frontier;
5. repeat until the frontier is empty;
6. only then load required payload objects in bounded batches.

Maintain a hashed visited set to prevent repeated processing.

Recursive presentation or path construction is allowed only after all required
graph metadata is already present in memory.

Complexity target:

```text
database calls = O(number of batches or graph levels)
not O(number of nodes)
```

## 7. Delta-resolution performance

Delta resolution must separate:

- in-pack objects;
- unresolved dependency metadata;
- external bases;
- resolved-object identity.

Required order:

1. parse pack metadata;
2. resolve all available in-pack dependencies;
3. repeat until no progress;
4. collect all remaining external base SHAs;
5. deduplicate them;
6. bulk-load them from the repository store;
7. seed a SHA-keyed lookup table;
8. run the resolver again.

Do not perform database or network fetches from inside per-delta resolution.

Do not merge external bases into a pack-index-keyed table using default object
index values.

Use a dedicated hashed mapping for stable identity lookup.

## 8. Internal-table key safety

Never reuse `sy-tabix` obtained through a secondary-key access as an implicit
primary-table index.

Forbidden:

```abap
LOOP AT lt_objects USING KEY by_sha ...
  lv_tabix = sy-tabix.
ENDLOOP.

READ TABLE lt_objects INDEX lv_tabix.
```

Safe patterns include:

- primary-index iteration when a primary index is required;
- `REFERENCE INTO` or data references;
- a dedicated hashed side index from semantic key to stable primary index;
- re-reading with the same explicitly named table key;
- immutable IDs rather than mutable keyed fields.

Do not mutate a field that participates in a primary or secondary key and then
assume all previously captured indices or references remain valid.

For delta resolution, do not use one mutable field for both declared base
identity and resolved object identity.

## 9. Hashed lookup patterns

For pure key lookup, prefer:

```abap
HASHED TABLE ... WITH UNIQUE KEY ...
```

For insertion order plus key lookup, use a data structure whose ordering and
lookup semantics are explicit. Do not assume a secondary-key lookup yields a
usable primary index.

For LRU caches, prefer one of:

- hashed entries plus a separate ordered recency index;
- a monotonic sequence field with a dedicated sorted key;
- an explicitly tested cache class.

Do not use delete-and-reinsert as the default recency update when duplicate-key
or stale-index behavior is possible.

An empty XSTRING is a valid value. Cache presence must be tracked separately
from payload content.

## 10. Avoid repository-wide reads for incremental work

An incremental fetch containing 100 objects must not read every SHA or every
payload already stored for a repository containing 1,000,000 objects.

Forbidden patterns include:

- select all READY object keys and subtract incoming keys in ABAP;
- populate a full repository cache before checking a small key set;
- scan all pack entries to resolve one known object;
- load all branches to update one branch state.

Query only the incoming or required keys using bounded set-based operations.

Repository-wide scans are allowed only for explicit administration,
maintenance, migration, or full verification, never on the normal fetch,
branch-switch, Stage, Diff, or status hot paths.

## 11. Cache scope and invalidation

Caches must have an explicit scope:

- request;
- decode attempt;
- repository;
- session;
- application server.

A cache without a documented scope is not acceptable.

Avoid invalidating an entire repository cache after changing a small key set.
Invalidate or update only affected keys where possible.

Decode and delta-base caches should normally be attempt-scoped. Failed attempts
must not leak negative or partial cache entries into later attempts.

Cache invalidation must occur once per batch or publish step, not once per row.

## 12. XSTRING and memory rules

Avoid repeatedly concatenating a growing XSTRING in a loop.

Potentially quadratic pattern:

```abap
LOOP AT lt_chunks INTO DATA(lv_chunk).
  CONCATENATE lv_result lv_chunk
    INTO lv_result
    IN BYTE MODE.
ENDLOOP.
```

Prefer:

- chunk tables;
- bounded buffers;
- streaming/sink APIs where available;
- one final concatenation;
- direct persistence of chunks;
- prevalidated or declared output sizes.

Do not hold duplicate copies of:

- HTTP response;
- side-band-decoded pack;
- raw pack;
- reconstructed object payload;
- persistent row buffer

unless the peak-memory effect is explicitly justified and measured.

When the HTTP API returns one complete XSTRING, account for that entire response
in the memory budget before adding decoder and persistence buffers.

## 13. Transaction rules

Do not execute `COMMIT WORK` per object or per small batch unless a documented
crash-resume protocol requires it.

Low-level object, delta, and tree methods must not decide transaction
boundaries.

The orchestrator owns publication.

If durable staging is required:

1. persist rows with an attempt-scoped non-visible status;
2. commit staging according to a documented batch policy;
3. validate the complete operation;
4. atomically promote the successful attempt;
5. clean or invalidate failed attempts.

Ready-state visibility, branch-state updates, and completeness certificates must
not be published independently.

## 14. HTTP and remote-call rules

Never issue one HTTP request per missing tree, blob, delta base, or path.

Required remote strategy:

1. collect missing identities;
2. deduplicate;
3. classify by fetch semantics and server capabilities;
4. build bounded request batches;
5. fetch each batch once;
6. persist before retrying consumers.

Network request count must be proportional to the number of bounded batches, not
the number of objects.

Progressive-deepen retry sequences are not a performance substitute for a
correct graph/materialization design.

## 15. Performance observability

Every large-scale path must expose or log these counters without credentials or
payload content:

- correlation/attempt ID;
- repository key;
- operation/fetch mode;
- input object count;
- unique key count;
- SQL statement count by logical operation where measurable;
- rows read/written;
- HTTP request count;
- bytes received;
- pack object count;
- commits/trees/blobs/deltas processed;
- database cache hits/misses;
- external delta-base count;
- batch count;
- maximum batch rows;
- maximum batch bytes;
- elapsed phases;
- peak memory where measurable.

Diagnostics must be bounded and must not log one line per object in production.

## 16. Required performance review

Before a performance-sensitive implementation is approved, review the complete
end-to-end call chain, not only the changed method.

The review must search for:

- SQL statements inside loops;
- methods called inside loops that contain SQL;
- HTTP calls inside loops;
- unnecessary full-table or full-repository reads;
- repeated reads of identical keys;
- payload reads used only for presence checks;
- repeated XSTRING growth;
- repository-wide cache population;
- full cache invalidation after small updates;
- `COMMIT WORK` below the orchestration layer;
- nested loops without hashed lookup;
- unbounded internal tables;
- duplicate payload copies.

A review result must state:

```text
estimated SQL calls
estimated HTTP calls
estimated peak payload bytes
algorithmic complexity
large-repository acceptance status
```

## 17. Mandatory acceptance scenarios

Do not approve based only on tiny ABAP Unit fixtures.

At minimum validate or instrument:

### Small correctness fixture

- 1–20 objects;
- exact expected results;
- negative and exceptional cases.

### Medium integration fixture

- at least 5,000 objects;
- mixed commits, trees, blobs, and deltas;
- multiple batches.

### Large-scale validation

- target production cardinality or a justified synthetic substitute;
- at least 40,000 paths/objects where applicable;
- shared objects across multiple branches;
- cold and warm cache;
- interrupted attempt and retry.

### Incremental scaling check

For an incremental operation affecting `K` objects in a repository containing
`N` objects, verify that normal database and network work scales primarily with
`K` and required graph frontiers, not with all `N`.

## 18. Hard failure conditions

A performance review must fail when any of these remain on a production hot
path:

- SQL statement per object;
- HTTP request per object;
- recursive SQL tree walk;
- external delta-base singleton reads;
- repository-wide object-key read for a small incremental pack;
- full payload reads for existence checking;
- unbounded XSTRING accumulation;
- one transaction commit per object;
- branch-specific physical duplication of shared Git objects;
- progressive deepen treated as the large-repository strategy;
- performance justified only by small unit tests;
- no measurable SQL/HTTP/batch counters;
- no large-repository acceptance scenario.

Exceptions require an explicit owner-approved decision with measured evidence.
