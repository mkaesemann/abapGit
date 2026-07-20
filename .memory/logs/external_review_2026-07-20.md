# Deep Code Review – ORTEC abapGit Git Transport, Pack Decoder and Persistent Object Store

**Repository/branch reviewed:** `mkaesemann/abapGit`, branch `ortec/abapit_1_133_opt-rework`
**Review date:** 2026-07-20
**Focus:** Git wire protocol, branch switching, shallow/thin fetches, REF/OFS deltas, object persistence, correctness and database performance
**Source:** External review supplied by Michael (`abapgit-ortec-deep-review-findings.md`), archived here verbatim for reference by the architecture hardening plan in `.memory/state.md`.

---

## 1. Correction and review scope

This report distinguishes:

- **Confirmed defect:** directly visible in the supplied implementation.
- **Probable defect:** code path strongly explains the incident, but runtime packet/DB evidence is still needed.
- **Design risk:** currently defensible but fragile or expensive.
- **Already addressed:** a correct mitigation exists, but adjacent paths remain inconsistent.

### Severity scale

- **Critical:** can produce wrong repository contents, unrecoverable fetch failures, cross-repository reads, or corrupt completeness claims.
- **High:** common correctness/performance failure in large repositories.
- **Medium:** limited scenario, observability, maintainability, or avoidable overhead.
- **Low:** hardening/testability.

---

## 2. Executive summary

The implementation contains substantial and thoughtful work: repository-scoped storage, streaming decoding, REF/OFS delta support, sparse returned object sets, bulk helpers, a byte-budgeted base cache, explicit completeness checks, retry-without-haves signaling, and many regression tests. The current failure is therefore not caused by a total absence of multi-branch logic. It is caused by **remaining inconsistencies between negotiation truth, persisted completeness, decoder dependency resolution, and recovery semantics**.

### Most likely incident chain

1. A previously fetched branch contributes one or more commits to the repository-wide candidate `have` set.
2. The new branch is requested with shallow/deepen semantics and perhaps a verified or insufficiently verified common commit.
3. Azure DevOps legitimately emits deltas against objects it believes the client has, or emits an in-pack REF_DELTA chain whose final base identity is not known at pre-scan time.
4. `get_base_bytes` cannot find SHA `35fc...` in `ZAOG_OBJ_STORE` and raises retry-without-haves.
5. Thin and non-thin retries still traverse enough shared state/decoder metadata to reproduce the same missing-base lookup.
6. The last `force_full` request now correctly suppresses `have` and `deepen`, but the decoder can still misclassify an unresolved later REF_DELTA base as external, or the received request/pack is not proven to be the full expected response.

The fastest safe correction is **not** "fetch the missing SHA individually." It is to make the final recovery fetch a formally isolated transaction: no haves, no shallow/deepen/filter, fresh client, fresh session and pack IDs, clean per-decode caches, then verify that the wanted commit graph is complete before publishing branch state. However, making the final recovery fetch a formally isolated transaction is too expensive with respect to runtime and memory, so it can **not** be implemented.

---

## 3. Protocol and correctness findings

## F-01 – Completeness and `have` eligibility are still not one atomic invariant

- **Severity:** Critical
- **Confidence:** High
- **Status:** Probable defect / architectural root cause
- **Affected:** `zcl_abapgit_ortec_fetch_neg`, `zcl_abapgit_ortec_repo_state`, `zcl_abapgit_ortec_obj_store`, `persist_pull_result`

A Git `have` asserts a common commit from which the server may omit reachable objects or use external REF_DELTA bases. Presence of the commit row alone is insufficient. The implementation has `get_verified_have_commits` and `is_commit_complete`, but other paths still call `get_have_commits`, and `persist_pull_result` records `fetch_commit` plus `ZAOG_COMMIT_HIST` after storing only the passed object set. If that object set is sparse, filtered, shallow, resumed, or incomplete, branch state can become a stronger claim than the stored graph warrants.

**Fix:** Introduce one repository-wide `commit_materialization` record keyed by `(repo_key, commit_sha1)` with states `PENDING`, `COMPLETE`, `INVALID`. Set `COMPLETE` only after graph verification succeeds in the same LUW that promotes object rows. Every path that creates haves must read only `COMPLETE`. Remove direct derivation of have eligibility from `fetch_commit`, commit history, or commit-row presence.

**Acceptance tests:**

- Commit row present but one tree missing → never emitted as have.
- All objects present but promotion not committed → never emitted as have.
- Complete commit on branch A → eligible while fetching branch B.
- Filtered fetch → never marks the complete graph unless independently verified.

## F-02 – Non-thin fetch is incorrectly treated as recovery-equivalent to a complete fetch

- **Severity:** Critical
- **Confidence:** High
- **Status:** Confirmed design flaw
- **Affected:** `upload_pack_by_branch`, `upload_pack_by_commit`

The retry cascade is thin → non-thin → force-full. Disabling `thin-pack` only requires a self-contained pack; it does not remove haves, shallow boundaries, deepen limits, or filters. Therefore the second attempt can still be shallow and can still omit graph regions. Its failure should not be interpreted as proof of corruption.

**Fix:** Rename retry modes to explicit request contracts:

1. `INCREMENTAL_THIN`
2. `INCREMENTAL_SELF_CONTAINED`
3. `RECOVERY_UNBOUNDED`

Log the exact effective flags and SHA counts for each. Do not describe attempt 2 as "full."

## F-03 – `force_full` behavior is locally fixed, but not centrally enforced

- **Severity:** High
- **Confidence:** High
- **Status:** Already addressed in one builder; consistency risk remains
- **Affected:** `build_upload_pack_buffer`, other fetch builders such as tip/filtered fetch

The current builder correctly omits `deepen` when `iv_force_full = abap_true` and skips haves. This directly fixes an earlier live defect documented in the tests. However, the invariant exists as conditional string-building logic rather than a validated request model. Other fetch paths independently construct protocol buffers and can reintroduce deepen/filter/capability inconsistencies.

**Fix:** Create a typed request policy and one v1 request serializer. Add a final assertion before transmission:

```text
RECOVERY_UNBOUNDED => haves empty, shallow empty, deepen absent,
                      filter absent, thin-pack absent
```

## F-04 – REF_DELTA "missing externally" cannot be decided during raw pre-scan

- **Severity:** Critical
- **Confidence:** Very high
- **Status:** Confirmed problem class; tests acknowledge prior defect
- **Affected:** `zcl_abapgit_ortec_pack_dec`, `zcl_abapgit_ortec_delta`, `zcl_abapgit_ortec_pack_stream`

A REF_DELTA references the final object ID of its base. The base may itself be an unresolved delta later in the pack. Before that base is resolved, its row does not expose the final SHA. A pre-scan that sees no resolved row with the target SHA cannot conclude that the base is external. Your `chain_onto_later_unresolved` test documents exactly this topology.

**Fix:** Keep unresolved REF dependencies in a deferred set. Run in-pack fixpoint resolution first. Only after no in-pack progress is possible may unresolved SHA dependencies be bulk-resolved against the persistent store. After loading external bases, run another fixpoint. Report the residual dependency graph if still unresolved.

## F-05 – External base acquisition is performed one SHA at a time in streaming resolution

- **Severity:** High
- **Confidence:** Very high
- **Status:** Confirmed performance defect
- **Affected:** `zcl_abapgit_ortec_pack_stream=>get_base_bytes`

`get_base_bytes` calls `obj_store=>get_object` per cache miss. The LRU avoids repeated reads of the same base but does not remove one DB roundtrip per distinct base. A large thin pack with many bases becomes N+1 SQL.

**Fix:** After the in-pack-only fixpoint, deduplicate all unresolved external base SHAs and call `get_objects` once per byte-/row-bounded batch. Seed a decode-local hashed map/LRU, then resume resolution. Never fetch an external base from inside the per-object loop unless it is a last-resort diagnostic path.

## F-06 – Singleton global base cache is not repository-scoped

- **Severity:** Critical
- **Confidence:** High
- **Status:** Confirmed design defect
- **Affected:** `zcl_abapgit_ortec_base_cache`, `get_base_bytes`

The cache key is SHA only and `get_instance()` is global for the internal session. Git object IDs are content-addressed, so identical SHA normally implies identical bytes; nevertheless the implementation supports repository scoping, object status, cleanup, and potentially mixed object-format/repository contexts. More importantly, stale cached bytes survive retry boundaries unless explicitly cleared, undermining recovery isolation.

**Fix:** Make the cache instance decode-scoped, not class-global. At minimum key it by `(repo_key, object_format, sha)` and clear it at the start/end of each pack and before recovery attempt 3. Prefer dependency injection from the decoder.

## F-07 – Blank repo-key fallback remains in the standard delta path

- **Severity:** Critical
- **Confidence:** Very high
- **Status:** Confirmed defect
- **Affected:** `zcl_abapgit_git_delta=>delta`, `zcl_abapgit_ortec_obj_store`

The standard delta fallback calls `get_object(iv_repo_key = '')` and relies on a session-global active repository key. The supplied tests explicitly identify this as a branch-switch incident risk. This allows stale context, nested operations, tests, or alternate entry paths to search the wrong repository.

**Fix:** Pass `repo_key` explicitly through every decoder/resolver signature. Reject an initial key in production paths. Keep the active-key fallback only behind a temporary compatibility adapter with a warning and removal date.

## F-08 – Object identity and delta-base identity are overloaded

- **Severity:** Critical
- **Confidence:** High
- **Status:** Confirmed design defect
- **Affected:** shared `ty_object`, delta resolvers

For unresolved REF_DELTA rows, `sha1` is used as the base SHA; for resolved rows it becomes the reconstructed object SHA. Mutating semantic identity complicates secondary keys, makes pre-scan ambiguous, and previously caused stale secondary-key behavior.

**Fix:** Introduce a dedicated internal pack-entry type:

```text
pack_index, pack_offset, storage_type, declared_size,
base_sha1, base_offset, resolved_sha1, resolved_type,
resolved_data/status
```

Do not reuse the public decoded-object structure for unresolved pack entries.

## F-09 – Mutating keyed fields requires explicit table modification everywhere

- **Severity:** Critical
- **Confidence:** High
- **Affected:** delta resolution tables and SHA indexes

The tests note a defect where direct field-symbol mutation of SHA left a secondary key stale. Current code includes corrective logic, but the data model still encourages mutation of key components during promotion.

**Fix:** Make unresolved metadata immutable with respect to its primary lookup keys. Insert a resolved identity mapping instead of changing keys in place. Add ATC/custom check or focused code review rule forbidding field-symbol writes to key components.

## F-10 – Multiple external bases can collide through default pack index values

- **Severity:** Critical
- **Confidence:** Very high
- **Status:** Previously observed; regression risk
- **Affected:** non-streaming resolver/prefetch merge

Objects loaded from the store do not naturally have a pack index. Treating them as pack rows gives multiple bases index `0`, causing duplicate-key or wrong-base resolution. Existing tests acknowledge this.

**Fix:** Never merge external bases into a pack-index-keyed table. Keep a separate hashed table keyed by SHA. If compatibility requires one table, allocate a separate namespace (for example negative synthetic IDs), but separation is safer.

## F-11 – Recovery attempt does not prove isolation from prior failed sessions

- **Severity:** Critical
- **Confidence:** Medium-high
- **Status:** Probable defect
- **Affected:** retry cascade, raw pack/session/index cleanup

The final retry creates a fresh HTTP client, but correctness also requires a fresh decode session, pack ID, no pending index/meta rows from attempts 1/2, and cleared decode-local caches. Cleanup is distributed across decoder catch blocks and session utilities. A failure before a specific cleanup point can leave state that influences the next attempt.

**Fix:** Give each attempt a unique fetch-attempt ID. Scope raw pack, pack metadata, temporary object rows, and index rows to it. Before attempt 3, abandon attempts 1/2 atomically; do not reuse their metadata. Promotion must select only the current attempt ID.

## F-12 – Shallow state is parsed but not modeled as durable graph semantics

- **Severity:** High
- **Confidence:** High
- **Status:** Design defect
- **Affected:** `parse`, repo state, negotiation

The parser collects `shallow` and `unshallow` lines, but branch/repository state primarily tracks `is_shallow`, fetch commit, and timestamps. A boolean is not enough to represent the set of shallow boundary commits. Without the exact boundary set, ancestor traversal can classify incomplete history as complete or generate inappropriate haves.

**Fix:** Persist shallow boundaries as `(repo_key, fetch/ref scope, commit_sha)`. Completeness traversal must stop at a known shallow boundary and classify the graph as shallow-complete, not fully complete. Only advertise such commits under a negotiation policy that also sends correct shallow lines.

## F-13 – Capability handling is string-based and insufficiently strict

- **Severity:** High
- **Confidence:** High
- **Status:** Confirmed risk
- **Affected:** capability extraction and buffer building

Capabilities are extracted from advertisement text and request strings are assembled manually. Protocol v0/v1 requires only advertised capabilities to be requested. Substring matching risks false positives; independent builders can request inconsistent sets.

**Fix:** Parse capabilities into a set. Serialize only intersection(requested, advertised). Record the negotiated set in diagnostics. Add tests for capability names as prefixes/substrings and missing `filter`, `thin-pack`, `ofs-delta`, `side-band-64k`, `multi_ack_detailed`, and `no-progress`.

## F-14 – pkt-line helper cannot encode general protocol lengths

- **Severity:** High
- **Confidence:** Very high
- **Status:** Confirmed generic defect
- **Affected:** `zcl_abapgit_git_utils=>pkt_string`

The helper rejects strings `>= 255` and constructs a packet length using a one-byte value prefixed with `00`. Git pkt-line supports four hexadecimal length digits and much larger payloads. Current individual want/have lines fit, but agent strings, future arguments, error paths, or v2 sections need a compliant encoder.

**Fix:** Compute byte length, add four, format as exactly four ASCII hexadecimal digits, and enforce the protocol maximum. Use byte length, not character length.

## F-15 – side-band parser silently ignores channel 2 and channel 3

- **Severity:** High
- **Confidence:** High
- **Status:** Confirmed observability/correctness defect
- **Affected:** `parse`, v2 `decode_pack`

Only band 1 is appended. Progress (2) may be intentionally ignored, but fatal error channel 3 must become an exception. Silently discarding it can yield an empty/truncated pack and misleading downstream "missing object" errors.

**Fix:** Parse all bands: 1=data, 2=bounded diagnostic/progress, 3=fatal remote error. Preserve the server message in the raised exception. Reject unknown channels.

## F-16 – Pack/trailer/zlib parsing contains heuristic byte skipping

- **Severity:** Critical
- **Confidence:** High
- **Status:** Confirmed robustness defect
- **Affected:** standard `zcl_abapgit_git_pack=>zlib_decompress` and equivalent paths

The standard decoder checks Adler32, then advances one byte and retries, then another byte. Heuristic resynchronization can mask an incorrect compressed-length calculation and shift parsing across object boundaries. A valid pack parser must consume exactly the zlib stream length and four-byte checksum.

**Fix:** Remove byte-skipping heuristics. Make decompression return exact consumed bytes, validate Adler32 at that exact position, and fail deterministically. Add concatenated-stream and corrupt-boundary tests.

## F-17 – Sparse decoder return contract is implicit and easy to misuse

- **Severity:** High
- **Confidence:** High
- **Status:** Confirmed design risk
- **Affected:** `decode_streaming`, callers expecting `ty_objects_tt`

`decode_streaming` intentionally returns only commits while trees/blobs live in the store. Reusing the same return type as a full decoded pack makes accidental callers assume completeness. The code contains comments to defend this, which signals an API mismatch.

**Fix:** Return a typed result:

```text
pack_id, commits, persisted_object_count,
materialization_state, sparse = true
```

Do not expose a sparse set under a generic `objects` name.

## F-18 – Persisted object bytes are not uniformly re-hash-verified on read

- **Severity:** Critical
- **Confidence:** Medium-high
- **Status:** Hardening gap
- **Affected:** object-store read/base resolution

Pack checksum validation proves transport integrity, and reconstructed objects are hashed during decode. But persistent rows can be stale, manually changed, partially written, or mismatched with metadata. `get_base_bytes` trusts returned bytes after lookup.

**Fix:** Store verified hash/status and re-hash at least on first read per session or when status is not `R`. On mismatch mark `CORRUPT`, exclude from haves, and force recovery. Include type in the Git object hash computation.

---

## 4. Database and memory performance findings

## P-01 – `get_object` routes through bulk machinery for a singleton

- **Severity:** Medium
- **Confidence:** High
- **Affected:** object store

A singleton lookup invokes `get_objects`, which performs deduplication, chunking, cache logic, and missing-set construction. This is convenient but expensive on the hottest delta-base path.

**Fix:** Provide a true single-row fast path with identical validation semantics, while the decoder should normally use bulk prefetch (F-05).

## P-02 – Reachability traversal repeatedly loads tree data in batches per frontier

- **Severity:** High
- **Confidence:** High
- **Affected:** `get_reachable_objects`, `get_reachable_sha1s`, completeness checks

Breadth-first frontier loading is better than one SELECT per node, but repeated completeness checks for the same commits decode the same trees and query the same keys. This is expensive before every fetch negotiation.

**Fix:** Persist a materialization certificate per commit (F-01). Re-run full reachability only when invalidated or when object status changes. Cache decoded tree edges separately from blob bytes.

## P-03 – `has_dangling_delta_base` performs several scans/queries over pack index data

- **Severity:** High
- **Confidence:** High

The method performs multiple SQL statements and loops to infer dangling bases. Called per candidate commit, this multiplies negotiation cost.

**Fix:** Maintain unresolved-delta count/status per pack or commit materialization record during decode/promotion. Query a single aggregate flag rather than reconstructing it repeatedly.

## P-04 – `persist_pull_result` selects every ready SHA in the repository

- **Severity:** High
- **Confidence:** Very high
- **Affected:** `zcl_abapgit_ortec_fastpath=>persist_pull_result`

The method executes a repository-wide `SELECT obj_sha1 ... status = 'R'` and then checks the incoming objects in memory. As the cache grows, each pull reads an ever-growing key set even when only a small pack arrived.

**Fix:** Deduplicate incoming SHAs, then query only those keys in chunked `IN` ranges. Alternatively use insert-with-duplicate handling where supported. Never read the complete repository object keyspace for a small delta.

## P-05 – Object-store class cache can materialize too much repository metadata

- **Severity:** High
- **Confidence:** High
- **Affected:** `populate_cache`, class-data cache

Session-global cache population and invalidation create a coarse all-or-nothing cache. Direct DB writes invalidate the whole cache. Large repositories pay both memory and rebuild cost.

**Fix:** Replace repository-wide cache with bounded positive/negative key caches or request-scoped maps. Invalidate only touched SHAs. Avoid caching XSTRING payloads in the metadata cache.

## P-06 – Walk paths still contain object-by-object fallback reads

- **Severity:** High
- **Confidence:** Very high
- **Affected:** standard and ORTEC porcelain walk/walk_tree

When a tree/blob is absent from the passed object table, recursive walk calls `get_object` for each node. `walk_prep` improves the ORTEC path, but fallback code remains N+1 and can be reached during partial states.

**Fix:** Make prewarm/top-up mandatory before recursive walk. The recursive walk must operate only on an in-memory metadata map plus bounded blob batches, never issue SQL itself.

## P-07 – `COMMIT WORK` occurs inside low-level decode/persistence methods

- **Severity:** Critical
- **Confidence:** Very high
- **Affected:** streaming resolver, pack decoder/store, `persist_pull_result`

Internal commits make test cleanup difficult and break atomicity between object promotion, pack status, materialization state, and branch state. If a later step fails, partially published truth remains.

**Fix:** One transaction owner: the fetch orchestrator. Low-level methods return changes or write pending rows but never commit. If crash-resumable staging requires durable commits, use an explicit two-phase protocol: durable `STAGED` rows that are never visible as ready, then one atomic promotion LUW.

## P-08 – Temporary rows use the main object table

- **Severity:** High
- **Confidence:** High
- **Affected:** streaming decode statuses `I`/`R`

Pending/incomplete and ready objects share `ZAOG_OBJ_STORE`. This increases index churn, complicates every read predicate, and raises accidental visibility risk.

**Fix:** Prefer a separate attempt-scoped staging table or partitioned key design. If retained, every read API must enforce `status = R`; add DB-level indexes beginning with `(repo_key, status, obj_sha1)` as appropriate to actual predicates.

## P-09 – Full XSTRING rows are loaded where only presence/type is needed

- **Severity:** High
- **Confidence:** High
- **Affected:** completeness, graph checks, object reads

The code has `get_present_sha1s`, which is good, but other routines still select/return full object structures during graph validation. Large blobs dominate memory and DB transfer.

**Fix:** Enforce three APIs: key presence only; metadata without bytes; payload load. Completeness must never select blob payloads.

## P-10 – Batch limits are not consistently byte-budgeted

- **Severity:** High
- **Confidence:** High
- **Affected:** loading/persisting blobs and bases

A row-count batch is unsafe for highly variable blob sizes. A handful of huge blobs can exhaust ABAP heap; many tiny blobs underutilize DB roundtrips.

**Fix:** Batch by both maximum rows and estimated/known `obj_size`, with an oversize-single-item path. Apply the same policy to DB reads, write batches, returned object tables, and LRU admission.

## P-11 – Repeated concatenation of XSTRINGs can become quadratic

- **Severity:** High
- **Confidence:** High
- **Affected:** pack assembly, side-band parsing, delta application, zlib output

Repeated `CONCATENATE ... INTO same_xstring IN BYTE MODE` may repeatedly allocate/copy growing buffers. This is especially expensive for large packs or blobs.

**Fix:** Use chunk tables/stream abstractions and concatenate once, or bounded chunk accumulation. For delta application, preallocate/segment output according to declared result size and validate it.

## P-12 – Progress and timeout redispatch occur on hot loops without centralized throttling

- **Severity:** Medium
- **Confidence:** Medium

Frequent GUI progress calls or redispatch checks can dominate tight loops; too infrequent calls risk timeouts.

**Fix:** throttle by elapsed time and processed bytes, not per object. Instrument counts in SAT/ST12.

---

## 5. Additional correctness hardening

## H-01 – Validate declared delta source and result sizes

- **Severity:** Critical
- **Confidence:** High

Delta headers contain source and result sizes. Skipping them "for performance" loses essential corruption detection and can allow out-of-range copy instructions or memory amplification.

**Fix:** Parse both varints, require source size = base length, enforce result size and configured maximum, validate every copy range, and require produced length = declared result size.

## H-02 – Guard all stream reads against bounds

- **Severity:** Critical
- **Confidence:** High

Several local stream methods directly slice XSTRINGs. Malformed input can trigger `CX_SY_RANGE_OUT_OF_BOUNDS` rather than a domain exception, and partial state may already have been written.

**Fix:** central checked-read primitive; convert bounds failures into an ORTEC Git exception carrying pack ID, object index, offset, and requested length.

## H-03 – Validate object type inherited through delta chains

- **Severity:** High
- **Confidence:** High

A delta has the base object's logical type. Resolver code must carry the resolved base type through every chain and reject unresolved/ref/ofs types as final stored types.

**Fix:** assert final type is commit/tree/blob/tag before hashing and promotion.

## H-04 – SHA-1-only schema prevents object-format evolution

- **Severity:** Medium
- **Confidence:** High

The Git pack format can use SHA-256 repositories; fixed 40-character IDs and 20-byte trailer assumptions hard-code SHA-1.

**Fix:** Document SHA-1-only support and reject advertised `object-format=sha256` explicitly. Longer-term add object format to repository identity and variable-length IDs.

## H-05 – URL canonicalization determines repository isolation

- **Severity:** Critical
- **Confidence:** Medium

`repo_key` is resolved from URL. Equivalent URL variants, credentials, URL encoding, casing, trailing `.git`, or redirects can create duplicate keys; over-normalization can merge distinct repositories.

**Fix:** define and test canonical remote identity. Strip credentials, normalize only proven-equivalent syntax, include Azure organization/project/repository identity where available, and enforce uniqueness in DB.

## H-06 – Catch-and-fallback blocks can hide persistent corruption

- **Severity:** High
- **Confidence:** High

Several paths catch broad ORTEC/abapGit exceptions and silently fall back. This is good for availability but can repeatedly pay failed fast-path work and conceal corruption.

**Fix:** classify exceptions: protocol incompatibility → fallback; transient network → retry policy; local corruption/invariant violation → invalidate/quarantine and emit visible diagnostics. Preserve complete causal chains.

---

## 6. Recommended implementation sequence (reviewer's own suggestion, superseded by the orchestrator's phased plan in `.memory/state.md`)

### Phase A – Reproduce and instrument before changing semantics
### Phase B – Correct recovery and state truth
### Phase C – Correct delta dependency model
### Phase D – Eliminate N+1 SQL and memory spikes
### Phase E – Protocol hardening

(Full detail in the reviewer's original text - superseded by the concrete, code-reconciled phase breakdown in `.memory/state.md`'s "Architecture hardening plan" section, which maps every F-/P-/H- finding to a specific phase and notes which are already addressed by this session's work.)

---

## 7. Required regression and integration tests

### Branch switching
- Fetch branch A, then unrelated branch B.
- Fetch A, then B with long shared history.
- Fetch A shallow, switch to B whose merge base lies outside shallow boundary.
- Force-push/rebase B after A was cached.
- Switch A → B → A with no new remote objects.
- Two repositories in one internal session to prove repo context isolation.

### Delta topology
- REF_DELTA base earlier/later in pack.
- Base itself a REF_DELTA or OFS_DELTA.
- Multiple levels and mixed REF/OFS chain.
- Two distinct external bases in one pack.
- Empty base/blob.
- Missing base, corrupt base bytes, wrong type, wrong final SHA.
- Pack where pre-scan cannot know a later delta's final identity.

### Recovery cascade
For final recovery, assert serialized request contains:
- no `have`; no `shallow`; no `deepen`, `deepen-since`, `deepen-not`, `deepen-relative`; no `filter`; no `thin-pack`; wanted branch tip only.
Also assert attempt 3 uses no pending rows/cache entries from attempts 1/2.

### SQL/performance
- External bases: O(number of batches), not O(number of deltas).
- Tree/blob walk: O(frontier batches), no SQL inside recursion.
- Persist 100 new objects in a store of 1,000,000: query only incoming keys.
- Completeness of already-certified commit: constant-number DB reads.
- Peak memory bounded by configured byte budgets.

---

## 8. Suggested incident-specific diagnostic decision tree

For SHA `35fcfcb0379260fd21602953a666827b64d931f3`:

1. Query `ZAOG_OBJ_STORE` by the exact `repo_key` with all statuses.
2. If row exists as ready: repo-key/cache/read predicate defect.
3. If row exists incomplete/pending: failed-attempt publication/cleanup defect.
4. If absent, inspect current pack metadata (in-pack unresolved vs. genuinely external).
5. Independently run native Git `index-pack --fix-thin`/`verify-pack` against the exact captured pack.

**Orchestrator's own follow-up (2026-07-20, via GitHub API, NOT native git tooling):** confirmed the SHA is a
real, non-GC'd commit from 2017, 4737 commits behind the branch tip - see `.memory/state.md`'s "Sixth
issue" entry. This answers the decision tree's step 4/5 question directly: the object is genuinely
external and genuinely present upstream - the request itself (how much history is asked for) is the gap,
not decoder misclassification or repo-key contamination.

---

## 9. Final conclusion

The supplied implementation is not a superficial prototype; many expected failure modes already have targeted code and tests. The remaining problem is that correctness is spread across several partially overlapping mechanisms: branch state, commit history, object state, pack index state, active repo context, shallow state, decoder metadata, and retry-local caches. A branch switch stresses exactly those boundaries.

The highest-value architectural change is to establish two non-negotiable truths:

1. **A commit is offered as `have` only if one atomic, repository-scoped materialization certificate says the required graph is complete under known shallow semantics.**
2. **A recovery fetch and its decoder operate in a fresh, attempt-scoped context and publish nothing until the wanted graph is validated.**

Once those are implemented, the reported repeated missing-base failure should either disappear or produce a precise residual dependency report rather than the same generic exception across three retries.

---

## 10. Protocol references

- Git pack format: pack entries, REF_DELTA, OFS_DELTA, thin-pack semantics: https://git-scm.com/docs/pack-format
- Git pack transfer protocol and pkt-line framing: https://git-scm.com/docs/gitprotocol-pack
- Git protocol capabilities v0/v1: https://git-scm.com/docs/gitprotocol-capabilities
- Git protocol v2: https://git-scm.com/docs/gitprotocol-v2
- Git fetch shallow/deepen/unshallow semantics: https://git-scm.com/docs/git-fetch
