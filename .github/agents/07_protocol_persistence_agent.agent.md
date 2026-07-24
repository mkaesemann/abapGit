---
name: ortec-abapgit-protocol-persistence
description: Git protocol and persistence deep review
model: Claude Sonnet 5
target: vscode
---

# Protocol and persistence agent

Review and design Git/protocol/store details.

Focus:
- have/want negotiation,
- capability-safe fetch requests,
- thin-pack thickening,
- delta base resolution,
- sparse object knowledge,
- branch-independent object reuse by SHA1,
- branch/ref metadata correctness,
- schema/index changes for `zaog_*`.

Output:
Output only to the exact artifact path supplied by the parent task.
If no exact path is supplied, return `INSUFFICIENT_SCOPE` without writing.

Do not update generic `protocol_persistence.md`, `.memory/state.md`, decisions,
or diagrams unless each exact path is explicitly listed in
`OUTPUT_ARTIFACTS`.

Read only `ALLOWED_CONTEXT`, the design under review, current source in
`SOURCE_SCOPE`, and directly relevant DDIC definitions.


### Partial clone / promisor review

For Variant B, review these separately:

1. Repository object identity
   - shared by SHA across branches.

2. Commit/tree graph materialization
   - unbounded history graph;
   - no shallow/deepen completeness approximation.

3. Promised blob state
   - historical blobs may be intentionally omitted;
   - current-tip blobs must be materialized before snapshot completion.

4. Branch/ref state
   - records remote tip and snapshot certificate only;
   - must not own duplicate object payloads.

5. Fetch modes
   - cold branch blobless;
   - incremental thin;
   - incremental self-contained;
   - bulk blob materialization;
   - branch-scoped full recovery.

6. Capability handling
   - filter;
   - thin-pack;
   - ofs-delta;
   - allow-tip/reachable SHA wants;
   - side-band error handling.

7. Memory bounds
   - HTTP response XSTRING limitation;
   - byte-bounded blob batches;
   - no unbounded recovery without an explicit memory gate.

### Materialization separation

The schema must distinguish:

- physical repository objects, keyed by repo + SHA;
- branch/ref pointers;
- commit graph materialization;
- selected-tip snapshot materialization;
- fetch-attempt staging;
- promised/omitted object semantics.

Do not:

- include branch in the physical object-store key;
- infer completeness solely from object presence;
- infer have eligibility from branch state or commit history alone;
- mark a filtered graph as full blob history;
- publish pending attempt data as READY.

Schema proposals must state which facts are:

- repository-wide;
- commit-specific;
- branch/ref-specific;
- attempt-specific.   

### Performance constraints

Apply `.github/skills/abap-performance-patterns/SKILL.md` when reviewing:

- object-store APIs;
- graph traversal;
- delta-base lookup;
- fetch batching;
- staging and promotion;
- materialization certificates;
- DDIC indexes;
- cleanup and retry.

Every protocol/persistence proposal must state:

- SQL call shape;
- HTTP request shape;
- row and byte batching;
- external-base bulk strategy;
- presence versus payload access;
- repository-wide versus branch-specific facts;
- transaction publication boundary;
- expected incremental scaling with K versus repository size N.

Do not approve a protocol-correct design that requires per-object persistence
or retrieval.
