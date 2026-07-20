---
description: Implement Variant B repository-shared blobless cache with per-branch snapshot materialization
agent: ortec-abapgit-orchestrator
argument-hint: Optional slice identifier; defaults to Slice 0
---

# Variant B - owner-approved implementation specification

Work in `mkaesemann/abapGit` on branch
`ortec/abapgit_1_133-opt-rework`.

## Goal

Implement a durable Git cache architecture with:

- one physical SHA-addressed object store per repository;
- branch/ref state separate from object payloads;
- first access to an unknown branch using an unbounded blobless fetch
  (`filter blob:none`, no `deepen`);
- bulk materialization of only blobs referenced by that branch tip;
- repository-wide reuse of commits, trees, and unchanged blobs;
- later incremental fetches using certified `have` commits and thin/OFS deltas
  only when every required base is provably available;
- branch-scoped full unfiltered fetch only as exceptional, memory-gated recovery.

Correctness priority is correctness, then production-scale performance, then
maintainability. Preserve standard abapGit behavior when ORTEC is disabled.

## Start and memory rules

1. Read `.memory/state.md` first.
2. Treat this prompt as the current owner decision and active topic
   `variant-b-partial-clone`.
3. Read only active links and source files required for the current slice.
4. Do not resume H4 as an independent topic. Existing H4 code/tests are evidence
   and must be classified as reuse, adapt, replace, or obsolete.
5. Historical memory is evidence, not current truth. Mark contradicted or replaced
   conclusions accordingly.
6. Do not repeat broad archaeology. Perform focused current-source reconciliation.
7. Keep chat output concise; write details to focused `.memory` artifacts.
8. Do not ask for confirmation between approved slices. Stop only for a hard-stop
   condition below.

## Non-negotiable invariants

- Physical objects are keyed by repository plus SHA, never by branch.
- Branch rows are pointers/status only and never own duplicate payloads.
- `deepen N` is not completeness and progressive deepen is forbidden as
  correctness or final recovery.
- Promised blobs must be distinguishable from unexpectedly missing objects.
- A graph is certified only when its requested commit and complete tree closure
  are present and verified.
- A branch snapshot is complete only when every blob referenced by its tip tree
  is READY and hash-verified.
- A commit is used as `have` only when a materialization certificate supports the
  request mode.
- Missing/unknown/promised data is never remote deletion.
- Resolve in-pack dependencies before classifying a delta base as external.
- External bases are deduplicated and bulk-loaded.
- No SQL or HTTP request per object, tree node, blob, or delta base.
- Incremental work scales with required objects `K`, not all repository objects
  `N`.
- Failed attempts publish no ready objects, branch state, or certificates.
- Never silently decode the same failed ORTEC pack through standard decoding.
- Pass repository keys explicitly; no blank/session-global productive fallback.
- Request only capabilities advertised by the server.
- Payload batches have row and byte limits plus oversized-object behavior.

## Mandatory gated workflow

For every repository-scale slice:

1. focused reconciliation/design;
2. correctness design review;
3. protocol/persistence review when applicable;
4. senior performance review in `DESIGN_GATE` mode;
5. senior implementation, with exact mechanical subtasks delegated to junior;
6. low-cost performance scan;
7. senior performance review in `IMPLEMENTATION_AUDIT` mode;
8. regression validation.

Implementation requires correctness and performance approval. Regression cannot
close a slice while a blocking performance verdict remains.

# Slice 0 - Current-source reconciliation

Read completely only the current implementations and relevant test includes of:

- `zcl_abapgit_ortec_fastpath`;
- `zcl_abapgit_ortec_pack_stream`;
- `zcl_abapgit_ortec_pack_dec`;
- `zcl_abapgit_ortec_fetch_neg`;
- `zcl_abapgit_ortec_repo_state`;
- `zcl_abapgit_ortec_obj_store`;
- `zcl_abapgit_ortec_walk_prep`;
- `zcl_abapgit_ortec_missing_obj`;
- `zcl_abapgit_ortec_porcelain`;
- `zcl_abapgit_git_transport`;
- directly used `ZAOG_*` DDIC definitions.

Write `.memory/logs/variant_b_reconciliation.md` mapping each requirement below
to existing methods/fields or `NEW`.

Identify every live occurrence of:

- progressive-deepen recovery;
- `force_full` that can emit `deepen`;
- recursive or per-object `complete_missing_object` network repair;
- uncertified or shallow-ambiguous bare haves;
- silent standard decode fallback;
- blank repo-key fallback;
- SQL/HTTP calls from hot loops;
- repository-wide reads used for incremental work;
- payload reads used for presence checks;
- unbounded XSTRING copies;
- low-level `COMMIT WORK`.

Do not edit productive code in Slice 0.

# Slice 1 - Durable materialization model

Reconcile existing schema first. Reuse equivalent fields/classes; do not create
duplicates.

If no equivalent exists, add repository+commit materialization state containing:

- repository key;
- commit SHA;
- history level: unknown, graph-complete, full-complete;
- snapshot state: none, pending, complete, invalid;
- attempt ID;
- verified and updated timestamps.

Branch state may contain a materialized commit pointer and snapshot state, but
not object payload ownership.

Provide an ORTEC materialization API with no internal `COMMIT WORK`:

- get state;
- begin attempt;
- mark graph complete;
- mark snapshot complete and update branch pointer atomically under the
  orchestrator-owned publication boundary;
- mark full complete;
- invalidate commit;
- test graph/full have eligibility;
- clean incomplete attempt state.

Do not trust old commit history, fetch pointers, index markers, or object presence
as an automatic backfill. Existing rows become certified only after verification.

# Slice 2 - Explicit fetch modes and one request serializer

Replace interacting booleans and misleading `force_full` semantics with explicit
ORTEC fetch modes:

- `INCREMENTAL_THIN`;
- `INCREMENTAL_SELF_CONTAINED`;
- `INITIAL_BRANCH_BLOBLESS`;
- `MATERIALIZE_BLOBS`;
- `RECOVERY_BRANCH_FULL`.

Delete progressive-deepen constants/methods from the live flow and later remove
them after references/tests are gone.

Create one validated request serializer using server-advertised capabilities.

### INITIAL_BRANCH_BLOBLESS

- one want for target tip;
- no haves initially;
- no shallow/deepen variants;
- `filter blob:none` only when advertised;
- no thin pack for cold initialization;
- if filter is unavailable, raise a structured unsupported-capability result;
  do not silently issue a huge unfiltered fetch.

### INCREMENTAL_THIN

- target tip want;
- only certified haves;
- no deepen;
- request thin/OFS only when advertised and supported by the decoder and local
  completeness guarantee.

### INCREMENTAL_SELF_CONTAINED

- target tip want;
- certified haves allowed;
- no thin and no deepen.

### MATERIALIZE_BLOBS

- bounded batch of missing reachable blob SHAs;
- no haves, shallow, deepen, filter, or thin;
- legal only when advertised server behavior permits these SHA wants.

### RECOVERY_BRANCH_FULL

- target tip only;
- no haves, shallow, deepen variants, filter, or thin;
- fresh HTTP client, attempt/session/pack IDs, and decode-local cache;
- explicit memory gate because the current HTTP layer may materialize the full
  response XSTRING.

Add exact wire tests asserting required and forbidden tokens for each mode.

# Slice 3 - Cold-branch blobless graph acquisition

Add an ORTEC orchestration method for an unknown branch:

1. create fresh attempt identity;
2. mark materialization pending;
3. fetch `INITIAL_BRANCH_BLOBLESS`;
4. decode and persist commits/trees as READY;
5. treat filtered historical blobs as promised, not corrupt;
6. validate pack trailer and every reconstructed object hash/type;
7. require requested tip commit;
8. iteratively traverse the complete tree closure using bulk metadata/payload
   access for commits and trees only;
9. collect unique current-tip blob SHAs without loading their payloads;
10. fail without certificate if any required commit/tree/base is missing;
11. mark graph complete only after successful verification;
12. hand the blob set to snapshot materialization.

Use a typed decode result that explicitly reports blobless/promisor semantics,
object counts, promised blob count, and unresolved dependency count. Success
requires zero unresolved dependencies.

# Slice 4 - Materialize selected branch snapshot

Provide a bulk graph API that returns unique current-tip blob SHAs:

- iterative frontier queues;
- bulk-load one frontier at a time;
- hashed visited set;
- no SQL/HTTP in recursive per-node walk;
- no blob payload selection;
- raise on missing/non-ready commit or tree.

Bulk-subtract READY blob presence using bounded set operations.

Materialize missing blobs:

1. deduplicate missing SHAs;
2. group by row and byte limits;
3. define oversized-object handling;
4. fetch each bounded batch once;
5. decode, hash-verify, and bulk-persist;
6. re-check the complete missing set;
7. publish snapshot-complete state only when no current-tip blob remains missing.

If arbitrary reachable SHA wants are not supported, choose one capability-safe
bounded bulk method. Never issue one request per blob. If no correct bounded
method exists, use memory-gated branch recovery or standard fallback with a
structured reason.

# Slice 5 - Wire Variant B into branch pull/switch

Decision flow:

1. resolve repository and advertised tip;
2. read branch state and commit certificate;
3. if unchanged tip snapshot is complete, validate remote tip and serve locally;
4. if branch/tip lacks graph certificate, run cold blobless initialization and
   snapshot materialization;
5. if a known branch tip changed, build only certified haves, try incremental
   thin when safe, then one self-contained retry when appropriate;
6. verify new graph and materialize only new current-tip blobs;
7. publish branch pointer/certificates only after success;
8. on unexpected missing base, clean attempt and allow at most one memory-gated
   branch-full recovery;
9. never use progressive deepen.

# Slice 6 - Have eligibility and shallow handling

Stop deriving haves directly from commit history, fetch pointer, object-row
presence, or branch name.

Candidates may come from history, but each must pass the materialization API.
Exclude wants, deduplicate, and cap deterministically.

Do not emit shallow-tracked commits as bare haves without exact persisted shallow
boundaries and matching wire semantics. Variant B's normal graph state is
unbounded, not numeric-depth shallow.

Test cross-branch shared ancestor reuse, uncertified rejection, graph certificate
compatibility with promised blobs, invalid-state rejection, and no bare shallow
have.

# Slice 7 - Delta resolution and bulk external bases

Reconcile streaming and non-streaming ORTEC resolvers to one correctness model:

1. parse immutable pack-entry metadata with separate declared base and resolved
   identity fields;
2. register plain resolved entries;
3. run an in-pack fixpoint until no progress;
4. collect residual external base SHAs once;
5. bulk-load READY external bases into a separate SHA-keyed map;
6. run the fixpoint again;
7. raise one structured residual dependency report if unresolved entries remain.

No database/network access during per-delta application. Do not merge external
bases into a pack-index table with default index zero. Remove targeted recursive
completion from the delta hot path. Use explicit repository keys and an
attempt-scoped byte-budget cache.

Test later unresolved REF bases, mixed REF/OFS chains, multiple external bases,
corrupt bases, out-of-order chains, no DB/network in the in-pack phase, and
secondary-key index safety.

# Slice 8 - Transaction and attempt isolation

- unique correlation, attempt, session, and pack IDs per network attempt;
- staged rows visible only to their attempt and never READY/have-eligible;
- set-based failed-attempt cleanup;
- orchestrator-owned publication of object readiness, certificates, and branch
  pointer;
- remove accidental low-level commits;
- if crash-resume staging is durable, document and enforce a two-phase staged to
  published protocol;
- verify no prior failed attempt affects the next attempt.

# Slice 9 - Remove obsolete behavior

After replacement tests are green:

- remove progressive-deepen code/tests;
- remove misleading force-full API;
- remove recursive per-base network completion from decoder;
- remove silent standard decode of the failed same pack;
- remove productive blank repo-key fallback;
- retain only memory-gated branch-full ORTEC recovery;
- keep all-refs/full-repository clone outside normal flow;
- preserve standard abapGit fallback when ORTEC is disabled or safely declines.

# Mandatory performance model

Every design and implementation slice must state:

- expected cardinality;
- estimated SQL and HTTP calls;
- row and byte batch limits;
- maximum single-object handling;
- table lookup complexity;
- maximum simultaneous payload/XSTRING copies;
- cache scope/invalidation;
- transaction count;
- expected behavior for 1, 1,000, 40,000, and 1,000,000 stored objects.

An incremental operation affecting `K` objects must not load/scan all `N` stored
objects. Tiny tests do not prove production performance.

# Mandatory acceptance scenarios

1. Cold first branch: no haves/deepen, blobless graph, bulk tip blobs, complete
   certificate.
2. Previously unseen second branch sharing 95-98%: shared rows reused, only new
   graph/tip blobs transferred.
3. Warm same-branch update: certified haves; unchanged blobs not fetched.
4. Branch A to B to A with unchanged A: no rematerialization.
5. Missing commit/tree after blobless fetch: hard failure, no certificate.
6. Missing promised historical blob outside current tip: allowed.
7. Missing current-tip blob: snapshot incomplete.
8. Filter absent: controlled fallback, no pretend partial success.
9. Arbitrary blob wants rejected: bounded fallback, never request per SHA.
10. Unexpected delta base: in-pack fixpoint, bulk store lookup, one recovery only.
11. Failed attempt: no ready state/certificate/ref publication.
12. ORTEC disabled: standard behavior unchanged.
13. Medium 5,000-object and large 40,000-object/batch tests.
14. About 100 affected objects with 1,000,000 stored keys: no repository-wide
    incremental read.

# Observability

Produce bounded aggregate diagnostics for correlation/attempt, repository,
branch/tip, fetch mode, advertised/requested capabilities, want/have count,
filter/deepen/thin flags, received bytes, pack/object type counts, graph/blob
missing counts, batches, rows/bytes, cache hits/misses, unresolved dependencies,
and phase timings. Never log credentials or payloads and never one line per
object in production.

# Deliverables

- `.memory/logs/variant_b_reconciliation.md`;
- `.memory/logs/variant_b_design.md`;
- `.memory/reviews/variant_b_design_review.md`;
- `.memory/diagrams/variant_b_flow.mmd`;
- per-slice performance design/audit reports;
- per-slice implementation/regression handoffs;
- final changed-object and migration list;
- IT8/ES6 live acceptance checklist.

# Hard stops

Stop and ask the owner only when:

- the target Azure connection does not advertise required filter capability;
- no bounded bulk current-tip blob materialization method exists and the only
  alternative is an unsafe unbounded response;
- required DDIC changes are incompatible with the target SAP/ABAP release;
- correctness requires major standard-abapGit restructuring instead of minimal
  hooks;
- current source proves an owner-approved invariant impossible;
- a correctness/performance trade-off is not decided here.

Do not substitute a previous workaround or request confirmation between slices.
