# Variant B Package D — bootstrap brief

Status: `READY_FOR_D0_DESIGN`

Validated starting HEAD:

```text
29199f629773c676e0eaa2f3a006f5167d304ae8
```

## Authority and use

This is a compact Package D bootstrap, not a replacement for current productive
source. Current source and reproducible evidence outrank historical memory.

Use this file to avoid rereading Package C issue history and large archived
state snapshots.

## Closed baseline

Package C is `SAP_VALIDATED_COMPLETE` in SAP IT8:

- activation, ATC and ABAP Unit passed;
- cold and warm-unchanged flows passed;
- snapshot publication is constrained to `HIST_LEVEL = F` plus
  `SNAP_STATE = C`;
- repository bookkeeping and cache administration passed;
- bounded large-repository blob reconstruction is functionally validated;
- final broad performance tuning remains deferred and non-blocking.

Package D must preserve this baseline.

## Package D boundaries

### D0

One shared design and three reviews for Slices 7 and 8. No productive coding.

### D1

Generalized bounded external delta-base resolution:

- stable base identity;
- iterative in-pack fixpoint;
- deduplicated bulk external-base load;
- bounded recovery;
- no per-delta SQL or HTTP;
- unified streaming/non-streaming semantics;
- bounded base cache and payload copies.

### D2

Final attempt/session/transaction isolation:

- unique attempt correlation;
- staged data not globally READY;
- explicit staged visibility;
- set-based failed-attempt cleanup;
- orchestrator-owned publication;
- explicit commit ownership;
- crash-safe and idempotent retry;
- stale-attempt rejection;
- Package C `F/C` invariant preserved.

## Historical issues already resolved

Do not reopen historical Package C issue-fixing threads without current
regression evidence, including:

- missing full-complete transition before snapshot publication;
- incomplete cold repository bookkeeping;
- cache-clear ownership and missing certificate cleanup;
- oversized SHA range in blob reconstruction;
- local test-friend include serialization;
- manifest duplicate-path and fetch-list deduplication fixes.

These are regression constraints, not active design tasks.

## Package D source-discovery focus

Start from current source around:

- ORTEC pack streaming and fastpath decoders;
- ORTEC delta resolver and delta application;
- base-cache implementation;
- object-store staging/readiness APIs;
- fetch session, pack metadata/index and raw-pack persistence;
- commit materialization state and repository state;
- all current commit/rollback sites;
- existing delta, decoder, persistence and Package C regression tests.

Do not scan unrelated standard abapGit code unless the current ORTEC call chain
crosses that boundary.

## Non-negotiable constraints

- correctness > performance > maintainability;
- no missing/unknown data interpreted as deletion;
- no per-object SQL or HTTP;
- no per-base HTTP repair;
- no blank productive repository key;
- no uncertified haves;
- no deepen/shallow in Variant B;
- no unbounded ranges, DB transfers or payload copies;
- no silent decode fallback;
- standard behavior unchanged when ORTEC is disabled;
- Package E owns legacy removal.

## Token discipline

Initial reads are limited to:

1. `.memory/state.md`
2. `.memory/handoffs/variant-b-package-c-c2-checkpoint.md`
3. this bootstrap
4. `.github/prompts/variant-b.prompt.md`

Read additional memory only for a named unresolved decision. Store detailed
findings in Package D artifacts and return compact evidence packets.
