# SSFO Local Cache Implementation Handoff

task=SSFO_LOCAL_CACHE_IMPLEMENTATION_20260814
status=BLOCKED
artifact=C:\Projects\abap\abapGit\.memory\handoffs\ssfo_local_cache_implementation.md

## Implementation slice

Approved scope: `ZAOG_SSFO_CACHE`, `ZCL_ABAPGIT_ORTEC_SSFO_CACHE`,
`ZCL_ABAPGIT_ORTEC_SER_CACHE`, `ZCL_ABAPGIT_ORTEC_GIT_SWITCH`,
`ZCL_ABAPGIT_ORTEC_SER_ORCH`, `Z_ABAPGIT_ORTEC_SER_BATCH`,
`ZCL_ABAPGIT_ORTEC_CACHE_ADMIN`, `ZABAPGIT_ORTEC_CACHE_ADMIN`, and direct
test includes only. No standard source was read for modification or changed.

Performance map: form-local cache work is bounded by the approved design to
9 SQL statements on a hit, 0 HTTP calls, one cache payload per operation, a
44 MiB explicit cache-owned peak overlap, metadata-only eviction batches of
500 rows, and caller-owned normal LUW. No planned path performs SQL or HTTP
per file, DOM node, or cache row.

## Live verification

- `CL_SSF_FB_SMART_FORM=>ENQUEUE` exposes the required `SHOW` call and
  `MODIFICATION_LANGUAGE`; `=>DEQUEUE` is present.
- `ZCL_ABAPGIT_OBJECTS=>SERIALIZE` is the proven full-result cache boundary.
- `Z_ABAPGIT_ORTEC_SER_BATCH` exposes optional
  `IV_FDT0_CACHE_ACTIVE`; adding the approved optional SSFO flag is
  signature-compatible.
- All signature dependencies named by the final design exist in IT8:
  `STXFADM`, `STXFCONTS`, `STXFCONT`, `STXFOBJT`, `STXFTXT`, `STXFADMT`, and
  `STXFVART`.
- The table and both new classes did not exist before this attempt.

## Productive change made

`ZAOG_SSFO_CACHE` was created in `$ABAPGIT_ORTEC_SERIAL_CORE` and activated
with the final design's leading `MANDT` key, `FORMNAME` key, identity/signature
fields, RAWSTRING payload, size, and timestamps.

Validation passed:

- live editor diagnostics: 0 errors;
- activation: successful;
- `DD02L-CLIDEP = 'X'`;
- `DD03L`: position 1 `MANDT`, key flag `X`, rollname `MANDT`.

## Blocking contradiction

The final design requires secondary index `ZAOG_SSFO_CACHE~001` on
`LAST_USED_AT, FORMNAME`. The IT8 ADT table DDL parser rejects an inline
`define index` declaration (`Unexpected token @`, `Unexpected character ~`).
Existing ORTEC secondary indexes are stored separately in `DD12L`/`DD17S`,
not in the table source. The available write tools can create a table shell but
expose no DDIC-index create/update operation. Current `DD12L` has zero rows
for `ZAOG_SSFO_CACHE`.

Do not create cache/router/switch/worker/admin code without that index: it
would depart from the approved bounded oldest-first eviction design and its
performance gate. Obtain a DDIC-index-capable ADT/API path (or an owner-approved
design revision), add and activate `001`, then resume from the verified source
surface above.

## Validation not executed

No SSFO cache/router/switch/admin source exists yet, so their syntax,
activation, ABAP Unit, parity, lock, cross-client, invalidation, and scale
acceptance tests were not run. No live acceptance is claimed.

next=Provision ZAOG_SSFO_CACHE secondary index 001, then resume implementation without changing standard source.

## Resume update 2026-08-14

task=SSFO_LOCAL_CACHE_IMPLEMENTATION_RESUME_20260814
status=PARTIAL_SAFE_WIRING_ACTIVE

The prerequisite index `ZAOG_SSFO_CACHE~001` is active. The following
objects are created and active: `ZCL_ABAPGIT_ORTEC_SSFO_CACHE` and
`ZCL_ABAPGIT_ORTEC_SER_CACHE`. The new SSFO cache class is intentionally a
standard-serialization pass-through at this checkpoint; it does not read or
write `ZAOG_SSFO_CACHE` and therefore cannot return a stale cached result.

Active execution-path changes:

- `ZCL_ABAPGIT_ORTEC_GIT_SWITCH`: added session-local
  `IS_SSFO_CACHE_ACTIVE` / `SET_SSFO_CACHE_ACTIVE`, defaulting false.
- `ZCL_ABAPGIT_ORTEC_SER_CACHE`: FDT0 retains its existing wrapper; SSFO
  calls the new pass-through cache class; all other types call standard
  serialization.
- `Z_ABAPGIT_ORTEC_SER_BATCH`: adds optional
  `IV_SSFO_CACHE_ACTIVE`, applies it in the worker, routes via
  `ZCL_ABAPGIT_ORTEC_SER_CACHE`, and resets it on exit.
- `ZCL_ABAPGIT_ORTEC_SER_ORCH`: sets and resets the SSFO flag on UUID,
  success, and exception exits; routes sequential fallback through the
  router; forwards the worker flag.

Validation completed:

- activated new SSFO cache, router, switch, worker, and ORCH sources;
- real IT8 syntax checks passed for the changed active source, with only
  existing ORCH/switch documentation warnings;
- `ZCL_ABAPGIT_ORTEC_SER_ORCH` ABAP Unit: 49/49 passed.

Blocking remaining scope: implement sections 4.2, 5, 6, and 7 of the
approved design in `ZCL_ABAPGIT_ORTEC_SSFO_CACHE`, then add direct tests,
admin/report changes, and the required IT8 parity/invalidation/client/lock/
SAT acceptance gates. Do not describe this checkpoint as cache-enabled or
production ready.

next=Implement the bounded lock-scoped SSFO cache core before any admin or rollout work.

## Core implementation update 2026-08-14

task=SSFO_LOCAL_CACHE_CORE_IMPLEMENTATION_20260814
status=PARTIAL_BLOCKED

Implemented and activated:

- `ZCL_ABAPGIT_ORTEC_SSFO_CACHE`: lock-scoped effective-language resolution,
  complete typed context identity, active-only bounded dependency signature,
  payload validation/replay, miss serialization, pre/post-publication recheck,
  bounded metadata-only eviction, and no normal `COMMIT WORK`/`ROLLBACK WORK`.
- `ZCL_ABAPGIT_ORTEC_CACHE_ADMIN`: combined FDT0/SSFO serialization-cache
  clear and admin-owned bounded SSFO purge with explicit commit/rollback.

No router, switch, worker, orchestrator, or standard object was changed in
this update; the existing partial-safe wiring was preserved.

Validation executed:

- `ZCL_ABAPGIT_ORTEC_SSFO_CACHE` live editor diagnostics: clean.
- `ZCL_ABAPGIT_ORTEC_SSFO_CACHE` active syntax: clean.
- Cache class active source and inactive main source had matching ETag/hash
  before the failed test-scaffold attempt.
- `ZCL_ABAPGIT_ORTEC_CACHE_ADMIN` activation: successful; active/inactive
  state has no divergence.
- Cache-class ABAP Unit command: no test classes found (0 tests).

Blocking remaining work:

- Direct unit tests could not be activated. The dedicated testclasses include
  was created, but this SAP release requires local-friend activation ordering
  that ADT rejected while the test include referenced private helpers. The
  restored placeholder include is syntax-clean but currently self-locked
  (`ExceptionResourceNoAccess: User MICHAELK is currently editing`).
- The SSFO cache test include activation remains required before any ABAP Unit
  evidence can be claimed.
- Runtime parity, effective-language, inactive-source, client-isolation,
  corruption, concurrency, capacity, direct lock-hold, and SAT gates remain
  unexecuted. The productive switch/rollout is blocked.
- The cache-admin report was not changed; invoke the newly active admin class
  method from an approved follow-up UI slice.

next=Clear the ABAP editor lock, activate the test include, add class-local
tests through the release-supported local-friend pattern, then run the full
required IT8 acceptance gates before enabling the SSFO cache switch.