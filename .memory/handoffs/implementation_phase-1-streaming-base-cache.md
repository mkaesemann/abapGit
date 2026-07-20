# Implementation handoff: phase-1-streaming-base-cache

## Scope
- Implement a standalone, byte-budgeted LRU cache for delta-base object bytes.
- Keep the work isolated from the live decode path.
- Add unit tests for the cache itself.

## Implemented
- Added class [src/ortec/git/zcl_abapgit_ortec_base_cache.clas.abap](src/ortec/git/zcl_abapgit_ortec_base_cache.clas.abap) with singleton access, get/put/clear semantics, LRU eviction, and oversize-object no-op handling.
- Added ABAP Unit coverage in [src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap](src/ortec/git/zcl_abapgit_ortec_git_tests.clas.testclasses.abap) for round-trip storage, missing lookups, LRU eviction, oversize admission, and clearing.

## Validation
- Workspace diagnostics: no errors reported for either affected file.
- ABAP naming check: no method names exceeded the 30-character limit.

## Notes
- The cache is intentionally self-contained and ready for later integration into the streaming decode design.
