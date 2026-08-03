# SER-5 — WAPA Review and Design

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_SER5_WAPA_REVIEW
STATUS=REVIEWED, DISPOSITION_DECIDED
DEPENDS_ON=serialization_ser0_audit_ortec.md §5
```

## Disposition

```text
DECISION = KEEP_WITH_CORRECTIONS (now) + REFACTOR_INTO_PROVIDER (later,
           separate slice, gated on tests existing first)
```

## Evidence-based review against each required axis

```text
EXACT_STANDARD_OUTPUT_PARITY  Not independently proven this pass - no
  existing test suite exists to compare against (see TEST_COVERAGE below).
  This is the single largest open risk for WAPA specifically, independent
  of any architecture question. REQUIRED BEFORE any further WAPA change
  (including the later provider-wrapping slice).

LANGUAGE/TEXT_AND_METADATA_SEMANTICS  SOURCE_CONFIRMED: dual-language
  handling (sy-langu + master_language via cl_o2_api_pages=>
  get_master_language()) with OTR-guid-based translation conversion when a
  page's own language differs from the layout language
  (cl_o2_helper=>call_int_to_ext_converter). This mirrors the legacy API's
  own semantics by design (the class's exists() check explicitly matches
  cl_o2_api_application=>load's "active OR inactive version" rule). No
  gap identified in the STATIC read of this logic; only lacks a test that
  PROVES it against the legacy path's actual output for a translated page.

AUTHORIZATION_AND_INACTIVE-OBJECT_BEHAVIOR  exists() intentionally matches
  legacy semantics (version IN ('A','I')). Content reads are explicitly
  scoped to the active version only (O2PAGEVH/O2PAGPAR/O2PAGCON reads use
  version = 'A' or an active-version IMPORT key) - an object with ONLY an
  inactive version exists() = true but serialize() would read no active
  content. Whether the LEGACY path has the exact same "exists but nothing
  to serialize" behavior for an inactive-only page was NOT verified this
  pass - flagged as a required parity test case (T-WAPA-3 below), not
  assumed either way.

PAGE/CONTENT_COMPLETENESS  Page-level metadata reads (O2PAGDIR/O2PAGDIRT/
  O2PAGEVH/O2PAGPAR/O2PAGPART) are already bulk (FOR ALL ENTRIES, one
  round trip for the WHOLE application's page set) - genuinely efficient,
  no gap. O2PAGCON content itself is read per-page inside a LOOP via
  `IMPORT ... FROM DATABASE o2pagcon(tr) ID ls_pagecon_key` - this is
  TYPE-INHERENT (O2PAGCON is a cluster/pool table; IMPORT FROM DATABASE
  with an explicit key is the standard, safe access pattern for such
  tables - there is no FOR ALL ENTRIES equivalent for a keyed cluster
  IMPORT without a custom multi-key retrieval loop, which would still be
  N reads, just batched differently, and carries its own correctness risk
  of assuming an undocumented physical storage layout). NOT changing this
  is the SAFE default; a future dedicated micro-design could investigate
  whether SAP provides any bulk cluster-read API, but this is NOT assumed
  to exist and is NOT designed here.

PAGE-COUNT_AND_BYTE-BOUNDED_PROCESSING  The source's own comment ("do not
  add an artificial WAPA page size cap") reflects a DELIBERATE choice
  already made by the implementer: a page's raw content is one xstring,
  matching abapGit's own existing per-object output shape, and this is
  consistent with every other object type's serialize() contract (no
  object type in this codebase truncates its own output based on size).
  RECOMMENDATION: do not introduce a new page-count/byte cap either -
  this would be a NEW behavior inconsistent with the rest of the codebase,
  not a gap to fix. The GENERIC batch orchestration's own
  c_max_object_output_bytes (SER-2 §6) already provides the SCHEDULING-
  level protection (isolating an unexpectedly huge WAPA page's future
  batches) without touching WAPA's own serialize() contract.

CLEANUP_AND_PEAK_MEMORY  build_context() loads all pages' metadata for ONE
  application at once (bounded by that application's own page count, not
  the whole repository) - acceptable, matches the existing per-object
  serialize() memory shape used everywhere else in this codebase. No
  cross-application accumulation identified.

SAP_RELEASE_COMPATIBILITY  Not independently re-verified this pass (would
  require access to multiple SAP release levels); flagged UNKNOWN, not
  assumed compatible or incompatible. The existing is_wapa_active() switch
  with a clean legacy fallback (SOURCE_CONFIRMED) already provides the
  correct SAFETY NET if a specific release turns out incompatible - no
  new release-detection logic is designed here.

SAFE_FALLBACK  SOURCE_CONFIRMED: is_wapa_active() = false routes to the
  unmodified legacy cl_o2_api_application/cl_o2_api_pages path with no
  code change required at the call site (zcl_abapgit_object_wapa.clas.abap
  L547/L613) - this is exactly the pattern the rest of this design follows
  and needs no correction.
```

## The one mandatory correction: test coverage

```text
GAP        Zero ABAP Unit tests exist for ZCL_ABAPGIT_ORTEC_WAPA today
           (SOURCE_CONFIRMED, ORTEC sub-audit §8).
WHY_NOW    Any future change to this class (including the later provider-
           wrapping refactor) has NO regression safety net today. This is
           independent of, and higher priority than, any architecture
           change - fixing it does not require or imply any design
           decision beyond "write tests for the class as it exists now."
REQUIRED_TESTS (minimum set, CL_OSQL_TEST_ENVIRONMENT pattern already
  proven working in this workspace per user memory notes - reuse it):
  T-WAPA-1  exists() true for an application with only an active version,
            true for inactive-only, false for neither (matches the
            legacy load() contract's documented "active OR inactive"
            rule).
  T-WAPA-2  serialize() output for a simple single-page application
            matches a manually-constructed expected file set (page dir
            text, event handlers, parameters, content xstring, XML
            wrapper) - a golden-file/structural comparison, not a live
            legacy-API diff (no live system in this workspace).
  T-WAPA-3  An application with an ACTIVE-only page vs one with an
            INACTIVE-only page - confirms the actual serialize() behavior
            in the inactive-only case (raises, or produces an empty page,
            whichever the code does) so this becomes a PINNED, known
            behavior instead of an open question.
  T-WAPA-4  A page whose own language differs from the master language,
            confirming the OTR-guid translation-conversion call is invoked
            with the correct converter type argument (structural test via
            a test double for cl_o2_helper if feasible, otherwise a
            documented manual verification step).
  T-WAPA-5  Multiple pages, confirming NO cross-page data leakage (each
            page's own O2PAGEVH/O2PAGPAR rows do not bleed into another
            page's file set) - directly protects the FOR ALL ENTRIES bulk-
            read-then-per-page-filter pattern.
```

## Later slice (not authorized now): REFACTOR_INTO_PROVIDER

```text
GOAL       Wrap ZCL_ABAPGIT_ORTEC_WAPA behind zif_abapgit_ortec_ser_prov
           (SER-3 §1) so WAPA-heavy repositories (the owner's stated
           motivating case - thousands of WAPA/BSP artifacts for a
           deployed UI5 application) benefit from SER-2's batch RFC
           dispatch and per-batch (rather than always-per-application)
           metadata prefetch.
ENTRY_CONDITION  T-WAPA-1..5 exist and pass; SLICE 1/2 (bulk-exists
           verification + adaptive batch core) are implemented and
           measured; a dedicated design+review cycle is run for this
           slice specifically (not decided in advance here, since it
           touches a class with today's zero test coverage - a design
           written before tests exist would have nothing to validate
           itself against).
NOT_AUTHORIZED_NOW  No WAPA architecture change is part of this
           completion report's approved scope.
```
