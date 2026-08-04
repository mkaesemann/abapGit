# SER-SLICE-1 — DOMA Version-Semantics Evidence Log

```text
PACKET=COMPACT_HANDOFF_V1
TASK=SERIALIZATION_SER_SLICE_1
STATUS=LOCAL_COMPLETE
DEPENDS_ON=.memory/logs/serialization_provider_design.md §2 (DR-003 open item),
  .memory/reviews/serialization_correctness_review.md (DR-003)
PRODUCTIVE_CODE_CHANGED=NO
STATE_MD_CHANGED=NO
```

## Scope note (owner-approved expansion over the original bootstrap text)

The original bootstrap handoff's SLICE 1 definition was documentation-only
("OUTPUT: a short addendum ... no productive code, no separate commit
needed"). The current combined SER-SLICE-1/2 owner prompt explicitly
requires an EXECUTABLE parity harness (§B3) with a defined test matrix and
its own stop condition for a missing seam. This log treats the CURRENT
explicit prompt as authoritative and supersedes the older bootstrap text's
narrower scope — implemented accordingly, with both a written evidence
addendum (this file) AND real ABAP Unit tests (see §4).

## 1. Method(s) traced

`ZCL_ABAPGIT_OBJECT_DOMA` (standard abapGit, read-only, unmodified):
`zif_abapgit_object~serialize`, `serialize_texts`, `zif_abapgit_object~exists`.
Cross-referenced against `ZCL_ABAPGIT_OBJECT_DTEL~serialize` (existing,
already-prefetched sibling type) for a same-family sanity comparison, and
against the live DDIC catalog (`DD01L`/`DD01T`/`DD07L`/`DD07T`/`DD01V`/
`DD07V`, `DD28S` view-selection-condition table) via read-only queries on
the connected IT8 system.

## 2. Confirmed version semantics (the DR-003 answer)

```text
MAIN_LANGUAGE_READ        CONFIRMED_FROM_ACTIVE_SOURCE + CONFIRMED_FROM_SAP_API_CONTRACT
  zif_abapgit_object~serialize calls `DDIF_DOMA_GET( name = <domain>
  state = 'A' langu = mv_language )` - EXPLICIT active-version request.
  If `ls_dd01v IS INITIAL OR lv_state <> 'A'` (no active version could be
  read), the method RETURNs immediately - NO DD01V/DD07V_TAB/DD01L_EXTRA/
  text/longtext nodes are added, and NO exception is raised. This is a
  silent-empty-payload contract, not a miss/error signal.

TRANSLATION_LANGUAGE_READS  CONFIRMED_FROM_ACTIVE_SOURCE + CONFIRMED_FROM_SAP_API_CONTRACT
  `serialize_texts` calls `DDIF_DOMA_GET( name = <domain> langu = <lang> )`
  for every OTHER language found in DD01V/DD07V - the `state` parameter is
  OMITTED. `DDIF_DOMA_GET`'s own signature (read directly from the live
  system, includeSignature=true) declares `VALUE(STATE) TYPE ddobjstate
  DEFAULT 'A'` - so every translation-language read is ALSO active-version
  only, identical to the main read. There is NO DOMA code path anywhere
  that reads an inactive version.

FIXED_VALUE_FILTER         CONFIRMED_FROM_ACTIVE_SOURCE
  `DELETE lt_dd07v WHERE appval = abap_true` removes application/customer-
  appended fixed values from the serialized main-language table; the
  translation-language loop iterates the ALREADY-filtered `it_dd07v`
  parameter, so this filter is inherited consistently for every language.

**CRITICAL FINDING - the original SER-3 "read DD01V/DD07V directly" plan
would have introduced a REAL parity bug, now corrected before any
provider code exists:**

  DD01V_IS_NOT_VERSION_FILTERED   CONFIRMED_FROM_SAP_API_CONTRACT (live
    DDIC metadata, not inferred from naming)
    `DD01V` is a DATABASE VIEW (`DD02L-TABCLASS='VIEW'`,
    `VIEWCLASS='D'`) joining `DD01L` + `DD01T` on `DOMNAME` ONLY (`DD28S`
    selection conditions for CONDNAME='DD01V' contain no AS4LOCAL/AS4VERS
    predicate at all - verified by direct query). `DD01V` does not even
    PROJECT the `AS4LOCAL`/`AS4VERS` columns as output fields (confirmed
    via `DD03L`). A bulk `SELECT * FROM DD01V` THEREFORE RETURNS BOTH
    ACTIVE AND INACTIVE DD01L/DD01T ROWS MIXED TOGETHER, with no field
    available to tell them apart. Any domain with an uncaptured pending
    change would silently produce WRONG (non-active) or DUPLICATE header
    data via a naive `DD01V` bulk read - a real, previously undetected
    output-parity risk for the future DOMA provider.
    REQUIRED_MITIGATION (for SER-3, not implemented here): a bulk DOMA
    header/text provider MUST select from the BASE TABLES directly -
    `DD01L WHERE as4local = 'A' AND as4vers = '0000'` and `DD01T WHERE
    as4local = 'A' AND as4vers = '0000'` - mirroring DTEL's own already-
    correct `ZCL_ABAPGIT_OBJECT_DTEL` pattern (`DD04L`/`DD04T WHERE
    as4local = 'A' AND as4vers = '0000'`, confirmed by direct source read)
    - NOT a plain `SELECT * FROM DD01V`.

  DD07V_IS_VERSION_FILTERED   CONFIRMED_FROM_SAP_API_CONTRACT (live DDIC
    metadata)
    `DD07V` (also `TABCLASS='VIEW'`) DOES bake in a version filter at the
    view-definition level: `DD28S` selection conditions for
    CONDNAME='DD07V' show `DD07L.AS4LOCAL = 'A' AND DD07T.AS4LOCAL = 'A'`
    (positions 5-6, joined to the DOMNAME+DOMVALUE_L join keys at
    positions 1-4). A bulk `SELECT * FROM DD07V` is therefore SAFE and
    already active-only - unlike `DD01V`. For defense-in-depth and
    consistency with DTEL's belt-and-suspenders style, SER-3 SHOULD still
    prefer `DD07L`/`DD07T WHERE as4local = 'A' AND as4vers = '0000'`
    directly (same tables the header provider already needs), but reading
    `DD07V` itself would NOT be a correctness bug the way reading `DD01V`
    would be.

EMPTY_VS_NONEXISTENT_EQUIVALENCE   CONFIRMED_FROM_ACTIVE_SOURCE (traced
  into `DDIF_DOMA_GET`'s own ABAP source, read live)
  A domain that does not exist AT ALL and a domain that exists ONLY with
  an inactive version are OBSERVATIONALLY IDENTICAL from
  `zif_abapgit_object~serialize`'s perspective: `DDIF_DOMA_GET`'s inner
  `CHECK NOT GOTSTATE IS INITIAL.` returns before populating `DD01V_WA` in
  BOTH cases (nonexistent domain -> `GOTSTATE` stays initial; inactive-
  only domain requested with `STATE='A'` -> also resolves to a non-'A',
  effectively-initial outcome for this caller's purposes) - no exception
  in either case, `ls_dd01v IS INITIAL` catches both uniformly. This
  equivalence is why the parity harness (§4) does not need to fabricate a
  live inactive-DDIC-modification fixture (a real system side effect this
  slice avoids) to pin the "inactive domain/version behavior" required
  test case - the "nonexistent object" fixture already exercises the
  identical code path.

FIXED_VALUE_TEXTS / LANGUAGES   CONFIRMED_FROM_ACTIVE_SOURCE
  Per-language text rows are matched by `(domvalue_l, domvalue_h)`, not by
  `valpos` (valpos can be renumbered on deserialize) - a "no translation"
  placeholder row is kept (with cleared text fields) rather than omitted,
  preserving row-count parity across languages.

DOCUMENTATION/LONGTEXT   CONFIRMED_FROM_ACTIVE_SOURCE, NOT DOMA-specific
  `serialize_longtexts`/`deserialize_longtexts` (inherited from
  `zcl_abapgit_objects_super`, ID `'DO'`) is the SAME shared mechanism
  used by dozens of other object types in this codebase - no DOMA-unique
  logic to trace further; out of this slice's evidence scope by design.

MASTER_LANGUAGE_ONLY / LXE GATES   CONFIRMED_FROM_ACTIVE_SOURCE, shared
  `mo_i18n_params` concerns (main_language_only, is_lxe_applicable),
  identical mechanism already used by other object types - not DOMA-
  specific.

EXISTS() SEMANTICS   CONFIRMED_FROM_ACTIVE_SOURCE
  `zif_abapgit_object~exists` is a plain `SELECT SINGLE domname FROM
  dd01l WHERE domname = ms_item-obj_name` with NO version filter at all -
  existence is defined purely by "at least one DD01L row exists,
  regardless of AS4LOCAL/AS4VERS". This is EXACTLY the same query
  `ZCL_ABAPGIT_ORTEC_BULK_EXISTS=>build_doma_buffer` already uses
  (SER-SLICE-0, cross-validated, no discrepancy).
```

## 3. No UNKNOWN remains that affects output parity

Every input the provider design (§2) needs is now `CONFIRMED_FROM_
ACTIVE_SOURCE` and/or `CONFIRMED_FROM_SAP_API_CONTRACT`. The one item that
could have remained open (which underlying tables/filter a bulk provider
must use) is now resolved with an EXACT, evidence-backed answer, including
the asymmetry between `DD01V` (unsafe to bulk-read directly) and `DD07V`
(safe). SER-SLICE-3's instructions are decision-free on this point.

## 4. Parity harness

```text
LOCATION   src/ortec/zcl_abapgit_ortec_ser_pref_ext.clas.testclasses.abap
           (NEW; placement note: this is a TEMPORARY home - the sibling
           DTEL prefetch cache already lives on this class and SER-3's new
           ZCL_ABAPGIT_ORTEC_SER_PROV_DD will own both DOMA and DTEL once
           created; relocate these tests there in SER-SLICE-3, do not
           duplicate them. Chosen because (a) no new global object may be
           created per this task's Phase A object-creation boundary, and
           (b) the standard ZCL_ABAPGIT_OBJECT_DOMA class must not be
           touched - adding a testclasses include to a standard abapGit
           object is a standard-abapGit change beyond the approved hooks.)
METHOD     no provider exists yet, so the harness calls the REAL
           ZCL_ABAPGIT_OBJECT_DOMA~serialize()/~exists() as a black box
           (constructed directly - the class is CREATE PUBLIC via its
           superclass) and asserts on the RENDERED XML STRING
           (ZCL_ABAPGIT_XML_OUTPUT~render()) - no productive serialization
           logic is reimplemented or duplicated in the test.
FIXTURES   Real, universal, permanently-stable SAP Basis domains,
           confirmed present via live read-only DDIC queries (never
           created or modified by this slice):
             XFELD  - DATATYPE=CHAR LENG=1, 2 fixed values with English
                      texts ('X'->"Yes", ''->"No"), no long text.
             CHAR30 - DATATYPE=CHAR LENG=30, VALEXI='' (no fixed values).
             ZZZZ_SLICE1_NOT_A_REAL_DOMAIN - fabricated, guaranteed-absent
                      name (no doubling needed or attempted - this is a
                      real absence check against the real DD01L table,
                      exactly like the SLICE-0 lesson learned about never
                      doubling DDIC catalog tables).
TESTS      active_with_fixed_values     - XFELD: DATATYPE/LENG/fixed
                                          value + text content present.
           active_without_fixed_values  - CHAR30: DATATYPE/LENG present,
                                          NO fixed-value content (empty
                                          optional collection case).
           nonexistent_or_inactive      - fabricated name: no DD01V node,
                                          no exception (covers BOTH
                                          "nonexistent object" and
                                          "inactive domain/version
                                          behavior" per the proven
                                          equivalence in §2).
           exists_matches_serialize     - exists()=TRUE for XFELD,
                                          FALSE for the fabricated name
                                          (standard-path reference result).
           output_is_stable             - two independent serialize()
                                          calls for XFELD render BYTE-
                                          IDENTICAL XML (stable/canonical
                                          output requirement).
NOT_COVERED  Multi-language fixed-value TEXT translation (a specific
           second language's DD07T/DD01T row) is not separately pinned
           with its own dedicated fixture in this slice - the MECHANISM
           (match-by-value, not-by-valpos, placeholder-on-miss) is fully
           confirmed from source (§2) and is not a DOMA-specific novel
           risk; SER-3's PARITY PROOF requirement (provider_design.md §2)
           already mandates a byte-identical comparison test before
           implementation is authorized, which will naturally exercise
           this path against real repository content at that time. This
           is disclosed, not silently skipped - no missing production
           seam blocks it, it is simply judged lower-value to fabricate a
           dedicated multi-language fixture for a mechanism already fully
           evidenced from source.
```

## 5. No provider or orchestration code was started

`ZCL_ABAPGIT_ORTEC_SER_PROV_DD`, `ZIF_ABAPGIT_ORTEC_SER_PROV`, and every
SER-SLICE-2 object remain uncreated. Only the existing, already-created
`ZCL_ABAPGIT_ORTEC_SER_PREF_EXT` gained one new `.clas.testclasses.abap`
include and its `.clas.xml`'s `WITH_UNIT_TESTS` flag - no new method, no
new CLASS-DATA, no behavior change to that class's own production code.

## 6. Local validation

```text
git diff --check                 PASS
changed-file inspection          PASS (exactly 2 files: new testclasses
                                  include + WITH_UNIT_TESTS flag on the
                                  SAME existing class's .clas.xml)
ABAP method-name length check    PASS (longest: active_without_fixed_
                                  values, 27 chars)
local syntax/error diagnostics   PASS (get_errors clean)
```

IT8 ABAP Unit execution is still required (see handoff) - not claimed as
executed by this agent.
