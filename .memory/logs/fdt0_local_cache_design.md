# FDT0 Local Runtime Cache — Design

```text
TASK_ID=FDT0_LOCAL_CACHE_DESIGN_REVISION_20260812
BASELINE_COMMIT=b4f41e38372a0fe9f67483f71e968b1885b594c1
SOURCE_SYSTEM=IT8 only
STATUS=DESIGN_REVISED_CYCLE_2_AWAITING_REVIEW
SCOPE=Design only, no code/DDIC/test/transport/state/diagram changes made
REVISION_OF=Cycle 1: AR-1-001 (BLOCKER, CLOSED), AR-1-002 (BLOCKER,
  CLOSED), AR-1-003 (MAJOR, CLOSED). Cycle 2: AR-2-001 (MAJOR, CLOSED —
  IT-01 now a literal, non-waivable §11 stop condition), AR-1-001-RESIDUAL
  (MINOR, wording reconciled, non-blocking) — from
  .memory/reviews/fdt0_local_cache_adversarial_review.md Cycles 1-2 — see
  sections 17 / 17a (finding-closure ledgers) for evidence
```

## 0. Owner requirement (verbatim scope)

Cache unchanged BRF+ FDT0 application serializations (option b). Cache
table is transparent, delivery class `#L`, data maintenance
`#RESTRICTED`, following existing `ZAOG_*` buffer-table conventions.
Local runtime accelerator only — cache content is never transported or
shared cross-system.

## 1. Evidence table

| Fact | Status | Source |
|---|---|---|
| `Z_ABAPGIT_ORTEC_SER_BATCH` loop calls `zcl_abapgit_objects=>serialize` once per TADIR row inside a `TRY`/`CATCH zcx_abapgit_exception`; no per-type provider exists for `FDT0` — it falls into `WHEN OTHERS` → `provider_fallback = 1` | CONFIRMED | Live read, `Z_ABAPGIT_ORTEC_SER_BATCH` source, this session |
| `ZCL_ABAPGIT_ORTEC_SER_ORCH=>route_to_sequential_fallback` independently calls the exact same `zcl_abapgit_objects=>serialize( is_item = ls_item io_i18n_params = ... )` shape, for forced-sequential objects and as the batch/merge failure recovery path | CONFIRMED | Live read, `ZCL_ABAPGIT_ORTEC_SER_ORCH` method `route_to_sequential_fallback`, this session |
| `zcl_abapgit_objects=>serialize` sets `rs_files_and_item-item = is_item` (echoed from the caller) plus `item-inactive` computed from `li_obj->is_active( )`; files get `sha1` computed via `zcl_abapgit_hash=>sha1_blob` | CONFIRMED | Live read, `zcl_abapgit_objects=>serialize`, this session |
| `zcl_abapgit_object_fdt0~serialize` calls `export_xml_application` (external schema), parses to a DOM, calls the private `filter_xml_serialize`, then `io_xml->set_raw`. `filter_xml_serialize` strips `ComponentReleases` and every volatile field (`CreationUser/Timestamp`, `ChangeUser/Timestamp`, `User`, `Timestamp`, all `Tr*` fields, `OversId`, `SoftwareComponent`, `DevelopmentPackage`) plus the root node's `Client/Date/SAPRelease/Server/SourceExportReqID/SystemID/Time/User` attributes | CONFIRMED | Live read, `ZCL_ABAPGIT_OBJECT_FDT0`, this session |
| `get_application_id`/`check_is_local`/`changed_by`/`exists` all resolve via `SELECT ... FROM fdt_admn_0000s WHERE object_type = 'AP' AND name = ms_item-obj_name` | CONFIRMED | Live read, `ZCL_ABAPGIT_OBJECT_FDT0`, this session |
| `fdt_admn_0000s` key is `id` only (`fdt_inc_key_0001`); data include `fdt_inc_admn_0000_data` carries `object_type, version, name, application_id, access_level, cr_user, cr_timestamp, ch_user, ch_timestamp, deleted, transported, tv_state, tv_timestamp, p_tv_timestamp, obsolete, local_object, ...` | CONFIRMED | Live read, `FDT_ADMN_0000S`/`FDT_INC_ADMN_0000_DATA`/`FDT_INC_QUERY_0000_DATA`/`FDT_INC_KEY_0001`, this session |
| Each FDT object id (application row itself, and every nested sub-object row) has exactly **one** row in `fdt_admn_0000s` — no version-history duplication was found; `version` is a stamp incremented in place, not a new row per save. Verified on the largest IT8 application (`A0DEC84CFD7ED61EE10000000A428724`, 5179 nested rows, single row for `id = application_id` itself at `version = 000007`) | MEASURED | Live `SAPQuery` against `FDT_ADMN_0000S`, IT8, this session |
| A single application's nested-object graph can be large: one IT8 application has 5179 non-`AP` admin rows sharing its `application_id`; 14 other applications exceed 700 rows each | MEASURED | Live `SAPQuery` GROUP BY, IT8, this session |
| System-wide `FDT_ADMN_0000S` has 307 non-deleted `AP` (application) rows total | MEASURED | Live `SAPQuery`, IT8, this session |
| `ZAOG_OBJ_STORE` (closest existing ZAOG buffer table) is delivery class `#L`, data maintenance `#ALLOWED` (not `#RESTRICTED`); `ZAOG_OBJ_INDEX` is class `#A`/`#ALLOWED`. The owner's explicit ask for `#RESTRICTED` on the new table is a deliberate divergence from the `ZAOG_OBJ_STORE` precedent, not an oversight | OWNER_DECISION | `OWNER_REQUIREMENT` (verbatim), cross-checked against live DDIC read of `ZAOG_OBJ_STORE`/`ZAOG_OBJ_INDEX`, this session |
| `ZCL_ABAPGIT_ORTEC_OBJ_STORE=>store_object`/`store_objects` write content-addressed rows via a plain `MODIFY ... FROM (TABLE)`, no explicit `COMMIT WORK`, no duplicate-key `TRY`/`CATCH` — the established ORTEC idiom for a content-addressed cache row is an idempotent upsert, not insert-with-catch | CONFIRMED | Live read, `ZCL_ABAPGIT_ORTEC_OBJ_STORE`, this session |
| `ZCL_ABAPGIT_ORTEC_GIT_SWITCH` already hosts every ORTEC feature toggle (`is_serial_prefetch_active`, `is_wapa_active`, `is_serial_batch_active`, ...), all defaulting OFF via `CLASS-DATA ... VALUE abap_false`, flipped only for the duration of one ORCH run or via a persisted per-repo setting through `zcl_abapgit_persistence_ortec` | CONFIRMED | Live read, `ZCL_ABAPGIT_ORTEC_GIT_SWITCH`, this session |
| `zcl_abapgit_persistence_ortec` is **not** `zcl_abapgit_ortec_*`-prefixed. To stay strictly inside `FDT0-INV-04`'s literal object list, this design does **not** touch it — the new feature flag is session-scoped only (`CLASS-DATA` on `ZCL_ABAPGIT_ORTEC_GIT_SWITCH`, no persistence), not a persisted per-repo setting like `use_serial_batch` | OWNER_DECISION (design choice, this session, to satisfy `FDT0-INV-04` literally) | Cross-checked against `ZCL_ABAPGIT_ORTEC_GIT_SWITCH` source, this session |
| `ZCL_ABAPGIT_ORTEC_SER_ORCH.clas.abap` currently has an **unactivated draft** differing from the active version only in whitespace realignment (parameter-list column alignment in `dispatch_batch`'s `CALL FUNCTION`, and two comment-alignment tweaks in `mark_queued_failures`/`discard_run_state`) — zero semantic difference, confirmed via `action=diff` | CONFIRMED | Live `SAPRead action=diff` (active -> inactive), this session |
| BRF+ cross-application object references (one application's graph embedding another application's owned objects) were not found in the code read this session and are assumed not to exist for `export_xml_application`'s purposes | ASSUMPTION | Not exhaustively verified — flagged as a stop condition in §11 |
| Whether `fdt_admn_0000s` is client-dependent was not confirmed from the rendered DDL (no explicit client-handling annotation was visible in the tool output) | UNVERIFIED | Flagged as an `AC-07` acceptance-test item, not a blocker |
| `zcl_abapgit_hash` has no `sha1`/`if_data`/`iv_hash_algo` overload. The correct method is `sha1_string( iv_data TYPE string ) RETURNING VALUE(rv_sha1) RAISING zcx_abapgit_exception`, which internally calls `cl_abap_message_digest=>calculate_hash_for_char( if_algorithm = 'SHA1' ... )` and returns **lower-case** hex (`TRANSLATE rv_sha1 TO LOWER CASE.`) | CONFIRMED | Live `SAPRead(method="sha1_string")` on `zcl_abapgit_hash`, this session (closes AR-1-003) |
| `CL_FDT_ADMIN_DATA` (`SFDT_CORE`) is the **single, non-subclassed** administrative-data/persistence class for every FDT object type (`GC_OBJECT_TYPE_APPLICATION='AP'`, `..._EXPRESSION='EX'`, `..._DBRULE_TEMPLATE='DT'` = decision table, `..._DATA_OBJECT='DO'`, `..._RULESET='RS'`, etc., per `IF_FDT_CONSTANTS`) — dispatch is by `mv_object_type` inside shared methods (e.g. `update_basic`'s `CASE mv_object_type`), not by per-type subclassing (`SAPNavigate action=hierarchy` on this class returns zero subclasses) | CONFIRMED | Live `SAPRead(method="*")` + `SAPNavigate(action="hierarchy")` on `CL_FDT_ADMIN_DATA`, live grep on `IF_FDT_CONSTANTS`, this session |
| `CL_FDT_ADMIN_DATA=>IF_FDT_TRANSACTION~SAVE` (the one save implementation shared by every object type, per the row above) is a no-op unless `has_unsaved_changes( ) = abap_true`; whenever it does run, it unconditionally sets `change_timestamp` (`GET TIME STAMP FIELD` / `trunc( gv_timestamp )`) on that object's own admin row (`mv_id`) before calling `save_buffer_db( )` + `update_basic( )`. Its sibling `NOTIFY_CHANGE` (invoked by content-changing setters prior to save) also unconditionally bumps `change_timestamp` and creates/increments a new `ts_version` entry (`ls_version-version = <ls_version>-version + 1`) on the same own-id row whenever the object is not in a pure deleted/obsolete-flag-only state | CONFIRMED | Live `SAPRead(method="if_fdt_transaction~save")` and `SAPRead(method="notify_change")` full bodies on `CL_FDT_ADMIN_DATA`, this session (closes AR-1-002 — see §6 for the full argument) |
| Decision tables are their own first-class FDT admin object (`object_type = 'DT'`, `GC_OBJECT_TYPE_DBRULE_TEMPLATE`), i.e. they get their own `fdt_admn_0000s` row (own `id`) exactly like a rule or expression, not a sub-row hanging off the application with no admin identity of its own | CONFIRMED | Live grep on `IF_FDT_CONSTANTS`, this session |

## 2. Cardinality / complexity / performance envelope (mandatory)

- **Expected production cardinality**: this cache's own row count is bounded by the number of distinct BRF+ *applications* ever exported through the ORTEC serializer in one client — a hard system ceiling (307 total `AP` rows system-wide on IT8 today), realistically single digits to low tens per abapGit repository. This is **not** a repository-object-count-scaling mechanism like `OBJ_PERF_FINAL` — a 1,000,000-object repository still only contains however many `FDT0` TADIR rows exist, which is bounded by the same system-wide BRF+ application ceiling, not by repository size.
- **SQL-call complexity per FDT0 object per serialize call**:
  - Miss path: `resolve_application_id` (1 `SELECT SINGLE`, by `name`) + `compute_signature` (1 `SELECT` filtered by `application_id`, cardinality = that one application's own graph, measured up to 5179 rows on IT8) + `try_read` (1 `SELECT SINGLE`, miss) + the existing, unmodified `zcl_abapgit_objects=>serialize` call (unchanged cost) + `store` (1 `MODIFY`, 1 row). Net overhead vs. today: 3 lightweight statements plus 1 upsert, all against a single application's own graph — no cross-application join, no full-table scan.
  - Hit path: same `resolve_application_id` + `compute_signature` + `try_read` (hit) — and the ~150–200 s `export_xml_application`/DOM-parse/`filter_xml_serialize` chain is skipped entirely.
- **HTTP-call complexity**: none. BRF+ export is an in-process ABAP call, not HTTP; a hit removes the heaviest in-process step, not any network call.
- **Row/byte batch policy**: `compute_signature`'s SELECT is a single unbounded (no `UP TO n ROWS`) SELECT scoped to one `application_id` — bounded by that application's own real graph size, never multiplied across applications. A defensive ceiling (`c_max_signature_rows = 200000`) causes the cache to be skipped (never blocks the real export) if a data anomaly ever returns an implausibly large row set.
- **Oversized-object behavior**: `c_max_cache_payload_bytes = 52428800` (50 MB) caps the `EXPORT`ed `ty_serialization` buffer; a payload above this cap is never written to the cache table (the real, correctly-serialized result is still returned to the caller unchanged) — this is the direct mitigation for `FDT0-INV-05`/HTTP_NO_MEMORY-adjacent risk.
- **Cache scope**: per SAP client (`MANDT` key), keyed by `(application_id, signature)` — **not** per repository, per user, or per session. `export_xml_application` takes no repository/devclass input, so the same `(application_id, signature)` always serializes to the same bytes regardless of which repository or run triggered it; sharing the cache across repositories/runs is a deliberate, safe amplification of the benefit, not a coincidence.
- **Transaction owner**: the calling process (the aRFC batch worker `Z_ABAPGIT_ORTEC_SER_BATCH`, or the ORCH dialog process running `route_to_sequential_fallback`) owns its own single-row `MODIFY`; no cross-process two-phase commit; matches the existing, already-production-proven `ZCL_ABAPGIT_ORTEC_OBJ_STORE=>store_object` idiom (plain `MODIFY`, no explicit `COMMIT WORK`, relies on the same natural aRFC/dialog commit boundary that already makes `ZAOG_OBJ_STORE` writes durable in production today).
- **Peak memory model**: the cache wrapper never holds more data concurrently than the existing successful `serialize()` path already holds — one transient `EXPORT`-buffer copy of the just-produced `ty_serialization`, size-checked against `c_max_cache_payload_bytes` and released immediately after the `MODIFY` (or after being discarded, if oversized).
- **1,000 / 40,000 / 1,000,000 stored objects**: not a meaningful axis for this cache (see cardinality above) — the design's cost/benefit is governed entirely by "how many distinct BRF+ applications does this repository's package scope contain", not by total repository object count.
- **Medium/large acceptance scenario**: (a) a repository with 2 unchanged BRF+ applications matching `EVIDENCE`'s exact IT8 measurement (`276078 ms` combined on a cold run) — a second, unmodified re-run must collapse that contribution to low hundreds of ms with byte-identical output; (b) one BRF+ application modified between runs (add/update/delete one nested rule/decision-table/data-object row) — must show a genuinely different signature and a full real re-export, never a false hit, confirmed via a live IT8 dry run.

## 3. DDIC design (`FDT0-AC-01`)

```text
FILE_OR_OBJECT=ZAOG_FDT_CACHE (new transparent table, 14 chars)
METHOD_OR_DDIC=DDIC table definition
ANCHOR=none (new object) — pattern-mirror source: ZAOG_OBJ_STORE
ACTION=insert
CHANGE=
  @EndUserText.label : 'ORTEC Git: FDT0 (BRF+) Serialization Cache'
  @AbapCatalog.enhancement.category : #NOT_EXTENSIBLE
  @AbapCatalog.tableCategory : #TRANSPARENT
  @AbapCatalog.deliveryClass : #L
  @AbapCatalog.dataMaintenance : #RESTRICTED
  define table zaog_fdt_cache {
    key client         : mandt not null;
    key application_id : abap.char(32) not null;
    key signature       : abap.char(40) not null;
    obj_name           : abap.char(40);
    payload            : abap.rawstring(0);
    payload_size       : abap.int4;
    created_at         : tzntstmpl;
    last_used_at       : tzntstmpl;
  }
INVARIANTS=FDT0-AC-01, FDT0-INV-05, FDT0-INV-08
SQL_SHAPE=NONE (DDL only)
ERROR_ROLLBACK_FALLBACK=NONE (new object, nothing to roll back)
TESTS=AC-07 T1 (activation), AC-07 T2 (field list matches this spec via DD03L)
VALIDATION=SAPActivate on the new table; SAPDiagnose action=syntax clean
STOP_IF=activation reports the delivery-class/data-maintenance combination is
  rejected by the system's DDIC checks (would require an owner decision to
  relax data-maintenance, not a silent fallback to #ALLOWED)
```

Field notes:

- `client`/`application_id`/`signature` form the full key. No `repo_key`
  column — §2 "Cache scope" proves `export_xml_application`'s output is a
  pure function of `(application_id, signature)`, independent of which
  repository/run triggered it, so a `repo_key` column would only fragment
  the cache without adding correctness.
- `signature` is `char(40)` to match every other SHA1-hex column already
  in the `ZAOG_*` family (`obj_sha1`, `commit_sha1`, `blob_sha1`,
  `path_hash`, `context_hash` in `ZAOG_OBJ_STORE`/`ZAOG_OBJ_INDEX`).
- `application_id` is `char(32)` to match the observed `fdt_admn_0000s
  -application_id` value shape (32 hex chars, e.g.
  `A0DEC84CFD7ED61EE10000000A428724`).
- `obj_name` is denormalized (not part of the key) purely for
  human-readable cache inspection; never used for lookup (lookup is
  always by `application_id` + `signature`).
- `payload` stores the **exact same wire format** already produced by
  `EXPORT data = ls_serialization TO DATA BUFFER` in
  `Z_ABAPGIT_ORTEC_SER_BATCH` today — i.e. a full `ty_serialization`
  (`item` + `files`), not just the raw FDT0 XML. This is what makes the
  parity proof in §6 trivial by construction.
- No `status` column: unlike `ZAOG_OBJ_STORE` (which models git
  add/delete provenance), a cache row here is binary — present and valid,
  or absent. Eviction (§8) is a hard `DELETE`, never a soft flag.

## 4. Class/method design (`FDT0-AC-02`)

New class name length check: `ZCL_ABAPGIT_ORTEC_FDT0_CACHE` = 28
characters (limit 30). New table name length check: `ZAOG_FDT_CACHE` = 14
characters (limit 14 for `TABL/DT`).

```text
FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_FDT0_CACHE (new class, CREATE PRIVATE, PUBLIC FINAL)
METHOD_OR_DDIC=PUBLIC CLASS-METHODS serialize
ANCHOR=none (new class) — signature mirrors the existing call shape at both
  interception anchors in §5
ACTION=insert
CHANGE=
  "! Drop-in replacement for zcl_abapgit_objects=>serialize at both ORTEC
  "! call sites. Transparent pass-through for every object type except
  "! FDT0, and for FDT0 whenever the cache feature flag is off — in both
  "! cases this method's own behavior is byte-identical to calling
  "! zcl_abapgit_objects=>serialize directly (FDT0-INV-02 fallback).
  CLASS-METHODS serialize
    IMPORTING
      !is_item          TYPE zif_abapgit_definitions=>ty_item
      !io_i18n_params    TYPE REF TO zif_abapgit_i18n_params
    RETURNING
      VALUE(rs_serialization) TYPE zif_abapgit_objects=>ty_serialization
    RAISING
      zcx_abapgit_exception.
INVARIANTS=FDT0-INV-01, FDT0-INV-02, FDT0-INV-04
SQL_SHAPE=NONE (dispatches to the private helpers below)
ERROR_ROLLBACK_FALLBACK=any exception from the real
  zcl_abapgit_objects=>serialize( ) call is re-raised unchanged, never
  swallowed; any exception/failure inside the cache read/write helpers
  themselves is caught internally and treated as a cache miss/no-op,
  never propagated
TESTS=UT-01..UT-06 (see AC-07)
VALIDATION=object-type/flag pass-through equivalence test (UT-01)
STOP_IF=none — this is the safe outer shell
```

```text
FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_FDT0_CACHE
METHOD_OR_DDIC=PRIVATE CLASS-METHODS resolve_application_id
ANCHOR=none (new method)
ACTION=insert
CHANGE=
  "! Mirrors ZCL_ABAPGIT_OBJECT_FDT0's own private GET_APPLICATION_ID
  "! logic exactly (that method cannot be called — it is PRIVATE on a
  "! standard, non-ORTEC class, and FDT0-INV-04 forbids editing that
  "! class to expose it).
  CLASS-METHODS resolve_application_id
    IMPORTING
      !iv_obj_name TYPE sobj_name
    RETURNING
      VALUE(rv_application_id) TYPE fdt_admn_0000s-application_id.
  " Body:
  SELECT SINGLE application_id FROM fdt_admn_0000s INTO rv_application_id
    WHERE object_type = 'AP'
    AND name = iv_obj_name.
INVARIANTS=FDT0-INV-02
SQL_SHAPE=SELECT SINGLE application_id FROM fdt_admn_0000s WHERE object_type = 'AP' AND name = @iv_obj_name INTO @rv_application_id
ERROR_ROLLBACK_FALLBACK=sy-subrc <> 0 leaves rv_application_id initial;
  caller (SERIALIZE) treats an initial application_id as "cache not
  usable for this object", delegates to the real serializer, never raises
TESTS=UT-02 (unknown obj_name -> initial result -> pass-through)
VALIDATION=CL_OSQL_TEST_ENVIRONMENT double on FDT_ADMN_0000S (see AC-07)
STOP_IF=none
```

```text
FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_FDT0_CACHE
METHOD_OR_DDIC=PRIVATE CLASS-METHODS compute_signature
ANCHOR=none (new method)
ACTION=insert
CHANGE=
  "! Full-graph signature — see FDT0-AC-03 proof (design §6) for why this
  "! is safe against adds/updates/deletes/activation-state changes
  "! anywhere in the application's owned object graph, including
  "! decision-table/rule/expression content-only edits (AR-1-002 closure).
  CLASS-METHODS compute_signature
    IMPORTING
      !iv_application_id TYPE fdt_admn_0000s-application_id
    RETURNING
      VALUE(rv_signature) TYPE char40.
  " Body (decision-free pseudocode):
  DATA lt_rows TYPE STANDARD TABLE OF ty_sig_row WITH EMPTY KEY.
  " ty_sig_row: id, object_type, version, ch_timestamp, deleted,
  "             tv_state, tv_timestamp, obsolete (local TYPES, this class only)
  SELECT id, object_type, version, ch_timestamp, deleted,
         tv_state, tv_timestamp, obsolete
    FROM fdt_admn_0000s
    WHERE application_id = @iv_application_id
    ORDER BY id
    INTO TABLE @lt_rows.
  IF lt_rows IS INITIAL OR lines( lt_rows ) > c_max_signature_rows.
    RETURN. " rv_signature stays initial -> caller treats as cache-unusable
  ENDIF.
  DATA lv_concat TYPE string.
  LOOP AT lt_rows INTO DATA(ls_row).
    lv_concat = lv_concat &&
      |{ ls_row-id };{ ls_row-object_type };{ ls_row-version };| &&
      |{ ls_row-ch_timestamp };{ ls_row-deleted };{ ls_row-tv_state };| &&
      |{ ls_row-tv_timestamp };{ ls_row-obsolete }\n|.
  ENDLOOP.
  TRY.
      " AR-1-003 fix: zcl_abapgit_hash has no sha1(if_data=/iv_hash_algo=)
      " overload (verified live this session, §1 evidence table) - the
      " correct, existing call is sha1_string, which takes the STRING
      " directly (no xstring conversion needed) and RAISES
      " zcx_abapgit_exception, so it must be wrapped here to preserve
      " this method's own no-RAISING signature and FDT0-INV-02's
      " "never propagate a cache helper failure" contract.
      rv_signature = to_upper( zcl_abapgit_hash=>sha1_string( lv_concat ) ).
    CATCH zcx_abapgit_exception.
      CLEAR rv_signature. " cache-unusable -> caller delegates to real serializer
  ENDTRY.
INVARIANTS=FDT0-INV-03, FDT0-INV-07
SQL_SHAPE=SELECT id, object_type, version, ch_timestamp, deleted, tv_state, tv_timestamp, obsolete FROM fdt_admn_0000s WHERE application_id = @iv_application_id ORDER BY id INTO TABLE @lt_rows
ERROR_ROLLBACK_FALLBACK=empty/oversized result -> initial rv_signature ->
  caller treats as cache-unusable, delegates to real serializer; a DB
  error propagates as a hard exception only if the SELECT itself raises
  one (not expected for a simple SELECT — no explicit CATCH needed beyond
  what ABAP SQL already does); a sha1_string failure (extremely unlikely -
  only raised on a genuine cx_abap_message_digest from the kernel digest
  API) is caught and treated identically to an empty/oversized result
TESTS=UT-03 (two different graphs produce two different signatures),
  UT-04 (adding/removing/updating one nested row changes the signature),
  UT-05 (row-count ceiling triggers cache-unusable, not an exception),
  UT-15 (compute_signature calls the real sha1_string overload and
  compiles - closes AR-1-003's buildability finding)
VALIDATION=live IT8 SAPQuery diff before/after touching one nested BRF+
  object (see AC-07 IT-01, now a mandatory pre-implementation acceptance
  test per AR-1-002 closure, not a design blocker — the underlying
  mechanism is proven at the CL_FDT_ADMIN_DATA source-code level, §6)
STOP_IF=none (the zcl_abapgit_hash API stop condition from Cycle 0 is
  resolved — see §1 evidence table and §17 finding AR-1-003)
```

```text
FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_FDT0_CACHE
METHOD_OR_DDIC=PRIVATE CLASS-METHODS try_read
ANCHOR=none (new method)
ACTION=insert
CHANGE=
  CLASS-METHODS try_read
    IMPORTING
      !iv_application_id TYPE fdt_admn_0000s-application_id
      !iv_signature      TYPE char40
    EXPORTING
      !es_serialization  TYPE zif_abapgit_objects=>ty_serialization
    RETURNING
      VALUE(rv_found)    TYPE abap_bool.
  " Body (decision-free pseudocode):
  DATA lv_payload TYPE zaog_fdt_cache-payload.
  SELECT SINGLE payload FROM zaog_fdt_cache INTO @lv_payload
    WHERE client = sy-mandt
    AND application_id = @iv_application_id
    AND signature = @iv_signature.
  IF sy-subrc <> 0.
    RETURN. " rv_found stays FALSE -> real miss
  ENDIF.
  TRY.
      IMPORT data = es_serialization FROM DATA BUFFER lv_payload.
    CATCH cx_sy_import_format_error cx_sy_import_mismatch_error
          cx_sy_compression_error cx_sy_conversion_codepage.
      " Corrupt row (FDT0-AC-04): self-heal by deleting it so it never
      " fails the same way again, then report a miss.
      DELETE FROM zaog_fdt_cache WHERE client = sy-mandt
        AND application_id = iv_application_id AND signature = iv_signature.
      RETURN.
  ENDTRY.
  IF es_serialization-files IS INITIAL.
    " Same "zero files is never a valid success" rule as
    " ZCL_ABAPGIT_ORTEC_SER_ORCH=>merge_into_mt_files/route_to_sequential_
    " fallback — a stored empty result is corrupt-equivalent, not a hit.
    DELETE FROM zaog_fdt_cache WHERE client = sy-mandt
      AND application_id = iv_application_id AND signature = iv_signature.
    RETURN.
  ENDIF.
  rv_found = abap_true.
  UPDATE zaog_fdt_cache SET last_used_at = @( get_timestamp( ) )
    WHERE client = sy-mandt AND application_id = @iv_application_id
    AND signature = @iv_signature. " best-effort, ignore sy-subrc
INVARIANTS=FDT0-INV-01, FDT0-INV-02, FDT0-INV-06
SQL_SHAPE=SELECT SINGLE payload FROM zaog_fdt_cache WHERE client = @sy-mandt AND application_id = @iv_application_id AND signature = @iv_signature INTO @lv_payload
ERROR_ROLLBACK_FALLBACK=any corrupt/empty stored payload is deleted and
  reported as a miss, never surfaced as an error to the caller
TESTS=UT-06 (hit returns identical files/item to a direct serialize call),
  UT-07 (corrupt payload -> self-heal delete + miss), UT-08 (stored
  zero-file payload -> self-heal delete + miss)
VALIDATION=byte-for-byte diff of cached vs. freshly-serialized files in a
  controlled IT8 test application (AC-07 IT-02)
STOP_IF=none
```

```text
FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_FDT0_CACHE
METHOD_OR_DDIC=PRIVATE CLASS-METHODS store
ANCHOR=none (new method)
ACTION=insert
CHANGE=
  CLASS-METHODS store
    IMPORTING
      !iv_application_id TYPE fdt_admn_0000s-application_id
      !iv_signature      TYPE char40
      !iv_obj_name       TYPE sobj_name
      !is_serialization  TYPE zif_abapgit_objects=>ty_serialization.
  " Body (decision-free pseudocode):
  DATA lv_payload TYPE zaog_fdt_cache-payload.
  EXPORT data = is_serialization TO DATA BUFFER lv_payload.
  IF xstrlen( lv_payload ) > c_max_cache_payload_bytes.
    RETURN. " FDT0-INV-05: never write an oversized row; real result
             " already returned to the caller regardless of this RETURN
  ENDIF.
  DATA ls_row TYPE zaog_fdt_cache.
  ls_row-client         = sy-mandt.
  ls_row-application_id = iv_application_id.
  ls_row-signature      = iv_signature.
  ls_row-obj_name       = iv_obj_name.
  ls_row-payload        = lv_payload.
  ls_row-payload_size   = xstrlen( lv_payload ).
  ls_row-created_at     = get_timestamp( ).
  ls_row-last_used_at   = ls_row-created_at.
  MODIFY zaog_fdt_cache FROM ls_row. " deliberately ignore sy-subrc —
    " a failed cache write must never turn an already-successful
    " serialize() result into a reported failure (FDT0-INV-02/06);
    " this intentionally diverges from ZCL_ABAPGIT_ORTEC_OBJ_STORE=>
    " store_object, which raises on MODIFY failure because a failed
    " *git object* store IS fatal there — a failed *cache* write here is
    " not, by design.
INVARIANTS=FDT0-INV-02, FDT0-INV-05, FDT0-INV-06, FDT0-INV-07
SQL_SHAPE=MODIFY zaog_fdt_cache FROM @ls_row
ERROR_ROLLBACK_FALLBACK=MODIFY failure is swallowed, never raised; the
  caller's already-successful serialization result is unaffected. Content
  addressing (§6 proof) means two concurrent writers for the same key
  always write byte-identical payloads, so an overwrite race is harmless
  (last writer wins, with identical content)
TESTS=UT-09 (payload above cap -> no row written, real result still
  returned), UT-10 (two sequential stores for the same key -> row count
  stays 1, content unchanged)
VALIDATION=AC-07 IT-03 (concurrent-writer simulation, see §7)
STOP_IF=none
```

```text
FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_FDT0_CACHE
METHOD_OR_DDIC=PUBLIC CONSTANTS c_max_cache_payload_bytes, c_max_signature_rows
ANCHOR=none (new class)
ACTION=insert
CHANGE=
  CONSTANTS c_max_cache_payload_bytes TYPE i VALUE 52428800. " 50 MB
  CONSTANTS c_max_signature_rows     TYPE i VALUE 200000.
INVARIANTS=FDT0-INV-05
SQL_SHAPE=NONE
ERROR_ROLLBACK_FALLBACK=NONE
TESTS=UT-05, UT-09
VALIDATION=NONE beyond unit tests
STOP_IF=none
```

```text
FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_GIT_SWITCH
METHOD_OR_DDIC=PRIVATE SECTION CLASS-DATA mv_fdt0_cache_active; PUBLIC
  CLASS-METHODS is_fdt0_cache_active, set_fdt0_cache_active
ANCHOR=existing block:
  CLASS-DATA mv_serial_batch_active TYPE abap_bool VALUE abap_false.
  (private section, end of the CLASS-DATA list) and the
  is_serial_batch_active/set_serial_batch_active method pair (public
  section)
ACTION=insert (after the anchor block, same section)
CHANGE=
  " private section addition:
  "! Session-scoped only (FDT0-INV-04: no persistence-class change for
  "! this feature — see design §1 evidence table for why). Defaults OFF
  "! so standard/existing behavior is unchanged unless a caller explicitly
  "! opts in for the duration of its own run, mirroring
  "! MV_SERIAL_PREFETCH_ACTIVE's own lifecycle discipline.
  CLASS-DATA mv_fdt0_cache_active TYPE abap_bool VALUE abap_false.

  " public section addition:
  "! Check if the FDT0 (BRF+) serialization result cache is enabled in
  "! this internal session.
  CLASS-METHODS is_fdt0_cache_active
    RETURNING VALUE(rv_active) TYPE abap_bool.
  "! Enable or disable the FDT0 (BRF+) serialization result cache in this
  "! internal session. Production caller is exclusively
  "! ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE, mirroring the existing
  "! SET_SERIAL_PREFETCH_ACTIVE on-entry/off-on-every-exit lifecycle.
  CLASS-METHODS set_fdt0_cache_active
    IMPORTING iv_active TYPE abap_bool.

  " implementation section addition:
  METHOD is_fdt0_cache_active.
    rv_active = mv_fdt0_cache_active.
  ENDMETHOD.
  METHOD set_fdt0_cache_active.
    mv_fdt0_cache_active = iv_active.
  ENDMETHOD.
INVARIANTS=FDT0-INV-04, binding invariant "standard behavior unchanged
  when any ORTEC feature flag is disabled" (.memory/state.md)
SQL_SHAPE=NONE
ERROR_ROLLBACK_FALLBACK=NONE
TESTS=UT-01 (flag OFF -> pass-through proven), UT-11 (flag ON/OFF toggling)
VALIDATION=SAPDiagnose action=syntax on ZCL_ABAPGIT_ORTEC_GIT_SWITCH
STOP_IF=none
```

```text
FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_SER_ORCH
METHOD_OR_DDIC=PUBLIC METHODS serialize (main entry point)
ANCHOR=existing line, near the top, right after the existing prefetch-window
  bracket comment:
    zcl_abapgit_ortec_git_switch=>set_serial_prefetch_active( abap_true ).
    lv_use_ortec_prefetch = abap_true.
  and the three existing "turn it back off" call sites: (a) inside the
  `CATCH cx_uuid_error.` block, right before
  `zcl_abapgit_ortec_git_switch=>set_serial_prefetch_active( abap_false ).`;
  (b) on the normal-completion path, right before the identical call
  after `purge_run_state( lv_run_id ).`; (c) inside the
  `CATCH zcx_abapgit_exception INTO DATA(lx_run_failure).` block, right
  before the identical call after `discard_run_state( lv_run_id ).`
ACTION=insert (one new line immediately after each of the four existing
  set_serial_prefetch_active bracket lines above, both the single ON call
  and all three OFF calls)
CHANGE=
  " AR-1-001 fix (Cycle 1 BLOCKER closure): mv_fdt0_cache_active is
  " CLASS-DATA on the SAME dialog-process class as this method - setting
  " it here, bracketed exactly like the existing MV_SERIAL_PREFETCH_
  " ACTIVE lifecycle, makes the flag visible to BOTH interception anchors
  " that run in this process: ROUTE_TO_SEQUENTIAL_FALLBACK (called later
  " in this same method, both for forced_seq objects and as the
  " batch/merge failure recovery path) and, via a new IMPORTING
  " parameter, the aRFC batch worker itself (see the
  " Z_ABAPGIT_ORTEC_SER_BATCH and dispatch_batch edits below - CLASS-DATA
  " alone cannot cross the RFC boundary, per the already-fixed SER-SLICE-5
  " precedent for MV_SERIAL_PREFETCH_ACTIVE).
  " (1) immediately after "lv_use_ortec_prefetch = abap_true." near the top:
  zcl_abapgit_ortec_git_switch=>set_fdt0_cache_active( abap_true ).
  " (2) immediately after each of the three existing
  "     set_serial_prefetch_active( abap_false ) calls (cx_uuid_error
  "     catch, normal completion, zcx_abapgit_exception catch):
  zcl_abapgit_ortec_git_switch=>set_fdt0_cache_active( abap_false ).
INVARIANTS=FDT0-INV-01, FDT0-INV-02, FDT0-INV-04, binding invariant
  "standard behavior unchanged when any ORTEC feature flag is disabled"
SQL_SHAPE=NONE
ERROR_ROLLBACK_FALLBACK=the flag is always reset to abap_false on every
  exit path (success, cx_uuid_error, zcx_abapgit_exception) - identical
  three-exit-point symmetry already proven correct for
  MV_SERIAL_PREFETCH_ACTIVE; a stuck-ON flag is not possible via any path
  that does not already reset the existing prefetch flag the same way
TESTS=UT-16 (serialize() sets is_fdt0_cache_active()=TRUE for the
  duration of the run and FALSE again after each of the three exit
  paths), UT-12 (route_to_sequential_fallback with an FDT0 key still
  drives mark_object_success/mark_object_failures identically, now with
  the flag genuinely ON during the call)
VALIDATION=run_unit_tests on ZCL_ABAPGIT_ORTEC_SER_ORCH after the change;
  live IT8 AC-07 IT-04 full run with the flag path exercised end-to-end
STOP_IF=the unactivated whitespace-only draft on this class (§1) must be
  reconciled (activated or discarded) before this edit and the
  route_to_sequential_fallback anchor substitution below are applied, so
  exact anchor text matches
```

```text
FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_SER_ORCH
METHOD_OR_DDIC=PRIVATE METHODS dispatch_batch
ANCHOR=existing CALL FUNCTION 'Z_ABAPGIT_ORTEC_SER_BATCH' EXPORTING list,
  specifically the existing line:
          iv_input_row_count      = lines( it_object_keys )
          iv_input_version        = 1
ACTION=insert (one new EXPORTING line, before iv_input_row_count for
  readability, no signature change to dispatch_batch itself needed)
CHANGE=
          " AR-1-001 fix: dispatch_batch runs in the SAME ORCH dialog
          " session as serialize() (which already brackets
          " mv_fdt0_cache_active via GIT_SWITCH class-data above), so
          " reading the flag directly here - rather than threading it
          " through the run context or dispatch_batch's own signature -
          " is sufficient and follows the existing minimal-footprint
          " precedent (no unnecessary signature/visibility changes).
          iv_fdt0_cache_active    = zcl_abapgit_ortec_git_switch=>is_fdt0_cache_active( )
          iv_input_row_count      = lines( it_object_keys )
          iv_input_version        = 1
INVARIANTS=FDT0-INV-01, FDT0-INV-02, FDT0-INV-04
SQL_SHAPE=NONE
ERROR_ROLLBACK_FALLBACK=unchanged - if the flag is OFF (default), the FM
  receives abap_false and behaves exactly as today (byte-identical
  pass-through, no cache read/write)
TESTS=UT-14 (worker-loop result row for an FDT0 object matches a direct
  standard-path serialize on both a cold and a warm cache, now via the
  real dispatch_batch/CALL FUNCTION path, not only route_to_sequential_
  fallback)
VALIDATION=IT8 fresh worker dispatch via ZCL_ABAPGIT_ORTEC_SER_ORCH=>
  SERIALIZE with the flag on, comparing output files against the flag-off
  baseline (AC-07 IT-04)
STOP_IF=none
```

```text
FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_SER_ORCH
METHOD_OR_DDIC=PRIVATE CLASS-METHODS route_to_sequential_fallback
ANCHOR=existing line inside the LOOP AT it_object_keys:
      TRY.
          DATA(ls_serialization) = zcl_abapgit_objects=>serialize(
            is_item        = ls_item
            io_i18n_params = zcl_abapgit_i18n_params=>new( is_params = ls_i18n_params ) ).
ACTION=replace
CHANGE=
      TRY.
          DATA(ls_serialization) = zcl_abapgit_ortec_fdt0_cache=>serialize(
            is_item        = ls_item
            io_i18n_params = zcl_abapgit_i18n_params=>new( is_params = ls_i18n_params ) ).
INVARIANTS=FDT0-INV-01, FDT0-INV-02, FDT0-INV-04
SQL_SHAPE=NONE (delegates)
ERROR_ROLLBACK_FALLBACK=unchanged — the surrounding CATCH
  zcx_abapgit_exception block already handles any failure identically to
  today, since the new wrapper re-raises unchanged
TESTS=UT-12 (route_to_sequential_fallback with an FDT0 key still produces
  the same mark_object_success/mark_object_failures behavior as before)
VALIDATION=run_unit_tests on ZCL_ABAPGIT_ORTEC_SER_ORCH after the change
STOP_IF=the exact line/whitespace differs from what is quoted above at
  implementation time (the ANCHOR was read from the ACTIVE version; the
  unactivated whitespace-only draft noted in §1 must be reconciled or
  activated first so ANCHOR matching does not fail)
```

```text
FILE_OR_OBJECT=Z_ABAPGIT_ORTEC_SER_BATCH (function module)
METHOD_OR_DDIC=FUNCTION signature (new IMPORTING parameter) + FUNCTION
  body, main per-object LOOP
ANCHOR=(a) existing signature, end of the IMPORTING list:
    VALUE(iv_input_row_count) TYPE i
    VALUE(iv_input_version) TYPE i DEFAULT 1
  (b) existing line 172 (confirmed live this session):
  zcl_abapgit_ortec_git_switch=>set_serial_prefetch_active( abap_true ).
  immediately before the DOKIL longtext-index preload comment/block
  (c) existing lines 285-287 (confirmed live this session):
        ls_serialization = zcl_abapgit_objects=>serialize(
          is_item        = ls_item
          io_i18n_params = zcl_abapgit_i18n_params=>new( is_params = ls_i18n_params ) ).
  (d) existing line 311 (confirmed live this session), immediately after
  ENDLOOP and before ev_output_row_count is assigned:
  zcl_abapgit_ortec_git_switch=>set_serial_prefetch_active( abap_false ).
ACTION=(a) insert new OPTIONAL IMPORTING parameter; (b)/(d) insert one new
  line immediately after each anchor; (c) replace
CHANGE=
  " (a) new IMPORTING parameter, added after iv_input_version:
    VALUE(iv_fdt0_cache_active) TYPE abap_bool OPTIONAL

  " (b) AR-1-001 fix (Cycle 1 BLOCKER closure): this worker session is a
  " SEPARATE aRFC process from ZCL_ABAPGIT_ORTEC_SER_ORCH - CLASS-DATA
  " does not cross the RFC boundary (identical root cause to the
  " already-fixed SER-SLICE-5 incident for MV_SERIAL_PREFETCH_ACTIVE,
  " confirmed via this FM's own live comment, §1). The activation call
  " must reflect whatever ORCH decided (iv_fdt0_cache_active, sourced
  " from dispatch_batch's is_fdt0_cache_active( ) read), not
  " unconditionally TRUE:
  zcl_abapgit_ortec_git_switch=>set_fdt0_cache_active( iv_fdt0_cache_active ).

  " (c) replace the serialize() call:
        ls_serialization = zcl_abapgit_ortec_fdt0_cache=>serialize(
          is_item        = ls_item
          io_i18n_params = zcl_abapgit_i18n_params=>new( is_params = ls_i18n_params ) ).

  " (d) reset immediately after the loop, mirroring the existing
  " set_serial_prefetch_active( abap_false ) reset at the same spot:
  zcl_abapgit_ortec_git_switch=>set_fdt0_cache_active( abap_false ).
INVARIANTS=FDT0-INV-01, FDT0-INV-02, FDT0-INV-04
SQL_SHAPE=NONE (delegates)
ERROR_ROLLBACK_FALLBACK=unchanged — the surrounding CATCH
  zcx_abapgit_exception block already builds ls_result-rc/msgid/... from
  whatever exception surfaces, identically to today; iv_fdt0_cache_active
  being OPTIONAL and unsupplied defaults to abap_false, so any existing
  caller that has not been updated (there are none outside
  EXACT_SOURCE_SCOPE, but this is defense in depth) gets byte-identical
  pass-through behavior
TESTS=UT-13 (worker-loop result row shape unchanged for a non-FDT0
  object), UT-14 (worker-loop result row for an FDT0 object matches a
  direct standard-path serialize on both a cold and a warm cache), UT-16
  (iv_fdt0_cache_active genuinely activates the flag inside this
  SEPARATE aRFC process, proving the boundary-crossing fix works, not
  only that the class-data setter compiles)
VALIDATION=IT8 fresh worker dispatch via ZCL_ABAPGIT_ORTEC_SER_ORCH=>
  SERIALIZE with the flag on, comparing output files against the flag-off
  baseline (AC-07 IT-04)
STOP_IF=none
```

## 5. Interception anchors (`FDT0-AC-02` summary)

There are exactly two, and only two, places in the entire ORTEC
serialization path where `zcl_abapgit_objects=>serialize` is called for a
`FDT0` object:

1. `Z_ABAPGIT_ORTEC_SER_BATCH`'s per-object `LOOP AT it_tadir` (batch
   worker, aRFC process) — every object dispatched through
   `ZCL_ABAPGIT_ORTEC_SER_ORCH=>dispatch_batch`/`before_dispatch` lands
   here, `FDT0` included (no per-type provider intercepts it earlier in
   this loop — confirmed, §1).
2. `ZCL_ABAPGIT_ORTEC_SER_ORCH=>route_to_sequential_fallback`'s
   `LOOP AT it_object_keys` (same dialog process as `SERIALIZE`) — reached
   for forced-sequential objects, receive failures, mismatched batch
   results, zero-file "successes", and failed merges.

Both call the identical shape
`zcl_abapgit_objects=>serialize( is_item = ... io_i18n_params = ... )`.
Both anchors are replaced with the identical
`zcl_abapgit_ortec_fdt0_cache=>serialize( ... )` call — the new class is
the single shared choke point for both dispatch paths, which is required
for `FDT0-INV-07` (a batch-path write must be visible to a
sequential-fallback-path read and vice versa, since both can legitimately
touch the same application within one run).

**Activation wiring (AR-1-001 closure).** A call-site substitution alone
is not sufficient — `ZCL_ABAPGIT_ORTEC_GIT_SWITCH=>is_fdt0_cache_active( )`
must actually return `abap_true` on both anchors for the substitution to
engage the cache at all. Anchor 2 is covered because
`route_to_sequential_fallback` runs in the same dialog session as
`ZCL_ABAPGIT_ORTEC_SER_ORCH=>serialize`, which now brackets the whole run
with `set_fdt0_cache_active( abap_true )` / `... ( abap_false )` (§4).
Anchor 1 is a *separate aRFC process* — CLASS-DATA cannot cross that
boundary (identical, already-fixed precedent: SER-SLICE-5 /
`MV_SERIAL_PREFETCH_ACTIVE`, confirmed live in `Z_ABAPGIT_ORTEC_SER_BATCH`'s
own comment, §1) — so `dispatch_batch` now reads the flag explicitly
(`iv_fdt0_cache_active = zcl_abapgit_ortec_git_switch=>is_fdt0_cache_active( )`)
and passes it as a new `Z_ABAPGIT_ORTEC_SER_BATCH` IMPORTING parameter,
and the FM body re-activates it locally in its own process before the
loop and resets it after — the exact bracket shape SER-SLICE-5 already
established for `MV_SERIAL_PREFETCH_ACTIVE` (§4).

## 6. Signature algorithm and proof (`FDT0-AC-03`)

**Algorithm.** For a resolved `application_id`:

```text
SELECT id, object_type, version, ch_timestamp, deleted,
       tv_state, tv_timestamp, obsolete
  FROM fdt_admn_0000s
  WHERE application_id = @iv_application_id
  ORDER BY id
  INTO TABLE @lt_rows.
signature = SHA1( concat_ordered( lt_rows, field_separator = ';', row_separator = LF ) )
```

**Why the application's own row is included.** `id = application_id` for
the application's own admin row (confirmed, §1 evidence), so it is
naturally included by the `WHERE application_id = ...` filter alongside
every nested object — one query covers the whole graph, no `UNION`/second
query needed.

**Why ADD cannot create a false hit.** A newly added nested object gets a
brand-new `id`, which appears as an additional row in the `ORDER BY id`
result set. The concatenated string — and therefore the SHA1 — changes
because the row *set*, not just individual field values, is part of the
hash input. A signature computed before the add can never equal one
computed after it (barring a SHA1 collision, out of scope for a
local-cache accelerator).

**Why UPDATE cannot create a false hit — including a content-only edit on
a nested rule/decision-table/expression (AR-1-002 closure).** Cycle 0 of
this design only empirically confirmed a `version`/`ch_timestamp` bump for
the *top-level application row's own* save counter and reasoned by
plausibility that the same held for nested objects. This cycle replaces
that plausibility argument with a source-level architectural proof:
`fdt_admn_0000s` rows are managed exclusively by `CL_FDT_ADMIN_DATA`
(`SFDT_CORE`) — **one shared, non-subclassed class for every FDT object
type**, dispatched internally by `mv_object_type` (confirmed via
`SAPNavigate action=hierarchy` returning zero subclasses, plus
`update_basic`'s own `CASE mv_object_type` handling of `EXPRESSION`/
`DATA_OBJECT` alongside every other type — §1). Decision tables are one
of those types (`object_type = 'DT'`, `GC_OBJECT_TYPE_DBRULE_TEMPLATE`) —
they are first-class FDT admin objects with their own `id`/admin row, not
untracked children of the application. Every save of *any* such object —
rule, decision table, expression, data object, or application — goes
through the one shared `CL_FDT_ADMIN_DATA=>IF_FDT_TRANSACTION~SAVE`,
which is a no-op unless `has_unsaved_changes( ) = abap_true`, and when it
runs, unconditionally sets `change_timestamp` on *that object's own*
`mv_id` row before persisting (`save_buffer_db( )` / `update_basic( )`).
Its sibling `NOTIFY_CHANGE` — invoked by content-changing setters before
save — likewise unconditionally bumps `change_timestamp` and
creates/increments a `ts_version` entry on the same own-id row whenever
the object is not in a pure deleted/obsolete-only state. Because this
mechanism is shared by construction (one class, type-dispatched, not
overridden per type), a content-only edit to a decision table's cell
values or a rule's formula text is, architecturally, indistinguishable
from any other FDT object save as far as `version`/`ch_timestamp` are
concerned — there is no separate, ungoverned content-write path that
could bypass this generic save/versioning layer. This closes the design's
central correctness risk (a false hit from an unproven metadata-follows-
content assumption) with source-code evidence rather than a plausibility
argument. `AC-07 IT-01` (a live edit-and-re-signature check on a real
decision table and a real rule) is a **mandatory, non-waivable stop
condition — see §11**, not merely defense-in-depth prose: this design does
not authorize enabling `set_fdt0_cache_active( abap_true )` in any
production/live-repository run, and implementation/deployment of this
cache cannot proceed to production, until IT-01 has actually been
executed on IT8 for both a decision-table cell-value edit and a rule
formula-only edit, with both runs showing a measured signature change.
The underlying mechanism (`CL_FDT_ADMIN_DATA`'s shared save/versioning
layer) is proven at the source-code level above; IT-01 is the live
confirmation that no undiscovered bypass of that shared layer exists —
until it has run and passed, that confirmation does not exist, and the
cache must remain off in every environment except the isolated test/dev
session used to run IT-01 itself.

**Why DELETE cannot create a false hit.** A logical delete sets
`deleted <> ''` on the affected row — included directly in the hash
input, so the row's contribution to the concatenated string changes even
if nothing else about it does. A later physical delete (`cl_fdt_delete_
handling=>delete_physical_via_job`, seen in `ZCL_ABAPGIT_OBJECT_FDT0
~delete`) removes the row from the result set entirely — same argument as
ADD, in reverse: the row set changes, so the hash changes.

**Why a torn/concurrent read cannot create a false hit (only, at worst, an
extra miss).** Two workers reading `fdt_admn_0000s` for the same
`application_id` at slightly different instants could, in the
extraordinarily rare case of a live concurrent edit to that exact
application during a git export, compute two different signatures for
what is "the same moment" from each worker's own point of view. This
cannot cause a **false hit**, because a false hit requires two different
underlying graph states to hash to the *same* signature and be looked up
against each other — the signature is a deterministic function of the
exact row set observed by one `SELECT`, so a stable, quiescent graph
state always reproduces the same signature every time it is read; a
`torn` mixed-state signature (if it ever occurred) simply corresponds to
no stable state at all, meaning nothing will ever compute that exact
signature again, so the row is a harmless orphan that a future genuinely
stable read never matches (a wasted cache slot, not a wrong answer). This
is the same asymmetry already relied on for the git object store's own
content-addressing safety (see `ZCL_ABAPGIT_ORTEC_OBJ_STORE`'s
duplicate-key handling notes).

**Residual risk (flagged, not blocking).** If a future finding shows a
BRF+ application's exported content can depend on data outside
`fdt_admn_0000s`'s owned-row set (e.g. a genuine cross-application
reference, or a global BRF+ configuration setting that is not itself
tracked as an owned admin row), this signature would miss that
dependency and produce a false hit. This is called out explicitly as
`ASSUMPTION` in §1 and as a non-goal/stop condition in §11 — no evidence
of such a dependency was found this session, but it was not exhaustively
ruled out either.

## 7. Hit / miss / corrupt / concurrent / error semantics (`FDT0-AC-04`)

| Case | Behavior |
|---|---|
| Object type ≠ `FDT0`, or `is_fdt0_cache_active( ) = abap_false` | `serialize()` calls the real `zcl_abapgit_objects=>serialize` directly, no cache read/write at all — byte-identical to today |
| `resolve_application_id` returns initial | Treated as cache-unusable; real serializer called; no cache write attempted (nothing to key on) |
| `compute_signature` returns initial (empty/oversized graph, §2) | Same as above |
| Cache lookup miss (`try_read` returns `rv_found = abap_false`) | Real serializer called; on success, `store()` attempted (best-effort); the real result is returned regardless of whether `store()` succeeds |
| Cache lookup hit, payload imports cleanly and is non-empty | Cached `ty_serialization` returned directly; real serializer is **not** called; `last_used_at` best-effort updated |
| Cache row exists but `IMPORT` raises a format/mismatch/compression/codepage exception | Row is deleted (self-heal); treated as a miss; real serializer called; result re-cached on success |
| Cache row exists but imports to zero files | Row is deleted (self-heal, same reasoning as `ZCL_ABAPGIT_ORTEC_SER_ORCH=>merge_into_mt_files`'s own "zero files is not success" rule); treated as a miss |
| Real `zcl_abapgit_objects=>serialize` raises `zcx_abapgit_exception` (cache miss path) | Exception re-raised unchanged to the caller; `store()` is never reached — `FDT0-INV-06` is satisfied structurally, not by a rollback |
| `store()`'s own `MODIFY` fails (`sy-subrc <> 0`) | Swallowed; the already-successful real result is still returned to the caller; no exception, no log entry required (best-effort only) |
| Two concurrent workers compute the same `(application_id, signature)` key | Both, if they reach `store()`, write byte-identical payloads (content-addressed by construction, §6); last `MODIFY` wins harmlessly; no torn read is possible mid-row because `MODIFY` is a single-row, single-statement operation |
| Two concurrent workers compute two *different* signatures for the same `application_id` (mid-edit torn read, §6) | Each writes its own row under its own key; neither can ever be looked up by the other's key; no mismatch is observable to any reader |

## 8. Lock / LUW / publication / rollback / eviction (`FDT0-AC-06`)

- **Locking**: none. The key already includes the full-graph signature,
  so there is nothing to protect against concurrent writers producing
  divergent content under the same key (§6/§7) — adding an `ENQUEUE` here
  would only add latency without closing any real race.
- **LUW/commit**: each `store()` call is a single `MODIFY` of one row;
  durability follows the natural commit boundary of whichever process
  calls it (aRFC function-module return for the batch worker, or the
  dialog step's own commit for `route_to_sequential_fallback`) — the same
  boundary that already makes `ZCL_ABAPGIT_ORTEC_OBJ_STORE`'s writes
  durable in production (confirmed precedent, §1). No new explicit
  `COMMIT WORK` is introduced.
- **Publication**: a row only ever becomes visible to other readers after
  the full `EXPORT`+size-check+`MODIFY` sequence completes — there is no
  earlier point at which a partial/placeholder row is written, so
  `FDT0-INV-06` holds without a two-phase status column.
- **Rollback**: if the calling process itself rolls back for an unrelated
  reason after `store()` but before its own natural commit, the
  `MODIFY` rolls back with it — this is standard ABAP DB-LUW behavior and
  requires no special handling; a rolled-back cache write simply never
  existed, which is indistinguishable from a miss that was never stored.
- **Eviction / size limits**: `c_max_cache_payload_bytes` (50 MB, §3)
  bounds any single row. Row *count* is not expected to need active
  eviction given the measured system-wide ceiling of 307 applications
  (§1/§2), but as defense in depth, `ZCL_ABAPGIT_ORTEC_CACHE_ADMIN`'s
  existing `get_overview`/`clear_repo` admin pattern is the natural place
  to add an optional, explicitly-owner-triggered "clear FDT0 cache"
  action in a later slice (see §11 non-goals — not built in this slice,
  since the owner's ask was the cache mechanism itself, not admin
  tooling, and `ZCL_ABAPGIT_ORTEC_CACHE_ADMIN` is out of
  `EXACT_SOURCE_SCOPE`).

## 9. Test plan (`FDT0-AC-07`)

**ABAP Unit** (new test class on `ZCL_ABAPGIT_ORTEC_FDT0_CACHE`, using
`CL_OSQL_TEST_ENVIRONMENT` doubles for `FDT_ADMN_0000S` and
`ZAOG_FDT_CACHE` per the already-proven pattern in this codebase, see
user memory notes on `cl_osql_test_environment` usage):

- UT-01: flag OFF -> `serialize()` result identical to calling
  `zcl_abapgit_objects=>serialize` directly (pass-through proof, both for
  `FDT0` and a non-`FDT0` type).
- UT-02: unknown `obj_name` -> `resolve_application_id` returns initial ->
  pass-through, no cache row written.
- UT-03: two different synthetic admin-row sets for the same
  `application_id` -> two different signatures.
- UT-04: adding, updating (`version`/`ch_timestamp` bump), and logically
  deleting (`deleted = 'X'`) one nested row each independently change the
  signature.
- UT-05: a synthetic row set above `c_max_signature_rows` ->
  `compute_signature` returns initial, not an exception.
- UT-06: a cache hit returns `files`/`item` identical to what a direct
  `zcl_abapgit_objects=>serialize` call would have produced for the same
  input (constructed via a stubbed/pre-seeded cache row).
- UT-07 / UT-08: corrupt payload / zero-file payload -> self-heal delete
  + reported miss.
- UT-09: an oversized `is_serialization` -> no row written, real result
  still returned unchanged to the caller.
- UT-10: two sequential `store()` calls for the same key -> exactly one
  row, content unchanged (idempotent upsert).
- UT-11: `is_fdt0_cache_active`/`set_fdt0_cache_active` toggle correctly
  and default to `abap_false`.
- UT-12 (on `ZCL_ABAPGIT_ORTEC_SER_ORCH`): `route_to_sequential_fallback`
  with an `FDT0` key still drives `mark_object_success`/
  `mark_object_failures` identically to before the anchor substitution.
- UT-13 / UT-14 (on `Z_ABAPGIT_ORTEC_SER_BATCH`, via a thin testable
  extraction if the FUGR itself is not directly unit-testable): worker
  result-row shape is unchanged for a non-`FDT0` object; for an `FDT0`
  object, a cold-cache run and a warm-cache run produce the same
  `et_result` row shape (`rc`, `output_file_count`, etc.), differing only
  in `elapsed_ms`.
- UT-15 (on `ZCL_ABAPGIT_ORTEC_FDT0_CACHE`): `compute_signature` calls
  the real `zcl_abapgit_hash=>sha1_string` overload and produces a
  40-char upper-case hex digest; a simulated `cx_abapgit_exception` from
  `sha1_string` is caught and treated as cache-unusable (closes AR-1-003).
- UT-16 (on `ZCL_ABAPGIT_ORTEC_SER_ORCH` and, separately, on
  `Z_ABAPGIT_ORTEC_SER_BATCH`): `is_fdt0_cache_active( )` is genuinely
  `abap_true` for the duration of `serialize()`/`route_to_sequential_
  fallback` and for the duration of the FM's own `LOOP AT it_tadir` when
  `iv_fdt0_cache_active = abap_true` is passed in, and `abap_false`
  again after every exit path on both sides (closes AR-1-001).

**Integration / IT8 fresh-run validation**:

- IT-01: on IT8, pick one real, low-risk BRF+ test application; run
  `compute_signature` before and after (a) a decision-table cell/row
  VALUE-only edit and (b) a rule formula-only edit, and confirm the
  signature actually changes for both. **Status after Cycle 2 (AR-2-001
  closure)**: IT-01 is a literal, non-waivable stop condition — see §11.
  §6 proves the underlying mechanism (`CL_FDT_ADMIN_DATA`'s shared
  save/versioning layer) at the source-code level, but that proof alone
  does not authorize enabling the cache in production: implementation and
  deployment of this cache CANNOT PROCEED TO PRODUCTION if IT-01 fails or
  is not executed. IT-01 must actually run and both variants
  (decision-table cell edit, rule formula edit) must show a measured
  signature change, recorded as a new §1 evidence row, before
  `set_fdt0_cache_active( abap_true )` may be enabled outside the
  isolated test/dev session used to run IT-01 itself.
- IT-02: cold-cache serialize of a real FDT0 object vs. warm-cache
  serialize of the same object -> byte-for-byte identical file content
  and `sha1` per file.
- IT-03: two concurrent aRFC batch dispatches both containing the same
  unchanged FDT0 application (constructed via two overlapping ORCH runs,
  or a manual dual dispatch) -> exactly one cache row for that
  `(application_id, signature)`, no dump, no mismatched content.
- IT-04: full repository serialize with the flag OFF (baseline) vs. ON
  (cold, then warm) — output file set and content must be identical
  across all three runs; only the FDT0 objects' `elapsed_ms` may differ;
  the `276078 ms`/`18.6%` EVIDENCE figure is expected to collapse on the
  warm run.
- IT-05: `run_atc_analysis`/`SAPDiagnose action=atc` on the new class and
  both changed anchors must be clean (or only pre-existing, unrelated
  findings) before this design is considered implementation-ready.

## 10. Parity proof, `filter_xml_serialize`, volatile fields (`FDT0-AC-05`)

The cache stores the **output** of a real, unmodified
`zcl_abapgit_objects=>serialize` call — which itself already invokes
`ZCL_ABAPGIT_OBJECT_FDT0~serialize` -> `filter_xml_serialize` internally,
on every cache-miss path, on both interception anchors (§5). By the time
`ZCL_ABAPGIT_ORTEC_FDT0_CACHE=>store` ever sees a `ty_serialization`
value, every volatile field
(`CreationUser/Timestamp`, `ChangeUser/Timestamp`, `User`, `Timestamp`,
every `Tr*` field, `OversId`, `SoftwareComponent`, `DevelopmentPackage`,
and the root node's `Client/Date/SAPRelease/Server/SourceExportReqID/
SystemID/Time/User` attributes — confirmed exhaustive list, §1) has
already been stripped by the standard object's own filter step. There is
**no separate volatile-field handling on the ORTEC side at all** —
caching happens strictly downstream of filtering, so a cache hit
reproduces exactly the bytes a live call would produce for the same
signature, by construction, not by a second independent filtering
implementation that could drift out of sync with the standard one.

This also means: if a future standard-abapGit upgrade changes what
`filter_xml_serialize` strips, the cache requires **no code change** to
stay correct — it will simply start caching whatever the (updated)
standard filter now produces, on the next real miss for each affected
application.

## 11. Non-goals and stop conditions (`FDT0-AC-08`)

**Non-goals for this slice**:

- No batch-prefetch provider for `FDT0` (unlike DOMA/DTEL/CLAS/INTF/MSAG/
  TABL/PROG/FUGR) — a prefetch provider accelerates the *miss* path by
  avoiding N+1 per-object reads across many objects of the same type;
  `FDT0` objects are few (bounded by 307 system-wide applications) and
  individually extremely expensive (~150–200 s), so the entire
  optimization opportunity is in avoiding re-serialization of *unchanged*
  content, not in batching reads across many BRF+ objects at once. A
  batch-prefetch provider would not address the measured cost driver at
  all.
- No custom FDT XML serializer/deserializer — explicitly forbidden
  (`FORBIDDEN_CHANGES`), and unnecessary: this design never re-implements
  BRF+ export logic, it only caches the standard object's own already-
  correct output.
- No admin/inspection UI extension to `ZCL_ABAPGIT_ORTEC_CACHE_ADMIN` in
  this slice — out of `EXACT_SOURCE_SCOPE`; flagged as a natural follow-up
  in §8, not built here.
- No persisted per-repository enable flag (unlike `use_serial_batch`) —
  intentionally session-scoped only, to keep every change strictly inside
  `zcl_abapgit_ortec_*` classes plus `Z_ABAPGIT_ORTEC_SER_BATCH`
  (`FDT0-INV-04`); `zcl_abapgit_persistence_ortec` is not touched.
- No default-on production enablement — the new flag defaults
  `abap_false`; turning it on for any repository/run is a separate,
  explicit owner action after this design is implemented and validated.

**Stop conditions** (must be resolved before implementation, not silently
worked around):

- **`AC-07 IT-01` (AR-2-001 closure) — literal, non-waivable stop
  condition, not prose.** `set_fdt0_cache_active( abap_true )` MUST NOT
  be enabled in any production or live-repository run — and
  implementation/deployment of this cache CANNOT PROCEED TO PRODUCTION —
  until IT-01 has actually been executed on IT8, on both a
  decision-table cell-value edit and a rule formula-only edit, with both
  runs recording a measured signature change as a new §1 evidence row
  (see §6, §9). This is distinct from every other stop condition below:
  it is not merely "resolve before implementation" but "the cache must
  stay off in every environment except the isolated test/dev session
  used to run IT-01 itself, until IT-01 has passed." §6's source-level
  architectural proof is necessary but not sufficient to close this gate
  on its own.
- Whether `fdt_admn_0000s` is client-dependent was not confirmed from the
  rendered DDL this session (§1, `UNVERIFIED`) — verify via `DD03L`
  before finalizing whether the new cache table's own `client` key is
  purely defensive convention (safe either way) or load-bearing.
- The cross-application-reference assumption in §6's residual risk must
  remain an explicit, visible caveat in any implementation handoff — it
  is not proven false, only not found in the code read this session.
- The unactivated whitespace-only draft on `ZCL_ABAPGIT_ORTEC_SER_ORCH`
  (§1) must be reconciled (activated or discarded) before the
  `route_to_sequential_fallback` anchor substitution and the new
  `serialize()`/`dispatch_batch` activation-wiring edits (§4) are
  applied, so exact anchor text matches.

Resolved this cycle (no longer stop conditions): the exact
`zcl_abapgit_hash` public API (AR-1-003, §1/§4), and the
metadata-follows-content signature-soundness question for nested
content-only edits (AR-1-002, §6) — both now have session-verified
source-code evidence, superseding the Cycle 0 STOP_IF text. `AC-07 IT-01`
is NOT resolved — see the literal stop condition above (AR-2-001
closure, Cycle 2).

## 12. Invariant matrix

| Invariant | Design mechanism | Section |
|---|---|---|
| FDT0-INV-01 (hit = output-parity) | Cache stores the real, already-filtered `ty_serialization` verbatim; hit returns it unchanged | §6, §10 |
| FDT0-INV-02 (miss/error/invalid never degrades) | Every failure mode (unresolved app id, oversized/empty signature graph, corrupt/empty cache row, MODIFY failure, sha1_string exception) falls back to the real serializer or is swallowed without altering its result | §4, §7 |
| FDT0-INV-03 (any graph change invalidates) | Full-graph signature over every admin row sharing `application_id`, including version/timestamp/deleted/activation-state fields; soundness for content-only edits proven via `CL_FDT_ADMIN_DATA`'s shared save/versioning layer (AR-1-002 closure) | §6 |
| FDT0-INV-04 (only ortec classes + batch FUGR change) | New class is `zcl_abapgit_ortec_*`; edits are confined to `ZCL_ABAPGIT_ORTEC_SER_ORCH` (`serialize`, `dispatch_batch`, `route_to_sequential_fallback`), `Z_ABAPGIT_ORTEC_SER_BATCH` (new OPTIONAL parameter + two activation-bracket lines + call-site substitution), and an additive flag pair in `ZCL_ABAPGIT_ORTEC_GIT_SWITCH`; `zcl_abapgit_persistence_ortec` and all standard classes are untouched | §4 |
| FDT0-INV-05 (bounded, no HTTP_NO_MEMORY risk) | `c_max_cache_payload_bytes` (50 MB) and `c_max_signature_rows` (200000) hard caps; row count independently bounded by system-wide BRF+ application ceiling | §2, §4 |
| FDT0-INV-06 (atomic enough — no valid-looking entry from a failed export) | `store()` is only ever reached after a fully successful `serialize()` return; single-row `MODIFY` has no partial-write window; no status column needed | §4, §8 |
| FDT0-INV-07 (concurrent workers never mismatch) | Content-addressed key (`application_id` + full-graph signature) makes any two writers for the same key write identical content by construction; shared table across both interception anchors, both now genuinely reachable (AR-1-001 closure) | §5, §6, §7 |
| FDT0-INV-08 (local #L cache, excluded from transport) | `#L` delivery class, `#RESTRICTED` data maintenance (owner-specified, §1); no transport-request logic anywhere in `store()`/`try_read()`; table holds only derived/reproducible cache content | §3 |

## 13. Acceptance criteria matrix

| AC | Status | Evidence |
|---|---|---|
| FDT0-AC-01 (DDIC) | ADDRESSED | §3 |
| FDT0-AC-02 (class/method/anchor) | ADDRESSED — activation wiring added Cycle 1 | §4, §5 |
| FDT0-AC-03 (signature + proof) | ADDRESSED — content-only-edit soundness now proven via `CL_FDT_ADMIN_DATA` source, not assumed | §6 |
| FDT0-AC-04 (hit/miss/corrupt/concurrent/error) | ADDRESSED | §7 |
| FDT0-AC-05 (parity + volatile fields) | ADDRESSED | §10 |
| FDT0-AC-06 (lock/LUW/publication/rollback/eviction) | ADDRESSED | §8 |
| FDT0-AC-07 (tests) | ADDRESSED, GATED — unit tests planned (not yet written/run, design stage; UT-15/UT-16 added Cycle 1); `AC-07 IT-01` is a literal, non-waivable stop condition (§11): implementation/deployment cannot proceed to production if IT-01 fails or is not executed | §9, §11 |
| FDT0-AC-08 (non-goals/stop conditions) | ADDRESSED — two of four Cycle 0 stop conditions resolved with evidence Cycle 1 | §11 |

## 14. Rejected alternatives

- **Per-object-type batch-prefetch provider** (mirroring DOMA/DTEL/CLAS/
  INTF/MSAG/TABL/PROG/FUGR): rejected — those providers amortize N+1 reads
  across *many* objects of a type; `FDT0` has too few objects and each is
  individually too expensive for that pattern to help (§11).
- **Custom FDT XML serializer**: rejected — explicitly forbidden, and
  redundant given `filter_xml_serialize` already produces exactly the
  bytes needed (§10).
- **Caching only the raw filtered XML string instead of the full
  `ty_serialization` `EXPORT` buffer**: rejected — would require
  reconstructing `item`/file metadata/`sha1` separately at read time,
  reintroducing a second place that could drift from
  `zcl_abapgit_objects=>serialize`'s own logic; caching the exact existing
  wire format (already used for the batch RFC today) is strictly simpler
  and safer.
- **Per-repository persisted enable flag** (mirroring `use_serial_batch`):
  rejected for this slice — would require touching
  `zcl_abapgit_persistence_ortec`, which is not `zcl_abapgit_ortec_*`-
  prefixed and therefore outside the literal `FDT0-INV-04` allow-list; a
  session-scoped flag on `ZCL_ABAPGIT_ORTEC_GIT_SWITCH` achieves the same
  safe-default-off behavior without that risk.
- **`repo_key` as part of the cache table key**: rejected — proven
  unnecessary because `export_xml_application`'s output does not depend
  on repository/devclass context (§3); adding it would only fragment the
  cache and reduce hit rate across repositories/runs referencing the same
  BRF+ application.
- **ENQUEUE-based locking around cache reads/writes**: rejected — the
  content-addressed key design already makes concurrent writes safe
  without locking (§6/§8); adding a lock would only add latency for no
  correctness benefit.
- **Soft-delete/status column on the cache table**: rejected — a cache
  row is binary (valid or absent); eviction is a hard `DELETE`, avoiding
  the extra state-machine complexity `ZAOG_OBJ_STORE`'s `status` column
  needs for its own, different (add/delete provenance) purpose.

## 15. Requirement traceability

| Requirement | Design element |
|---|---|
| OWNER_REQUIREMENT (option b, cache unchanged serializations) | Whole design |
| Transparent table, `#L`, `#RESTRICTED`, `ZAOG_*` conventions | §3 |
| Local runtime accelerator, never transported/shared cross-system | §3 (no transport logic), §8 (`FDT0-INV-08`) |
| EVIDENCE (two-object 276078 ms / 18.6% measurement) | §2 medium/large acceptance scenario, §9 IT-04 |
| EVIDENCE (no per-application-timestamp-alone signature) | §6 (full-graph signature, not a single timestamp) |
| EXACT_SOURCE_SCOPE objects | §1 evidence table cites every one that was actually read |
| FORBIDDEN_CHANGES | §11 non-goals map 1:1 to each forbidden item |

## 16. Open findings

- BLOCKING: 0
- MAJOR: 0 (AR-1-003 closed Cycle 1; AR-2-001 closed this cycle by
  making `AC-07 IT-01` a literal §11 stop condition — see §17a)
- MINOR (non-blocking, owner/implementer discretion): AR-1-004
  (`fdt_admn_0000s` client-dependency, resolved with evidence by the
  review itself — owner's key-design choice remains open, §11);
  AR-1-001-RESIDUAL (aRFC dump mid-loop could leave
  `mv_fdt0_cache_active` stuck on a reused work process — inherited,
  symmetric with the existing `set_serial_prefetch_active` exposure,
  named explicitly in §17 rather than claimed as zero residual risk).
- Four stop-condition items remain (§11), one of them a literal,
  non-waivable production gate: `AC-07 IT-01` (must PASS before
  `set_fdt0_cache_active( abap_true )` is ever enabled outside the IT-01
  test/dev session itself — implementation/deployment cannot proceed to
  production otherwise); `fdt_admn_0000s` client-dependency (verify via
  `DD03L`); the cross-application-reference residual-risk caveat (must
  stay visible in any implementation handoff); and reconciling the
  unactivated `ZCL_ABAPGIT_ORTEC_SER_ORCH` draft before applying the new
  anchor edits.

## 17. Finding-closure ledger (Cycle 1, against
`.memory/reviews/fdt0_local_cache_adversarial_review.md`)

```text
ID=AR-1-001
SEVERITY=BLOCKER
STATUS=CLOSED_AND_FIXED
ORIGINAL_CLAIM=mv_fdt0_cache_active could never become abap_true on any
  code path — the cache was inert on both interception anchors, doubly
  inert on the aRFC batch anchor that is the design's own primary target.
FIX=(1) ZCL_ABAPGIT_ORTEC_SER_ORCH=>serialize now brackets the whole run
  with set_fdt0_cache_active( abap_true ) near the top and
  set_fdt0_cache_active( abap_false ) on all three existing exit paths
  (cx_uuid_error catch, normal completion, zcx_abapgit_exception catch) —
  mirrors the proven MV_SERIAL_PREFETCH_ACTIVE bracket exactly. This alone
  closes anchor 2 (route_to_sequential_fallback, same session). (2) For
  anchor 1 (separate aRFC process, CLASS-DATA cannot cross the boundary —
  confirmed via this exact SER-SLICE-5 precedent, live-read this session):
  dispatch_batch now reads is_fdt0_cache_active( ) directly and passes it
  as a new iv_fdt0_cache_active EXPORTING parameter on the existing CALL
  FUNCTION 'Z_ABAPGIT_ORTEC_SER_BATCH'; the FM gained a matching OPTIONAL
  IMPORTING parameter and re-activates/resets the flag locally in its own
  process, bracketing its LOOP AT it_tadir exactly like the existing
  set_serial_prefetch_active( ) calls at the confirmed live line numbers
  172/311.
EVIDENCE=Live SAPRead (full method bodies) this session:
  ZCL_ABAPGIT_ORTEC_SER_ORCH methods serialize and dispatch_batch (both
  active-version, confirming the exact anchor text and existing
  set_serial_prefetch_active bracket shape); Z_ABAPGIT_ORTEC_SER_BATCH
  full signature (includeSignature=true) and grep confirming lines
  164-172 (SER-SLICE-5 comment + activation call), 285-287 (serialize
  call site), 311 (reset call) — the SER-SLICE-5 precedent this fix
  mirrors is itself live-verified, not assumed.
DESIGN_SECTIONS_UPDATED=§4 (three new/updated method blocks: ORCH
  serialize bracket, dispatch_batch CALL FUNCTION addition,
  Z_ABAPGIT_ORTEC_SER_BATCH signature+bracket), §5 (new "Activation
  wiring" subsection), §9 (UT-16), §12/§13 (INV-04/INV-07, AC-02 rows).
RESIDUAL_RISK=AR-1-001-RESIDUAL (MINOR, non-blocking, identified in the
  Cycle 2 review): Z_ABAPGIT_ORTEC_SER_BATCH's per-object loop has
  exactly one `CATCH zcx_abapgit_exception` per iteration; any other
  exception (a raw runtime dump, an uncaught non-zcx_abapgit_exception
  from BRF+'s own export chain, or from store()'s own unguarded EXPORT)
  escapes the loop without reaching the line-311
  set_fdt0_cache_active( abap_false ) reset. This exposure is inherited
  and symmetric with the existing, already-in-production
  set_serial_prefetch_active pattern (not a new architectural flaw
  introduced here), and it is not proven to cross into a *subsequent,
  unrelated* aRFC dispatch (depends on RFC server-group session reuse,
  out of EXACT_SOURCE_SCOPE) — named explicitly here rather than claimed
  as zero residual risk (AR-2-001 task's narrow wording reconciliation).
  Both interception anchors remain provably reachable with the flag
  genuinely ON in the normal (non-dump) case.
```

```text
ID=AR-1-002
SEVERITY=BLOCKER
STATUS=CLOSED_AND_FIXED
ORIGINAL_CLAIM=Design §6's non-false-hit proof was only empirically
  confirmed for the top-level application row's own save counter; it was
  unverified — and plausible to be false — for content-only edits to
  nested artifacts (decision-table cell/row values, rule formulas)
  specifically, since BRF+ commonly persists such content in dedicated
  tables distinct from the admin metadata table.
FIX=Replaced the plausibility argument with a source-level architectural
  proof, added as a new §6 subsection and three new §1 evidence rows:
  fdt_admn_0000s rows are managed exclusively by CL_FDT_ADMIN_DATA — one
  shared, non-subclassed class for every FDT object type (application,
  rule, decision table = object_type 'DT', expression, data object),
  dispatched by mv_object_type, not per-type subclassing. Every save of
  any such object goes through the one shared
  CL_FDT_ADMIN_DATA=>IF_FDT_TRANSACTION~SAVE (no-op unless
  has_unsaved_changes(), else unconditionally bumps change_timestamp on
  that object's own admin row) and its sibling NOTIFY_CHANGE (bumps
  change_timestamp + increments ts_version on content-changing setters).
  Because this layer is shared by construction, a decision-table
  cell-value edit or a rule formula edit is architecturally
  indistinguishable from any other FDT object save as far as
  version/ch_timestamp are concerned — there is no separate,
  ungoverned content-write path that bypasses it.
EVIDENCE=Live SAPRead (full method bodies) this session:
  CL_FDT_ADMIN_DATA method="*" (91-method inventory, confirming no
  per-type overrides of SAVE/NOTIFY_CHANGE exist to check), method
  "if_fdt_transaction~save" (full body), method "notify_change" (full
  body), method "update_basic" (full body, showing the shared
  CASE mv_object_type dispatch pattern); SAPNavigate action=hierarchy on
  CL_FDT_ADMIN_DATA (0 subclasses returned, confirming single-class
  type-dispatch architecture, not per-type subclassing); grep on
  IF_FDT_CONSTANTS confirming GC_OBJECT_TYPE_DBRULE_TEMPLATE='DT' is a
  first-class object type with its own admin identity.
DESIGN_SECTIONS_UPDATED=§1 (three new CONFIRMED evidence rows), §6 (new
  "Why UPDATE cannot create a false hit — including a content-only edit"
  subsection replacing the plausibility-only version), §4
  (compute_signature doc-comment cross-reference), §9 (IT-01 downgraded
  from blocking to mandatory defense-in-depth, with scope narrowed to
  decision-table-cell + rule-formula edits specifically), §11 (stop
  condition removed), §12/§13 (INV-03, AC-03 rows).
RESIDUAL_RISK=AC-07 IT-01 (live edit-and-re-signature check on a real
  decision table and a real rule) is retained as a mandatory
  pre-implementation acceptance test — defense-in-depth against any
  undiscovered bypass of the shared CL_FDT_ADMIN_DATA save layer, not
  because the mechanism itself remains in doubt. This was not executed
  this cycle (no live BRF+ content edit was made — design-only session,
  no productive/test data changes permitted per FORBIDDEN_CHANGES); it
  remains an explicit pre-implementation gate, not a design blocker.
```

```text
ID=AR-1-003
SEVERITY=MAJOR
STATUS=CLOSED_AND_FIXED
ORIGINAL_CLAIM=compute_signature's pseudocode called a non-existent
  zcl_abapgit_hash=>sha1(if_data=/iv_hash_algo=) overload — guaranteed
  compile failure.
FIX=Replaced with `rv_signature = to_upper( zcl_abapgit_hash=>sha1_string(
  lv_concat ) ).`, removing the now-unnecessary
  zcl_abapgit_convert=>string_to_xstring_utf8 call (sha1_string takes the
  STRING directly). Wrapped in TRY/CATCH zcx_abapgit_exception (the real
  method's RAISING contract) with the same "cache-unusable, return
  initial" fallback already used elsewhere in this method, preserving
  compute_signature's own no-RAISING signature and FDT0-INV-02.
EVIDENCE=Live SAPRead(method="sha1_string") on zcl_abapgit_hash, this
  session — confirms the real signature
  (IMPORTING iv_data TYPE string RETURNING VALUE(rv_sha1) RAISING
  zcx_abapgit_exception), its internal
  cl_abap_message_digest=>calculate_hash_for_char(if_algorithm='SHA1')
  call, and that it returns lower-case hex (hence the design's own
  to_upper() wrapping is still required and correct).
DESIGN_SECTIONS_UPDATED=§1 (new CONFIRMED evidence row), §4
  (compute_signature CHANGE/ERROR_ROLLBACK_FALLBACK/TESTS/STOP_IF), §9
  (UT-15), §11 (stop condition removed).
RESIDUAL_RISK=none — the fix uses an existing, already-read, exact public
  API; no further verification needed before implementation.
```

Not part of `REQUIRED_CORRECTIONS` for this cycle, left untouched:
`AR-1-004` (MINOR, client-dependency of `fdt_admn_0000s`) was already
closed with evidence by the review itself (confirmed
client-independent via `FDT_INC_KEY_0001`) and left as an explicit
owner-discretion choice between keeping the defensive `client` key
(current design, §3) or dropping it to widen the cache's cross-client
hit rate — no design-text change was needed or made this cycle beyond
what the review itself already recorded.

## 17a. Finding-closure ledger (Cycle 2, against
`.memory/reviews/fdt0_local_cache_adversarial_review.md` Cycle 2)

```text
ID=AR-2-001
SEVERITY=MAJOR
STATUS=CLOSED_AND_FIXED
ORIGINAL_CLAIM=§6/§9/§17 closed AR-1-002 with an architectural mechanism
  proof, but nothing in §11 (the design's own literal stop-condition
  list) or §13/§16 (status vocabulary) actually gated implementation or
  production enablement on IT-01 ever running — AC-07's "ADDRESSED
  (planned, not yet written/run)" status is textually identical to every
  other fully-closed AC row, and IT-01 was removed from §11's
  stop-condition list as "resolved" in Cycle 1. As written, nothing
  blocked an implementer from building and enabling the cache in
  production without ever running IT-01, for the single most severe
  failure mode (false cache hit) this design exists to prevent.
FIX=IT-01 is now a literal, non-waivable stop condition, not prose:
  (1) §11 lists it as an explicit stop condition (first bullet), stating
  unambiguously that set_fdt0_cache_active( abap_true ) must not be
  enabled outside the IT-01 test/dev session, and that implementation/
  deployment cannot proceed to production if IT-01 fails or is not
  executed; (2) §13's AC-07 row status changed from "ADDRESSED" to
  "ADDRESSED, GATED", textually distinguishable from every fully-closed
  AC row, with the same non-waivable wording inline; (3) §16 explicitly
  calls out IT-01 as one of four remaining stop conditions and the only
  one that is a literal production gate (previously implied resolved via
  the BLOCKING/MAJOR=0 counts alone); (4) §6 and §9's IT-01 prose updated
  to point to §11 as the authoritative gate rather than restating a
  softer "mandatory pre-implementation acceptance test"/"defense-in-depth"
  framing that a reader could mistake for optional.
EVIDENCE=No new source reads required for this fix (text-only
  reconciliation of the design's own internal status vocabulary, per the
  task's SOURCE_SCOPE) — direct confirmation needed for AR-2-001 was
  whether §11/§13/§16 actually listed IT-01 as a stop condition prior to
  this revision; independently re-read all three sections this cycle and
  confirmed the review's claim was accurate: IT-01 was absent from §11's
  bullet list and AC-07 used the same "ADDRESSED" token as every closed
  row.
DESIGN_SECTIONS_UPDATED=§6 (closing paragraph), §9 (IT-01 bullet status
  text), §11 (new first stop-condition bullet + resolved-list note),
  §13 (AC-07 row), §16 (open-findings summary).
RESIDUAL_RISK=none — IT-01 itself still has not been executed (no live
  BRF+ content edit was made this cycle either; design-only session, no
  productive/test data changes permitted per FORBIDDEN_CHANGES), but that
  is now unambiguously gated rather than ambiguously "planned". Executing
  IT-01 and recording its result in §1 remains the one action required
  before set_fdt0_cache_active( abap_true ) may be enabled in any
  non-test environment.
```

Not part of `REQUIRED_CORRECTION` for this cycle, addressed narrowly per
task instruction ("reconcile only if a narrow, exact design text change
is needed"): `AR-1-001-RESIDUAL` (MINOR) — the AR-1-001 ledger entry
above (§17) now names the inherited dump/exception-boundary exposure
explicitly instead of stating "none identified", closing the gap between
the design's own certainty language and the review's Cycle 2 finding,
without any mechanism/code change (matches the review's own
non-blocking, wording-only REQUIRED_CHANGE option (a)).
