# SSFO local serialization cache design

Task: `SSFO_LOCAL_CACHE_DESIGN_20260814`  
Baseline: IT8 active source, 2026-08-14  
Status: `IMPLEMENTATION_READY` under the `APPROVE_WITH_MINOR_REVISIONS` correctness and performance gates. Findings `DR-001`, `DR-002`, `DR-003`, `PERF-SSFO-003`, and `PERF-SSFO-004` are resolved below. Productive implementation may proceed in the checkpointed order; the productive switch and production rollout remain blocked by the acceptance gates in sections 10 and 13.

## 1. Decision and scope

Implement a persistent, client-isolated, full-result cache for `SSFO` serialization. The cache is used only inside the existing ORTEC adaptive serialization run window. It stores the final `zif_abapgit_objects=>ty_serialization`, including XML, extracted `.abap` files, file order, path, filename, data, SHA, and item state. A hot hit therefore bypasses `cl_ssf_fb_smart_form->load`, `xml_download`, the DOM walk, `serialize_sources`, `fix_ids`, and `sort_texts`.

The hook remains wholly in ORTEC code. No standard abapGit class is changed. The two existing ORTEC call sites that currently invoke `zcl_abapgit_ortec_fdt0_cache=>serialize` are changed to invoke a small ORTEC cache router. Classic sequential serialization and every call made while the session switch is off continue directly through `zcl_abapgit_objects=>serialize`.

The cache deliberately supports only the active Smart Form image. If a saved/inactive `STXFCONTS(XX)` image exists, caching is bypassed. This is the smallest provably correct rule because current SSFO serialization calls `LOAD` with default `IM_ACTIVE = space`: SAP first attempts the saved image and only then falls back to active data. Caching an inactive image would require proving `IMPORT ... FROM DATABASE STXFCONTS(XX)` success and all saved-image dependency semantics. Bypass preserves current behavior and gives strict active/inactive isolation.

## 2. Evidence ledger

### CONFIRMED

- `ZCL_ABAPGIT_OBJECT_SSFO=>ZIF_ABAPGIT_OBJECT~SERIALIZE` creates `CL_SSF_FB_SMART_FORM`, calls `LOAD( IM_LANGUAGE = '' )`, calls `XML_DOWNLOAD`, extracts `CODE`, `GTYPES`, `GCODING`, and `FCODING` children into separate ABAP files, then runs `FIX_IDS`, `SORT_TEXTS`, namespace normalization, and `IO_XML->SET_RAW`.
- `CL_SSF_FB_SMART_FORM=>LOAD`, when it does not already own a lock, calls its public `ENQUEUE` method with `LANGUAGE_UPD_EXIT = space`, `SUPPRESS_LANGUAGE_CHECK = space`, `MODE = 'SHOW'`, and the form name; it receives `MODIFICATION_LANGUAGE`, falls back to `SY-LANGU` only when that value is initial, and passes the resulting effective language to `SSF_READ_FORM` with default `I_ACTIVE = space`.
- `SSF_READ_FORM` reads `STXFADM`; tries saved `STXFCONTS(XX)` first; otherwise reads active `STXFOBJT`, `STXFTXT`, and `STXFCONT(XX)`; then reads language-specific `STXFADMT` and `STXFVART`.
- `SSF_READ_FMNUMBER` reads `STXFADMI`, but `ZCL_ABAPGIT_OBJECT_SSFO` does not receive or use `EX_FMNUMB` or `EX_FMNUMB_TEST`; `STXFADMI` is therefore not a serialization dependency.
- `STXFVAR` and `STXFVARI` are not read on the proven load/XML path. `STXFVART` is read and is a dependency.
- `ZCL_ABAPGIT_OBJECTS=>SERIALIZE` adds i18n output when applicable, adds metadata XML, obtains files, checks duplicates, sets item inactive state, and computes every file SHA. Caching below this boundary would require rebuilding or reparsing part of that result.
- The aRFC worker and `ZCL_ABAPGIT_ORTEC_SER_ORCH=>ROUTE_TO_SEQUENTIAL_FALLBACK` already route full serialization through `ZCL_ABAPGIT_ORTEC_FDT0_CACHE=>SERIALIZE`. The worker separately re-applies the session flag because class-data does not cross an aRFC boundary.
- `ZAOG_FDT_CACHE` proves the local-system `EXPORT ... TO DATA BUFFER` / `IMPORT ... FROM DATA BUFFER` payload pattern and transparent corrupt-row deletion.
- Current IT8 has 2,997 non-deleted `TADIR` `SSFO` objects.
- `/LOT/PL_MONITOR` currently has 64 active `STXFCONT` rows, no saved `STXFCONTS` rows, 1,366 `STXFOBJT` rows, 262 `STXFTXT` rows, two `STXFADMT` languages, and one `STXFVART` language.
- `CL_SSF_FB_SMART_FORM=>ENQUEUE` exposes `MODIFICATION_LANGUAGE TYPE SYLANGU`; it calls `RS_ACCESS_PERMISSION` with `GLOBAL_LOCK = 'X'`. `DEQUEUE` uses the same API with `MODE = 'FREE'`. A cache candidate must derive its effective language from this exact permission/lock boundary before hashing, signing, selecting, validating, or returning cached bytes.
- Data element `MANDT` is `CLNT(3)` on IT8. Existing table `ZAOG_FDT_CACHE` also begins with a MANDT-typed client key; the SSFO table deliberately uses the conventional field name `MANDT` as well as type `MANDT` so DDIC client dependency is explicit and independently verifiable.

### MEASURED

- Owner SAT baseline for `/LOT/PL_MONITOR`: 4.803 s gross serialization, 3.05 MB output.
- `FIX_IDS` is approximately 277 ms and `SORT_TEXTS` approximately 288 ms. The dominant path is Smart Form load/XML download and graph construction.
- The earlier broad trace reported 36.713 s end-to-end SSFO work; this design uses the focused 4.803 s serializer baseline for acceptance comparison.

### OWNER_DECISION

- Exact output parity is mandatory; cache misses and cache failures are transparent.
- No standard change is allowed unless essential. Current evidence proves none is needed.
- Persistent local caching is preferred over risky DOM-pass rewrites because the target form almost never changes.
- Implementation may proceed only through the checkpointed slices below; productive activation and rollout remain gated by the required runtime evidence.

### HYPOTHESIS

- Hashing bounded active persistence rows plus importing a 3.05 MB payload will be materially cheaper than 4.803 s and will avoid the measured load/XML graph. This must be confirmed by focused hot-hit SAT.
- The payload should remain below the fixed 12 MiB content and 16 MiB exported-payload limits for the target form.

### UNKNOWN

- Post-change hot-hit elapsed time and memory are not yet measured.
- Availability of a second IT8 client containing the same form is unknown. Cross-client acceptance remains a release gate; use a second client if available, otherwise prove the generated Open SQL client predicate and execute the scenario in the next available client before production rollout.

### SUPERSEDED

- `STXFADM-LASTDATE/LASTTIME` alone as an invalidation signature is rejected. It does not byte-prove changes in cluster, object text, long text, header caption, or variant caption dependencies.
- Merging DOM passes is not part of this slice. A full-result hit removes all passes without changing their semantics.

## 3. Invariants

| ID | Invariant |
|---|---|
| SSFO-I01 | A hit returns a byte- and order-identical `ty_serialization`: item, inactive flag, file count/order, path, filename, data, and SHA. |
| SSFO-I02 | No cached response is returned unless client, object type/name, effective form language resolved from the held `SHOW` enqueue, complete i18n context, serializer version, and active dependency signature match. |
| SSFO-I03 | Any saved/inactive `STXFCONTS(XX)` row disables SSFO caching for that call. |
| SSFO-I04 | Cache-disabled, identity-unknown, lock/permission failure, signature failure, miss, corrupt payload, oversize payload, SQL failure, or eviction failure falls back to current standard serialization. |
| SSFO-I05 | A hot candidate is validated while the same Smart Form `SHOW` lock/permission boundary used by `LOAD` is held; that operation is the sole source of the effective language, with initial `MODIFICATION_LANGUAGE` normalized to `SY-LANGU`. |
| SSFO-I06 | No cache method commits or rolls back the caller LUW. Only explicit admin clear owns `COMMIT WORK AND WAIT`/`ROLLBACK WORK`. |
| SSFO-I07 | One physical row exists per client/form. A different language/context/signature atomically replaces it; it can never be served across identities. |
| SSFO-I08 | Normal calls perform bounded, form-local SQL and hold at most one cache payload. No HTTP request is introduced. |
| SSFO-I09 | Payload/content, signature rows/bytes, eviction rows/batches, total rows, and total bytes have explicit bounds. Oversized work bypasses the cache. |
| SSFO-I10 | Session switches default off and are explicitly reset on every orchestrator/worker exit. aRFC workers receive the decision as an explicit parameter. |
| SSFO-I11 | Existing FDT0 behavior remains byte-identical through the new router. |
| SSFO-I12 | A serializer or payload-layout change increments `C_PAYLOAD_VERSION`; mixed payload versions are bounded misses and remain only until same-form replacement, bounded eviction/admin purge, or clear. Normal lookup does not self-delete an old version because its exact-version predicate does not select that row. |

## 4. Exact object model

All new global names are within the ABAP 30-character limit.

### 4.1 New table `ZAOG_SSFO_CACHE`

```text
deliveryClass        = L
dataMaintenance      = RESTRICTED
enhancementCategory  = NOT_EXTENSIBLE

KEY MANDT            TYPE MANDT NOT NULL
KEY FORMNAME         TYPE TDSFNAME NOT NULL
FORM_LANG            TYPE SPRAS
CONTEXT_HASH         TYPE CHAR40
SOURCE_SIGNATURE     TYPE CHAR40
PAYLOAD_VERSION      TYPE INT4
PAYLOAD              TYPE RAWSTRING(0)
PAYLOAD_SIZE         TYPE INT4
CREATED_AT           TYPE TZNTSTMPL
LAST_USED_AT         TYPE TZNTSTMPL
```

Add secondary index `ZAOG_SSFO_CACHE~001` on `LAST_USED_AT, FORMNAME`. `MANDT` must be the first key field, use data element `MANDT` (`CLNT(3)`), and make the table client-dependent in DDIC; activation must not proceed if DDIC does not recognize it as the client field. The primary key therefore enforces one row per client/form.

All productive Open SQL against `ZAOG_SSFO_CACHE` uses implicit client handling: no statement supplies `MANDT`, `CLIENT SPECIFIED`, `USING CLIENT`, or a cross-client connection. The ABAP SQL runtime injects the current `SY-MANDT` predicate for reads, writes, aggregates, updates, and deletes. Slice 1 must prove this before any caller activation by verifying active DDIC metadata (`DD03L`: position 1, field name `MANDT`, key flag set, rollname `MANDT`; `DD02L-CLIDEP = 'X'`) and by an SQL trace/smoke test showing ordinary Open SQL is restricted to the current client. The two-client acceptance gate remains mandatory before the productive switch is allowed.

### 4.2 New class `ZCL_ABAPGIT_ORTEC_SSFO_CACHE`

```abap
PUBLIC SECTION.
  CLASS-METHODS serialize
    IMPORTING
      is_item        TYPE zif_abapgit_definitions=>ty_item
      is_i18n_params TYPE zif_abapgit_definitions=>ty_i18n_params
    RETURNING VALUE(rs_serialization) TYPE zif_abapgit_objects=>ty_serialization
    RAISING zcx_abapgit_exception.

PRIVATE SECTION.
  CONSTANTS c_payload_version          TYPE i    VALUE 1.
  CONSTANTS c_max_signature_rows       TYPE i    VALUE 5000.
  CONSTANTS c_max_signature_bytes      TYPE int8 VALUE 16777216.
  CONSTANTS c_max_cache_content_bytes  TYPE int8 VALUE 12582912.
  CONSTANTS c_max_cache_payload_bytes  TYPE int8 VALUE 16777216.
  CONSTANTS c_max_cache_rows           TYPE i    VALUE 5000.
  CONSTANTS c_max_cache_total_bytes    TYPE int8 VALUE 5368709120.
  CONSTANTS c_eviction_batch_rows      TYPE i    VALUE 500.
  CONSTANTS c_max_eviction_batches     TYPE i    VALUE 10.

  CLASS-METHODS resolve_effective_language
    IMPORTING iv_formname TYPE tdsfname
    EXPORTING eo_form TYPE REF TO cl_ssf_fb_smart_form
              ev_language TYPE sylangu
    RAISING cx_ssf_fb.

  CLASS-METHODS compute_context_hash
    IMPORTING is_item TYPE zif_abapgit_definitions=>ty_item
              is_i18n_params TYPE zif_abapgit_definitions=>ty_i18n_params
              iv_language TYPE sylangu
    RETURNING VALUE(rv_hash) TYPE char40.
  CLASS-METHODS compute_active_signature
    IMPORTING iv_formname TYPE tdsfname
              iv_language TYPE spras
    EXPORTING ev_row_count TYPE i
              ev_input_bytes TYPE int8
    RETURNING VALUE(rv_signature) TYPE char40.
  CLASS-METHODS try_read
    IMPORTING iv_formname TYPE tdsfname
              iv_language TYPE spras
              iv_context_hash TYPE char40
              iv_source_signature TYPE char40
              is_item TYPE zif_abapgit_definitions=>ty_item
    EXPORTING es_serialization TYPE zif_abapgit_objects=>ty_serialization
    RETURNING VALUE(rv_found) TYPE abap_bool.
  CLASS-METHODS store
    IMPORTING iv_formname TYPE tdsfname
              iv_language TYPE spras
              iv_context_hash TYPE char40
              iv_source_signature TYPE char40
              is_serialization TYPE zif_abapgit_objects=>ty_serialization.
  CLASS-METHODS purge_to_limits.
  CLASS-METHODS get_serialization_bytes
    IMPORTING is_serialization TYPE zif_abapgit_objects=>ty_serialization
    RETURNING VALUE(rv_bytes) TYPE int8.
  CLASS-METHODS get_timestamp RETURNING VALUE(rv_ts) TYPE tzntstmpl.
```

Local payload type, in this exact field order:

```abap
TYPES: BEGIN OF ty_payload,
         payload_version  TYPE i,
         formname         TYPE tdsfname,
         form_language    TYPE spras,
         context_hash     TYPE char40,
         source_signature TYPE char40,
         serialization    TYPE zif_abapgit_objects=>ty_serialization,
       END OF ty_payload.
```

### 4.3 New router `ZCL_ABAPGIT_ORTEC_SER_CACHE`

```abap
PUBLIC SECTION.
  CLASS-METHODS serialize
    IMPORTING
      is_item        TYPE zif_abapgit_definitions=>ty_item
      is_i18n_params TYPE zif_abapgit_definitions=>ty_i18n_params
    RETURNING VALUE(rs_serialization) TYPE zif_abapgit_objects=>ty_serialization
    RAISING zcx_abapgit_exception.
```

Exact routing:

```text
FDT0 -> ZCL_ABAPGIT_ORTEC_FDT0_CACHE=>SERIALIZE(IS_ITEM, I18N_PARAMS=>NEW(IS_I18N_PARAMS))
SSFO -> ZCL_ABAPGIT_ORTEC_SSFO_CACHE=>SERIALIZE(IS_ITEM, IS_I18N_PARAMS)
other -> ZCL_ABAPGIT_OBJECTS=>SERIALIZE(IS_ITEM, I18N_PARAMS=>NEW(IS_I18N_PARAMS))
```

### 4.4 Existing switch `ZCL_ABAPGIT_ORTEC_GIT_SWITCH`

Add private `CLASS-DATA MV_SSFO_CACHE_ACTIVE TYPE ABAP_BOOL VALUE ABAP_FALSE` and public methods:

```abap
CLASS-METHODS is_ssfo_cache_active
  RETURNING VALUE(rv_active) TYPE abap_bool.
CLASS-METHODS set_ssfo_cache_active
  IMPORTING iv_active TYPE abap_bool.
```

They are direct getter/setter methods, identical in shape to the FDT0 methods. No persistent user setting is added.

### 4.5 Existing orchestrator and worker

- `ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE`: set SSFO cache active immediately beside FDT0 activation; reset it beside every existing FDT0 reset on UUID failure, successful exit, and exception exit.
- `ZCL_ABAPGIT_ORTEC_SER_ORCH=>ROUTE_TO_SEQUENTIAL_FALLBACK`: replace the FDT0 cache call with `ZCL_ABAPGIT_ORTEC_SER_CACHE=>SERIALIZE`, passing the already-final `LS_I18N_PARAMS` structure.
- `ZCL_ABAPGIT_ORTEC_SER_ORCH=>DISPATCH_BATCH`: pass `IV_SSFO_CACHE_ACTIVE = ZCL_ABAPGIT_ORTEC_GIT_SWITCH=>IS_SSFO_CACHE_ACTIVE( )` beside `IV_FDT0_CACHE_ACTIVE`.
- `Z_ABAPGIT_ORTEC_SER_BATCH`: add optional importing `VALUE(IV_SSFO_CACHE_ACTIVE) TYPE CHAR1 OPTIONAL`; set the worker-local flag beside FDT0 at entry; replace the FDT0 cache call with the router; reset the SSFO flag beside FDT0 at exit.
- Keep `IV_INPUT_VERSION = 1`; the work-item wire format is unchanged and the new FM parameter is optional.

### 4.6 Existing admin class/report

- `ZCL_ABAPGIT_ORTEC_CACHE_ADMIN=>CLEAR_SERIALIZATION_CACHE`: delete `ZAOG_FDT_CACHE` and `ZAOG_SSFO_CACHE` in one admin-owned LUW, return the combined `SY-DBCNT` total, commit once, and roll back both deletes on any exception. Change error text to `failed to clear serialization caches`.
- Add `CLASS-METHODS PURGE_SSFO_CACHE RETURNING VALUE(RV_DELETED) TYPE I RAISING ZCX_ABAPGIT_ORTEC_GIT`. It performs bounded age/size maintenance by calling the same metadata-only eviction algorithm until limits are met; it owns one final commit/rollback.
- `ZABAPGIT_ORTEC_CACHE_ADMIN`: change confirmation/result text from BRF+-only to `BRF+ and Smart Form serialization caches`. Add checkbox `P_PURGSS TYPE ABAP_BOOL` labeled by a text element, invoking `PURGE_SSFO_CACHE`; it is separate from destructive clear.

## 5. Canonical identity and signature

### 5.1 Effective Smart Form language and lock ownership

`RESOLVE_EFFECTIVE_LANGUAGE` is the only cache helper allowed to choose the form language. It creates `EO_FORM TYPE REF TO CL_SSF_FB_SMART_FORM`, then executes the exact public call used by `LOAD` when it does not already own a lock:

```abap
eo_form->enqueue(
  EXPORTING
    language_upd_exit       = space
    suppress_language_check = space
    mode                    = 'SHOW'
    formname                = iv_formname
  IMPORTING
    modification_language   = ev_language ).
IF ev_language IS INITIAL.
  ev_language = sy-langu.
ENDIF.
```

Successful return means `EO_FORM` owns the `SHOW` permission/global-lock boundary and the caller owns cleanup through `EO_FORM->DEQUEUE( FORMNAME = IV_FORMNAME )` followed by `FREE EO_FORM`. The helper must clear both exports before the call and on every exception. It must not accept a caller-supplied language, call `RS_ACCESS_PERMISSION` directly, or perform SQL. A cache operation never computes context, signature, row predicates, or payload validation before this helper succeeds.

### 5.2 Context hash

`COMPUTE_CONTEXT_HASH` rejects initial `IS_ITEM-OBJ_NAME`, object type other than `SSFO`, initial `SY-MANDT`, or initial `IV_LANGUAGE`. It constructs a typed structure in this exact order:

```text
PAYLOAD_VERSION
SY-MANDT
literal object type 'SSFO'
IS_ITEM (complete structure, unchanged)
IS_I18N_PARAMS (complete structure, including translation-language order)
IV_LANGUAGE (the effective language returned by `RESOLVE_EFFECTIVE_LANGUAGE`)
```

It exports that structure to a data buffer and computes uppercase `ZCL_ABAPGIT_HASH=>SHA1_BLOB`. The cache is local to one SAP system/kernel, so typed ABAP export bytes are the canonical framing; no delimiter-based string concatenation is allowed. `C_PAYLOAD_VERSION` invalidates serializer/payload changes.

### 5.3 Active source signature

`COMPUTE_ACTIVE_SIGNATURE` executes these exact bounded reads in order:

1. `SELECT SINGLE * FROM STXFADM WHERE FORMNAME = @IV_FORMNAME`.
2. `SELECT RELID, FORMNAME, SRTF2 FROM STXFCONTS WHERE RELID = 'XX' AND FORMNAME = @IV_FORMNAME UP TO 1 ROWS`. Any row returns initial signature and disables caching.
3. `SELECT * FROM STXFCONT WHERE RELID = 'XX' AND FORMNAME = @IV_FORMNAME ORDER BY RELID, FORMNAME, SRTF2`, limited to remaining row budget plus one.
4. `SELECT * FROM STXFOBJT WHERE FORMNAME = @IV_FORMNAME ORDER BY LANGU, FORMNAME, OBJTYPE, INCLUDE`, limited to remaining budget plus one.
5. `SELECT * FROM STXFTXT WHERE FORMNAME = @IV_FORMNAME ORDER BY SPRAS, TXTYPE, FORMNAME, INCLUDE, LINENR`, limited to remaining budget plus one. This intentionally includes every text type: it is a safe invalidation superset of `SSF_READ_FORM`'s internal `C_TEXT_FORM` filter and avoids coupling to an inaccessible function-group constant.
6. `SELECT * FROM STXFADMT WHERE LANGU = @IV_LANGUAGE AND FORMNAME = @IV_FORMNAME ORDER BY LANGU, FORMNAME`.
7. `SELECT * FROM STXFVART WHERE LANGU = @IV_LANGUAGE AND FORMNAME = @IV_FORMNAME ORDER BY LANGU, FORMNAME, VARI`, limited to remaining budget plus one.

The typed signature envelope contains, in exact order: literal `SSFO_ACTIVE_SIGNATURE_V1`, `SY-MANDT`, form name, language, complete `STXFADM` row, then the five ordered tables above. Export it once to a data buffer and hash with uppercase `SHA1_BLOB`.

Return initial signature when the admin row or active cluster is absent, a saved row exists, total selected rows exceed 5,000, exported signature input exceeds 16 MiB, or any SQL/export/hash exception occurs. Clear row tables before returning. This is a transparent cache bypass.

`STXFADMI`, `STXFVAR`, and `STXFVARI` are excluded for the confirmed reasons in section 2. A future SAP support change that adds one to the XML-producing load path requires incrementing `C_PAYLOAD_VERSION`, extending this envelope, and clearing the cache during deployment.

## 6. Read, miss, publication, and error semantics

### Hot candidate/read

1. If switch off or identity invalid, call standard serialization immediately.
2. Call `RESOLVE_EFFECTIVE_LANGUAGE`. This acquires the current permission/global-lock boundary and returns both the lock-owning Smart Form object and the effective language without calling `LOAD`.
3. While that same lock is held, compute context hash and active signature once using only the returned effective language.
4. `SELECT SINGLE PAYLOAD FROM ZAOG_SSFO_CACHE` by primary form key plus predicates `FORM_LANG`, `CONTEXT_HASH`, `SOURCE_SIGNATURE`, and `PAYLOAD_VERSION`.
5. Import `TY_PAYLOAD`; validate every envelope field including equality of `FORM_LANGUAGE` to the resolved effective language, exact stored item equality, non-empty files, cumulative content at most 12 MiB, and for every file `SHA1_BLOB(FILE-DATA) = FILE-SHA1`.
6. On valid hit, update `LAST_USED_AT`, dequeue, free the Smart Form object, and return the stored serialization.
7. A failed `LAST_USED_AT` update makes the candidate non-returnable. On language-resolution/lock/signature/read/import/validation/update/dequeue error, discard candidate state, attempt dequeue/free, and execute standard serialization. Cache errors never replace the standard result or exception.

Because the lookup includes `PAYLOAD_VERSION = C_PAYLOAD_VERSION`, a row written by an older payload version is an ordinary bounded miss; `TRY_READ` neither selects nor self-deletes it. The next successful publication for the same client/form replaces it atomically. Otherwise it remains bounded by the one-row/client/form key until normal eviction, explicit admin purge, or clear. Corrupt rows selected at the current version are deleted best-effort in the caller-owned LUW; failure to delete is still a transparent miss and no cache method commits that cleanup.

### Cold/miss and safe publication

1. After a valid locked signature misses, dequeue/free and call `ZCL_ABAPGIT_OBJECTS=>SERIALIZE` unchanged.
2. Return that standard result regardless of later cache work.
3. For publication only, call `RESOLVE_EFFECTIVE_LANGUAGE` again. Recompute context hash and active signature while its returned `SHOW` lock is held. Store only if post-serialization effective language equals the pre-serialization effective language, post-context hash equals pre-context hash, and post-signature equals pre-signature. A language change is always a transparent no-store result even if all language-independent rows are unchanged.
4. Reject content above 12 MiB before `EXPORT`. Export `TY_PAYLOAD`; reject payload above 16 MiB.
5. `MODIFY ZAOG_SSFO_CACHE FROM LS_ROW` atomically replaces the one client/form row. Run bounded purge in the same caller-owned LUW. Always dequeue/free in cleanup.
6. No cache exception is propagated after standard serialization succeeded.

The lock linearization point prevents a hit from racing an editor between language resolution, signature, and return. Cache publication cannot make stale data visible: a source change produces another signature; a modification-language change changes context/signature identity or fails the pre/post language comparison; a different client/language/context does not satisfy row predicates; a concurrent writer can only replace the row and cause a miss or another exact-key hit.

## 7. Capacity, eviction, and performance model

### Bounds and batching

| Dimension | Policy |
|---|---|
| Signature rows | 5,000 total across active dependency tables; fetch remaining budget + 1 to detect overflow. |
| Signature bytes | 16 MiB exported input; larger forms bypass. |
| Serialization content | 12 MiB sum of file XSTRING lengths; larger results are returned but not cached. |
| Exported cache payload | 16 MiB; larger payload is not stored. |
| Physical rows | Target 5,000 committed rows per client; primary key permits only one row/form. |
| Physical payload bytes | Target 5 GiB per client using `SUM(CAST(PAYLOAD_SIZE AS DEC(31,0)))`. |
| Eviction batch | 500 metadata-only rows selected by `LAST_USED_AT, FORMNAME`; one set-based delete per batch. |
| Eviction work/store | At most 10 batches, 5,000 metadata rows, and 20 select/delete statements. If pre-existing corruption remains over limit, delete the just-written row and bypass further growth; admin purge/clear repairs it. |
| Oversized object | Serialize normally; do not combine/store it. No chunked payload format is introduced. |

`PURGE_TO_LIMITS` never selects `PAYLOAD`. It first reads aggregate count/bytes. While over limits, it selects at most 500 oldest `FORMNAME, PAYLOAD_SIZE` rows, builds a deduplicated range, and executes one `DELETE ... WHERE FORMNAME IN @RANGE`. It subtracts deleted metadata locally. It performs at most ten batches. Concurrent transactions can temporarily exceed the target by at most one accepted row/payload per concurrent writer; the primary key, per-entry cap, ten-batch repair, and next store/admin purge make growth bounded and self-healing. Evicting a row concurrently being read is safe because the reader already imported its private payload; at worst the update affects zero rows and the next call misses.

### SQL/HTTP complexity

- Hot hit: 7 dependency SELECTs + 1 cache SELECT + 1 last-used UPDATE = 9 SQL statements; 0 HTTP. One `RESOLVE_EFFECTIVE_LANGUAGE` call performs one `SHOW` enqueue and one dequeue through the SAP enqueue/permission service; language resolution itself performs no SQL. Work is `O(R + B)` for this form's bounded signature rows/bytes and payload bytes, never repository-wide.
- Cold miss without eviction: 7 pre-signature SELECTs + 1 cache SELECT + current standard serializer SQL + 7 publication-signature SELECTs + 1 cache MODIFY + 1 aggregate = 17 cache SQL statements plus unchanged standard work; 0 added HTTP. Cache preflight and publication each perform one `RESOLVE_EFFECTIVE_LANGUAGE`/dequeue pair; the unchanged standard serializer retains its own `LOAD` enqueue behavior.
- Cold store with maximum eviction: at most 37 cache SQL statements plus unchanged standard work; 0 HTTP.
- Serialization of `N` SSFO objects is `O(N)` calls with constant bounded work per object. No SQL or network call is made per DOM node, source line, or cached file.

### Peak memory

- Current target: approximately 3.05 MB final files plus bounded signature rows/export and imported payload.
- Enforced worst-case cache-path model per internal session: each ownership group below has an independent byte cap; the count is the maximum number of simultaneously live payload-sized ownership groups, not the number of component file XSTRINGs inside one serialization. File content within `TY_SERIALIZATION` is one aggregate owner capped by cumulative `XSTRLEN`; SHA validation passes each file data field without making a second file-sized copy.
- Only one payload is read or written per call. Eviction loads metadata only.

| Phase | Simultaneous XSTRING/payload owners | Maximum groups | Maximum accounted bytes | Mandatory lifetime rule |
|---|---|---:|---:|---|
| Hot signature | Collected signature row payloads; typed signature export buffer | 2 | 16 MiB + 16 MiB = 32 MiB | Enforce cumulative signature bytes while collecting; after hashing, clear the envelope, selected row tables, and export buffer before cache `SELECT`. |
| Hot read/import/validation | Selected database `PAYLOAD`; imported serialization file data; current SHA input alias | 2 owners + 1 non-copying alias | 16 MiB + 12 MiB = 28 MiB | Import only after signature owners are cleared. Validate SHA by reference/alias; no copied file-sized hash input is permitted. Clear selected payload on miss/corruption before standard serialization. |
| Cold standard serialization | Returned standard serialization | 1 | 12 MiB cache-eligible content; larger content follows oversize bypass | Clear all preflight signature and cache-read owners before invoking standard serialization. |
| Cold publication signature | Retained standard serialization; collected post-signature rows; post-signature export buffer | 3 | 12 MiB + 16 MiB + 16 MiB = 44 MiB | Hash, compare, then clear both signature owners before payload export. |
| Cold payload export/SQL write | Retained standard serialization; exported `TY_PAYLOAD`/row payload; transient SQL transfer copy | 3 | 12 MiB + 16 MiB + 16 MiB = 44 MiB | The exported buffer becomes the row payload without another ABAP copy. Clear row/export payload immediately after synchronous `MODIFY` returns and before purge. |
| Oversize bypass | Returned standard serialization only | 1 cache-added group = 0 | Cache delta 0 after size rejection | Do not export or store. Content above 12 MiB remains solely the unchanged standard result; clear cache work first. |
| Corrupt payload | Selected database payload; partially/fully imported serialization | 2 | 16 MiB + 12 MiB = 28 MiB | Clear both before fallback; best-effort metadata-key delete adds no payload owner. |
| Eviction | Up to 500 `FORMNAME,PAYLOAD_SIZE` metadata rows | 0 payload groups | Metadata only, target <=64 KiB per batch | `PAYLOAD` is never selected; no standard result or store payload remains live when purge begins. |

The largest explicit cache-path overlap is therefore 44 MiB plus bounded ABAP/container metadata and kernel bookkeeping. The acceptance ceiling remains a conservative 96 MiB peak delta over the cache-disabled serializer baseline. Implementation must stop if ABAP parameter direction or SQL buffering introduces another payload-sized copy; it may resume only after the table above and measured ceiling are revised and reviewed.

### Cardinality behavior

| Stored/form cardinality | Expected behavior |
|---|---|
| 1 | One row; hot hit executes 9 cache SQL statements and no Smart Form load/XML download. |
| 1,000 | At most 1,000 rows/client; no full-cache scan except one aggregate on cold store/admin. Reads remain primary-key lookups. |
| 40,000 | Cache retains at most the 5,000 most recently used rows/5 GiB target. A 40,000-form sweep churns safely; per-session memory stays bounded and no payload-wide scan occurs. |
| 1,000,000 | Normal reads remain primary-key/form-local. Stores perform bounded eviction or skip publication after ten batches. The cache does not accumulate one million rows from an empty, correctly maintained deployment. Admin clear is set-based; no million-row internal table is built. |

Cache scope is persistent SAP database, isolated by client and form; the activation switch is internal-session scoped. Cache lifetime spans aRFC/dialog sessions until signature mismatch, replacement, eviction, admin purge, or clear. The caller/aRFC framework owns the normal serialization LUW. Cache code issues no commit/rollback. Admin clear/purge owns its explicit LUW.

## 8. Crash and concurrency matrix

| Event | Visible result | Persistent/cache outcome | Recovery |
|---|---|---|---|
| Crash before standard serialization | No successful object result | No new cache row | Existing retry behavior. |
| Crash after standard result but before cache store | Standard caller may fail with existing run semantics | Cache unchanged | Retry serializes or hits prior exact row. |
| Crash during `MODIFY`/eviction before LUW commit | No partial committed row set | Database rollback by LUW owner | Retry; cache is optional. |
| Corrupt/import-mismatch payload | Standard result is used | Matching row deleted in caller LUW when possible | Next successful cold call republishes. |
| Editor holds form | Cache preflight cannot acquire `SHOW` permission/lock | No cache response | Standard serializer runs and preserves current behavior. |
| Enqueue returns non-initial modification language | That language is used for context, signature, row predicate, and payload validation | No `SY-LANGU`-keyed response can be returned | Standard and cached paths address the same effective language. |
| Enqueue returns initial modification language | Effective language normalizes to `SY-LANGU`, matching `LOAD` | Normal exact-language lookup/publication | Same normalization is repeated on publication. |
| Modification language changes across cold serialization | Standard result is returned | Pre/post language mismatch prevents publication | Next call resolves and signs the new effective language. |
| Form changes during cold serialization | Standard result is returned | Post-signature differs; no store | Next call signs new source. |
| Form changes during candidate hit | Blocked by `SHOW` lock; no mixed read | Exact locked signature only | Editor proceeds after dequeue. |
| Two identical cold writers | Both may serialize | Same primary row; last identical write wins | No correctness impact. |
| Different language/context writers | Both results remain private and correct | Last row replaces earlier context; earlier context subsequently misses | Bounded one-row/form policy. |
| Eviction races a hit | Reader already imported payload | Row may be deleted; last-used update may affect zero rows | Current response valid; next call misses. |
| Admin clear races read | Reader may finish from private imported payload | Rows deleted and committed by admin | Next call cold. |
| Flag reset omitted by abnormal runtime termination | Internal session terminates | Persistent rows remain guarded by identity | New sessions default off; normal exits have explicit reset tests. |

## 9. Migration, mixed version, and rollback

1. Create/activate client-dependent `ZAOG_SSFO_CACHE` and its index. Before activating any caller, verify leading key `MANDT TYPE MANDT`, `DD03L` position/key/rollname metadata, `DD02L-CLIDEP = 'X'`, and implicit-client Open SQL behavior. No data migration exists; table starts empty.
2. Activate `ZCL_ABAPGIT_ORTEC_SSFO_CACHE`, router, switch, and admin changes.
3. Activate `Z_ABAPGIT_ORTEC_SER_BATCH` with optional SSFO flag before activating orchestrator callers.
4. Activate orchestrator/report last. Until then, old callers do not pass the optional flag and workers default SSFO caching off.
5. On payload/source-signature code changes, increment `C_PAYLOAD_VERSION` and clear caches in the deployment checklist. If clear is missed, the exact-version lookup treats the old row as a bounded miss; successful same-form publication replaces it, and normal eviction/admin purge/clear eventually removes it. No old-version payload is selected or self-deleted by `TRY_READ`.
6. Emergency rollback: first force `SET_SSFO_CACHE_ACTIVE( ABAP_FALSE )`/remove orchestrator activation, then roll back router/worker changes. The table may remain; no standard path reads it. Drop table/index only in a later cleanup transport.
7. FDT0 data/schema are unchanged. `CLEAR_SERIALIZATION_CACHE` intentionally becomes a combined clear; report text states both cache families.

## 10. Exact tests and acceptance

### ABAP Unit

Place tests in each owning class test include; use local friends, `CL_OSQL_TEST_ENVIRONMENT`, and no standalone global test class.

`ZCL_ABAPGIT_ORTEC_SSFO_CACHE` tests:

1. `effective_language_from_enqueue`: seam the public Smart Form boundary to return non-initial modification language `D` under logon language `E`; assert `D` is returned with the lock-owning object and is used in context, signature, row predicate, and payload validation.
2. `effective_language_fallback`: seam initial modification language under logon language `E`; assert effective language `E` and the same identity propagation.
3. `effective_language_failure_clears`: enqueue exception leaves form reference/language initial and routes to standard serialization.
4. `publication_language_change`: preflight `D`, publication `E`; standard result is returned and no row is stored.
5. `context_item_changes_hash`: change each `TY_ITEM` field used by the complete structure; hash differs.
6. `context_i18n_changes_hash`: main language, main-language-only, LXE, comment suppression, translation list value/order each differ.
7. `signature_active_fixture`: seed all six dependency tables plus active cluster; signature is non-initial and stable across insertion order.
8. `signature_adm_change`, `signature_cluster_change`, `signature_objt_change`, `signature_text_change`, `signature_admt_change`, `signature_vart_change`: mutate one field/byte in each dependency; signature differs.
9. `signature_inactive_bypasses`: add one `STXFCONTS` `XX` row; signature is initial.
10. `signature_language_isolated`: D/E caption fixtures produce different signatures.
11. `signature_row_cap_bypasses`: 5,001 rows return initial and report bounded count.
12. `payload_roundtrip_exact`: two files, including empty file data, retain item, inactive, order, path, filename, data, and SHA exactly.
13. `payload_corrupt_is_miss`, `payload_context_is_miss`, `payload_signature_is_miss`, `payload_item_is_miss`, `payload_bad_sha_is_miss`, `payload_zero_files_is_miss`: return false, clear output, and best-effort delete the selected current-version corrupt row in the caller LUW. `payload_version_is_bounded_miss`: an old-version row is not selected or deleted, returns false with clear output, is replaced by a successful same-form store, and is removable by purge/clear.
14. `last_used_failure_is_miss`: a validated payload with failed recency update is discarded and standard serialization is used.
15. `oversize_content_not_stored` and `oversize_export_not_stored`: no row; input result unchanged.
16. `same_form_replaces_context`: two contexts leave one physical row; only latest exact context hits.
17. `purge_rows_and_bytes`: inject limits through local-friend call to the private purge helper; oldest rows removed in 500-row-or-smaller batches, payload never selected, newest retained.
18. `purge_work_cap_skips_new_row`: pre-existing over-limit fixture remains bounded in work; current attempted row is removed after ten batches.

`ZCL_ABAPGIT_ORTEC_SER_CACHE` tests:

19. `other_type_standard_parity`: a stable tiny object result equals direct standard serialization.
20. `fdt0_route_regression`: switch-off FDT0 result equals direct existing FDT0 wrapper result.
21. `ssfo_switch_off_standard`: SSFO switch off does not read/write `ZAOG_SSFO_CACHE`.

`ZCL_ABAPGIT_ORTEC_GIT_SWITCH` tests:

22. `ssfo_default_off`, `ssfo_roundtrip`, and teardown restoring false.

`ZCL_ABAPGIT_ORTEC_CACHE_ADMIN` tests:

23. `clear_both_serial_caches`: one FDT and one SSFO row yield deleted count 2 and both tables empty.
24. `purge_ssfo_only`: over-limit SSFO fixtures are reduced; FDT rows remain.

`ZCL_ABAPGIT_ORTEC_SER_ORCH` tests:

25. Extend controlled exit-path tests/static friend seams to assert SSFO flag false after success and raised exception; fallback router test keeps existing zero-file/failure semantics.

### IT8 parity and invalidation

Use `/LOT/PL_MONITOR`; clear serialization caches before each scenario. Persist complete `TY_SERIALIZATION` fixtures or calculate a deterministic digest over every item/file field and ordered bytes.

1. Cache disabled standard run vs enabled cold run vs enabled hot run: assert complete structure equality, file order/count, XML bytes, extracted ABAP bytes, path, filename, SHA, item fields, and inactive flag.
2. Repeat through forced sequential fallback and real aRFC worker. Assert identical output and that worker-local SSFO flag resets after return.
3. Active dependency invalidation: make and activate one supported Smart Forms change for each observable category represented by cluster, object caption, long text, header caption, and variant caption. After each, assert old row does not hit, cold output differs only as standard dictates, then next hot output equals cold.
4. Inactive isolation: save without activation so `STXFCONTS(XX)` exists. Assert no SSFO row is read/written and enabled output equals cache-disabled output. Activate/discard, then assert active caching resumes.
5. Effective-language parity and isolation: first identify or create an approved IT8 Smart Form fixture whose `SHOW` enqueue returns a non-initial modification language different from `SY-LANGU`. Record both values, then run cache-disabled, enabled-cold, and enabled-hot serialization. Assert complete parity, persisted `FORM_LANG` equals modification language rather than logon language, and a session resolving another effective language cannot hit that payload. Also execute the initial-return fallback case and prove it uses `SY-LANGU`.
6. Client isolation: after Slice 1 metadata/SQL proof, run in two clients containing the form, or create an approved non-production fixture in the second client. Use ordinary Open SQL only. Assert each client sees only its own row/result and trace evidence contains the implicit current-client predicate. Absence of a second-client execution blocks the productive switch and production rollout.
7. Corruption: alter a test cache payload/version/SHA through an approved diagnostic fixture. Assert transparent miss, row removal, and exact standard output.
8. Oversize: use a synthetic serialization fixture above 12 MiB and exported payload above 16 MiB. Assert result returned and no cache row.
9. Concurrency: launch two aRFC serializations of the same form and two with different language/context. Assert no dump/deadlock, exact outputs, at most one row/client/form, and a subsequent exact-context call either hits or transparently misses.
10. Admin: report confirmation names both families; combined clear removes both; SSFO purge respects row/byte limits.

### Performance acceptance

- Focused non-aggregated SAT hot run contains no `CL_SSF_FB_SMART_FORM->LOAD`, `SSF_READ_FORM`, `CL_SSF_FB_SMART_FORM->XML_DOWNLOAD`, `FIX_IDS`, or `SORT_TEXTS` calls.
- Hot gross time is at most 1.0 s and at least 70% below the 4.803 s focused baseline for the unchanged 3.05 MB form, measured over five runs after one warm-up; report median and worst.
- Hot cache SQL count is at most 9 and HTTP count is zero; exactly one cache `SHOW` language-resolution/enqueue and one matching dequeue occur, and no language-resolution SQL is added.
- Cache-path peak-memory delta is at most 96 MiB over cache-disabled baseline and no payload above configured limits is retained. For a 12 MiB-content/16 MiB-export fixture, SAT reports peak delta for hot read and cold publication separately and confirms the phase ownership maxima above: at most two payload owners plus one non-copying SHA alias on hot validation, at most three payload owners on cold publication, and no signature/export overlap with cache import.
- Direct `SHOW` lock acceptance (`PERF-SSFO-004`): instrument or isolate in focused SAT/enqueue trace the interval from successful `RESOLVE_EFFECTIVE_LANGUAGE` enqueue return through the matching dequeue. Run five warmed hot hits after one warm-up; record median and worst lock-hold duration, with both at most 1.0 s, plus total hot time. Record the exact concurrent aRFC worker count used; exercise concurrent hot hits and one editor-contention scenario; assert no deadlock, no lock leak on every cache-error fallback, and exactly one matching `SHOW` enqueue/dequeue pair per candidate.
- Medium: 5,000 synthetic metadata rows plus at least 20 real/synthetic serialization calls demonstrate primary-key reads, eviction batches, cold/warm behavior, and no payload-wide scan.
- Large: 40,000 attempted cache publications with mixed repeated forms/contexts retain at most 5,000 committed rows and 5 GiB target plus documented concurrent-writer overshoot; per-session memory remains bounded and normal reads do not scale with table size.
- Million-row resilience: pre-load or simulate an administratively malformed 1,000,000-row table only in an isolated performance system. One normal store performs no more than ten 500-row eviction batches, removes/skips its new row if still over, and allocates no million-row internal table. Admin `CLEAR` remains set-based.

## 11. Requirement traceability

| Requirement | Design proof |
|---|---|
| Exact XML/files/path/mode/SHA parity | Full final `TY_SERIALIZATION` payload, per-file SHA validation, tests 1-2. |
| No changed-input hit | Locked complete active signature, context hash, version, post-cold recheck, tests 3-6. |
| Transparent fallback | Section 6 catches only cache work and always calls/returns standard result. |
| Minimal standard changes | Zero standard object changes; ORTEC router replaces two existing ORTEC FDT0 calls. |
| No unbounded DB/memory | Section 7 row/byte/batch caps and oversize bypass. |
| Client/language/object isolation | DDIC-recognized leading `MANDT` client key, implicit Open SQL handling proof, enqueue-derived language/context predicates, unit tests 1-4 and IT8 tests 5-6. |
| Hot avoids load/XML | Full-result hit and SAT gate. |
| Clear admin | Combined clear plus dedicated SSFO purge. |
| Concurrent safety | Primary-key replacement, Smart Form lock, race matrix, concurrency test. |

## 12. Rejected alternatives

1. **Timestamp-only signature**: rejected because it cannot prove all loaded dependency bytes.
2. **Cache inside `ZCL_ABAPGIT_OBJECT_SSFO`**: rejected because it would need to reconstruct `MO_FILES` side effects, reparse cached XML into iXML, and still would not include outer metadata/i18n/SHA/item processing.
3. **Change standard `ZCL_ABAPGIT_OBJECTS=>SERIALIZE`**: rejected because existing ORTEC worker/fallback hooks already own the needed boundary.
4. **Cache saved/inactive source**: rejected for this slice because row presence does not prove cluster `IMPORT` success/fallback semantics. Safe bypass meets parity and isolation.
5. **Per-language/context physical rows**: rejected because context combinations permit unbounded rows. One client/form row is sufficient and bounded.
6. **Per-source-file rows/chunking**: rejected because it complicates atomic publication and parity for a 3.05 MB target already below payload limits.
7. **DOM pass fusion**: rejected because it changes high-risk canonicalization behavior and gives no benefit on a full cache hit.
8. **Commit inside cache class**: rejected because it would commit unrelated caller work. Normal cache persistence follows caller/aRFC LUW ownership.

## 13. Review findings and verdict

- `ACCEPTED_AND_FIXED DR-01`: initial timestamp concept did not cover all dependencies. Fixed by typed, ordered active-row signature.
- `ACCEPTED_AND_FIXED DR-02`: current serializer prefers saved source. Fixed by unconditional cache bypass when `STXFCONTS(XX)` exists.
- `ACCEPTED_AND_FIXED DR-03`: signature/read TOCTOU could return stale bytes. Fixed by using the existing Smart Form `SHOW` permission/lock boundary for hit validation and publication recheck.
- `ACCEPTED_AND_FIXED DR-04`: full-result payload could cross language/i18n/item contexts. Fixed by complete typed context hash and explicit form language predicate.
- `ACCEPTED_AND_FIXED DR-05`: per-signature rows could grow without bound. Fixed by one client/form primary row, row/byte targets, and bounded LRU eviction.
- `ACCEPTED_AND_FIXED DR-06`: aRFC class-data does not propagate. Fixed by explicit optional FM parameter and worker-local set/reset.
- `ACCEPTED_AND_FIXED DR-07`: low-level commit would violate caller transaction ownership. Fixed by no normal cache commit/rollback; admin alone owns commits.
- `ACCEPTED_AND_FIXED DR-08`: caching below generic serialization would omit metadata/i18n/SHA behavior. Fixed by the proven outer ORTEC interception boundary.
- `ACCEPTED_AND_FIXED DR-001`: `SY-LANGU` was incorrectly assumed to be the effective load language. Fixed by one lock-owning `RESOLVE_EFFECTIVE_LANGUAGE` helper that captures `MODIFICATION_LANGUAGE` from the exact `SHOW` enqueue used by `LOAD`, normalizes only an initial result to `SY-LANGU`, and carries that value through context, signature, row predicate, payload validation, publication comparison, and tests.
- `ACCEPTED_AND_FIXED DR-002`: the DDIC model used an ambiguous `CLIENT` field without explicit client-dependency proof. Fixed by leading key `MANDT TYPE MANDT`, mandatory DDIC client-dependent activation metadata, implicit Open SQL restrictions, trace/smoke proof, and a two-client productive-switch gate.
- `ACCEPTED_AND_FIXED DR-003`: the design incorrectly claimed an old payload version self-deletes although exact-version SQL cannot select it. Fixed by defining it as a bounded miss until atomic same-form replacement, bounded eviction/admin purge, or clear; current-version corrupt-row cleanup is separately best-effort in the caller LUW.
- `ACCEPTED_AND_FIXED PERF-SSFO-003`: payload ownership overlap was implicit. Fixed by the phase lifetime table in section 7, a maximum of three simultaneous payload-sized ownership groups/44 MiB explicit bytes, mandatory clear points, non-copying SHA input, and a 12 MiB-content/16 MiB-export SAT gate.
- `ACCEPTED_AND_FIXED PERF-SSFO-004`: whole-hit elapsed time did not directly bound the Smart Form lock. Fixed by a five-warmed-run SAT/enqueue interval metric with median/worst at most 1.0 s, recorded aRFC concurrency, editor contention, exact enqueue/dequeue pairing, and lock-leak checks on error fallback.

Revision verdict: `APPROVE_WITH_MINOR_REVISIONS`; all required minor revisions are incorporated and the design is `IMPLEMENTATION_READY`. This does not waive runtime proof: the productive switch and production rollout remain blocked until all ABAP Unit, effective-language parity, DDIC/client, invalidation, second-client, concurrency, capacity, phase-memory, direct lock-hold, and hot/scale SAT gates pass.

## 14. Checkpointable implementation handoff

### Slice 1: DDIC and cache core

```text
FILE_OR_OBJECT=ZAOG_SSFO_CACHE and secondary index ZAOG_SSFO_CACHE~001
METHOD_OR_DDIC=table/index definitions in section 4.1
ANCHOR=new objects in package $ABAPGIT_ORTEC_SERIAL_CORE
ACTION=insert
CHANGE=create exact fields/key/index; no migration rows
INVARIANTS=SSFO-I02,SSFO-I07,SSFO-I09
SQL_SHAPE=primary-key form lookup; LAST_USED_AT/FORMNAME eviction index
ERROR_ROLLBACK_FALLBACK=activation failure stops slice; no callers activated
TESTS=DDIC activation metadata, current-client Open SQL trace/smoke test, and two-client isolation acceptance before productive switch
VALIDATION=IT8 activate table/index; verify DD03L position 1 MANDT/key/rollname, DD02L-CLIDEP = X, index metadata, and implicit current-client SQL predicate
STOP_IF=object names/field types differ from section 4.1, MANDT is not the recognized client field, DD02L-CLIDEP is not X, or ordinary Open SQL can observe another client's row
```

```text
FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_SSFO_CACHE
METHOD_OR_DDIC=all signatures and algorithms in sections 4.2,5,6,7
ANCHOR=new final public class in package $ABAPGIT_ORTEC_SERIAL_CORE
ACTION=insert
CHANGE=implement decision-free flow exactly; resolve effective language from the exact SHOW enqueue, retain its lock-owning form object, use typed EXPORT framing, active-only signature, full-result payload, pre/post language equality, and bounded purge; add local-friend ABAP Unit tests 1-18
INVARIANTS=SSFO-I01..SSFO-I09,SSFO-I12
SQL_SHAPE=seven ordered form-local signature reads; one primary-row payload read/update; one MODIFY; aggregate plus maximum ten 500-row metadata eviction batches
ERROR_ROLLBACK_FALLBACK=all cache errors produce miss/bypass; old payload versions remain bounded misses until replacement/purge/clear; selected current-version corrupt rows delete best-effort in caller LUW; never COMMIT/ROLLBACK; standard result/exception remains authoritative
TESTS=tests 1-18 including payload_version_is_bounded_miss, replacement, purge/clear removal, and phase-memory fixtures; use a seam for CL_SSF_FB_SMART_FORM enqueue/dequeue and CL_OSQL_TEST_ENVIRONMENT for SQL dependencies
VALIDATION=IT8 syntax, activation, ABAP Unit, ATC; inspect SQL for no payload select in purge and no SQL in file/row loops; SAT proves section 7 owner counts/clear points and section 10 direct SHOW lock interval
STOP_IF=SHOW cannot return modification language with the exact LOAD arguments, lock ownership cannot span identity/read/validation, pre/post effective language cannot be compared, another payload-sized copy violates section 7, median or worst warmed SHOW lock hold exceeds 1.0 s, typed source rows exceed declared bounds for target, or real compiler rejects bounded SQL shapes
```

### Slice 2: router, switch, and execution paths

```text
FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_SER_CACHE
METHOD_OR_DDIC=SERIALIZE
ANCHOR=new final public class in package $ABAPGIT_ORTEC_SERIAL_CORE
ACTION=insert
CHANGE=implement exact three-way routing in section 4.3 and tests 19-21
INVARIANTS=SSFO-I04,SSFO-I11
SQL_SHAPE=NONE in router
ERROR_ROLLBACK_FALLBACK=non-SSFO/FDT0 delegates unchanged to standard; exceptions propagate unchanged
TESTS=other_type_standard_parity,fdt0_route_regression,ssfo_switch_off_standard
VALIDATION=IT8 syntax, activation, ABAP Unit, ATC
STOP_IF=FDT0 direct-vs-router serialization differs
```

```text
FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_GIT_SWITCH
METHOD_OR_DDIC=MV_SSFO_CACHE_ACTIVE,IS_SSFO_CACHE_ACTIVE,SET_SSFO_CACHE_ACTIVE and testclasses
ANCHOR=existing FDT0 session switch declarations/implementations
ACTION=insert
CHANGE=add exact parallel SSFO flag/getter/setter; default false; add tests 22
INVARIANTS=SSFO-I10
SQL_SHAPE=NONE
ERROR_ROLLBACK_FALLBACK=default false
TESTS=ssfo_default_off,ssfo_roundtrip,teardown reset
VALIDATION=IT8 syntax, activation, ABAP Unit, ATC
STOP_IF=any persistent-setting dependency is introduced
```

```text
FILE_OR_OBJECT=Z_ABAPGIT_ORTEC_SER_BATCH
METHOD_OR_DDIC=function interface and body anchors IV_FDT0_CACHE_ACTIVE, line-local cache call, final resets
ANCHOR=existing FDT0 parameter/set/call/reset blocks
ACTION=insert|replace
CHANGE=add optional IV_SSFO_CACHE_ACTIVE CHAR1; set worker flag; route through SER_CACHE with LS_I18N_PARAMS; reset flag
INVARIANTS=SSFO-I10,SSFO-I11
SQL_SHAPE=NONE beyond delegated cache
ERROR_ROLLBACK_FALLBACK=worker default false when old caller omits parameter; object exceptions retain existing result-row behavior
TESTS=IT8 real aRFC parity and flag-reset scenarios
VALIDATION=activate FM before orchestrator; syntax, ATC, real worker execution
STOP_IF=FM interface cannot remain backward-compatible with optional parameter
```

```text
FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_SER_ORCH
METHOD_OR_DDIC=SERIALIZE,ROUTE_TO_SEQUENTIAL_FALLBACK,DISPATCH_BATCH and testclasses
ANCHOR=every existing FDT0 set/reset/call/dispatch line
ACTION=insert|replace
CHANGE=parallel SSFO set/reset; router call with raw LS_I18N_PARAMS; forward worker flag; add tests 25
INVARIANTS=SSFO-I04,SSFO-I10,SSFO-I11
SQL_SHAPE=NONE beyond delegated cache
ERROR_ROLLBACK_FALLBACK=all current cleanup paths additionally reset SSFO; dispatch failure still uses sequential fallback
TESTS=existing orchestrator suite plus flag cleanup and fallback routing
VALIDATION=IT8 syntax, activation, full orchestrator ABAP Unit, ATC, forced fallback and aRFC integration
STOP_IF=any exit path can leave SSFO flag true or FDT0 regression appears
```

### Slice 3: administration and production gates

```text
FILE_OR_OBJECT=ZCL_ABAPGIT_ORTEC_CACHE_ADMIN
METHOD_OR_DDIC=CLEAR_SERIALIZATION_CACHE,PURGE_SSFO_CACHE and testclasses
ANCHOR=existing FDT0-only clear method
ACTION=replace|insert
CHANGE=combined atomic clear; bounded SSFO purge; neutral error text; tests 23-24
INVARIANTS=SSFO-I06,SSFO-I09
SQL_SHAPE=two set-based DELETEs for clear; aggregate and bounded metadata batches for purge
ERROR_ROLLBACK_FALLBACK=admin owns one COMMIT WORK AND WAIT on success and ROLLBACK WORK on exception
TESTS=clear_both_serial_caches,purge_ssfo_only
VALIDATION=IT8 syntax, activation, ABAP Unit, ATC, manual report run
STOP_IF=combined clear can commit only one table or purge selects PAYLOAD
```

```text
FILE_OR_OBJECT=ZABAPGIT_ORTEC_CACHE_ADMIN
METHOD_OR_DDIC=selection screen and START-OF-SELECTION serialization-cache blocks/text elements
ANCHOR=P_CLRSER confirmation/result block
ACTION=replace|insert
CHANGE=rename user-facing BRF+-only text to BRF+ and Smart Form; add P_PURGSS action and confirmation
INVARIANTS=SSFO-I06
SQL_SHAPE=NONE in report
ERROR_ROLLBACK_FALLBACK=display admin exception; no hidden retry
TESTS=manual confirmation, cancel, clear, purge
VALIDATION=IT8 activation, ATC, authorized manual execution
STOP_IF=authorization check is weakened or destructive action lacks confirmation
```

```text
FILE_OR_OBJECT=IT8 runtime evidence
METHOD_OR_DDIC=acceptance section 10
ANCHOR=/LOT/PL_MONITOR baseline 4.803s,3.05MB
ACTION=insert evidence into a new owner-approved validation handoff after implementation
CHANGE=execute every parity,invalidation,isolation,concurrency,capacity,phase-memory,direct SHOW lock-hold,SAT scenario; do not edit this approved design except through reviewed finding convergence
INVARIANTS=SSFO-I01..SSFO-I12
SQL_SHAPE=hot <=9 cache SQL; HTTP=0
ERROR_ROLLBACK_FALLBACK=disable SSFO session flag and clear cache on any parity or stale-hit failure
TESTS=all ABAP Unit and IT8 scenarios in section 10
VALIDATION=byte parity,ATC,ABAP Unit,second client,concurrency,medium/large,SAT,phase owner counts,peak memory,direct SHOW lock median/worst and worker count
STOP_IF=any output mismatch,changed-input hit,LOAD/XML_DOWNLOAD on hot hit,hot >1.0s or <70% improvement,SHOW lock median or worst >1.0s,lock leak/deadlock,phase owner/byte bounds exceeded,rows/bytes exceed bounds,or second-client gate is unexecuted
```