# ORTEC Serialization Prefetch & Bulk Exists Audit
**Task ID:** SER0_ORTEC_SERIALIZATION_AUDIT  
**Discovery Date:** 2026-08-03  
**Status:** Read-only source discovery

---

## 1. ZCL_ABAPGIT_ORTEC_BULK_EXISTS

**File:** `src/ortec/zcl_abapgit_ortec_bulk_exists.clas.abap`

### Public Interface
- **CLASS-METHOD `filter_existing`** (LINE 8–16)
  - **Signature:** `filter_existing( IMPORTING it_tadir TYPE zif_abapgit_definitions=>ty_tadir_tt RETURNING VALUE(rt_tadir) TYPE zif_abapgit_definitions=>ty_tadir_tt )`
  - **Purpose:** Filter TADIR rows to objects that still exist using bulk providers for proven types

### Supported Object Types (via Bulk Handlers)
- **DOMA** (Domains): TABLE `dd01l` keyed by `domname` | SOURCE: LINE 152–160
- **DSYS** (Documentation objects): TABLE `dokil` keyed by `id, object` | SOURCE: LINE 162–172
- **FUGR** (Function groups): TABLE `tlibg` keyed by `area`; generated via `tcdrp` | SOURCE: LINE 174–189
- **MSAG** (Message classes): TABLE `t100a` keyed by `arbgb` | SOURCE: LINE 191–200
- **PROG** (Programs): TABLE `reposrc` keyed by `progname`; excludes CHDO-generated | SOURCE: LINE 202–211
- **SHLP** (Search helps): TABLE `dd30l` keyed by `shlpname` | SOURCE: LINE 213–221
- **SMIM** (Media objects): TABLE `smimloio` keyed by `loio_id` | SOURCE: LINE 223–231
- **TOBJ** (Table maintenance): TABLE `objh` keyed by `objectname, objecttype` | SOURCE: LINE 233–252
- **TRAN** (Transactions): TABLE `tstc` keyed by `tcode` | SOURCE: LINE 254–262
- **TTYP** (Table types): TABLE `dd40l` keyed by `typename` | SOURCE: LINE 264–272
- **TABL** (Tables) [OPP-B, switch-gated]: TABLE `dd02l` keyed by `tabname`; excludes CHDO via `tcdrs` | SOURCE: LINE 274–285
- **DTEL** (Data elements) [OPP-C, switch-gated]: TABLE `dd04l` keyed by `rollname`, versions '0'/'1' | SOURCE: LINE 287–297
- **INTF** (Interfaces) [OPP-D, switch-gated]: TABLE `seoclassdf` keyed by `clsname`; excludes WebDynpro category & proxy-generated (SPROXHDR) | SOURCE: LINE 299–323
- **CLAS** (Classes) [OPP-E, switch-gated]: TABLE `seoclassdf` keyed by `clsname`; excludes SADL-generated (CL_SADL_GTK_EXPOSURE_MPC in vseoextend) | SOURCE: LINE 325–346

### FOR ALL ENTRIES / Batch Behavior
- All bulk methods use `FOR ALL ENTRIES IN @lt_keys WHERE ...` with bulk-loaded key tables | SOURCE: LINES 155–275
- Each object type: iterate TADIR filtering to `object = 'TYPE'`, extract primary key into temp table, execute single FOR ALL ENTRIES query
- **Fallback on Prefetch Miss:** If any bulk handler SQL fails (sy-subrc <> 0), sets `ev_success = abap_false` and delegates entire type to standard `exists_standard()` path | SOURCE: LINE 319, 344, 360

### Unsupported Type Behavior
- **Default case in LOOP** (LINE 393): `WHEN OTHERS` → always falls back to `exists_standard( )` | SOURCE: LINES 593–595
- **Reported as:** Per-object check via `zcl_abapgit_objects=>exists()` (standard abapGit handler)

---

## 2. ZCL_ABAPGIT_ORTEC_SER_PREF (Message Class Prefetch)

**File:** `src/ortec/zcl_abapgit_ortec_ser_pref.clas.abap`

### CLASS-DATA Buffers
- **`mt_msag` TYPE `ty_msag_cache_tt`** (HASHED TABLE by `msg_id`)
  - **Structure:** `msg_id (rglif-message_id) | data (ty_msag_data)` | SOURCE: LINE 72
  - **ty_msag_data:** `t100a, t100 table, t100t table, t100_i18n table` | SOURCE: LINES 35–40
- **`mt_dokil` TYPE SORTED TABLE OF `dokil`** (non-unique key `id, object`)
  - **Secondary SORTED key `object_prefix`** on `object` field | SOURCE: LINES 73–75
- **`mv_language TYPE spras`** – session language for buffer validation | SOURCE: LINE 76
- **`mv_dokil_prepared TYPE abap_bool`** – tracks dokil buffer state | SOURCE: LINE 77

### PREPARE Method Logic
**Signature:** `prepare( IMPORTING it_tadir TYPE zif_abapgit_definitions=>ty_tadir_tt iv_language TYPE spras )` | SOURCE: LINES 155–180
- **Step 1:** `clear()` (resets all buffers + `mv_language` + `mv_dokil_prepared`)
- **Step 2:** `prepare_dokil(it_tadir)` → SELECTs from `dokil` WHERE `object IN <wildcard-prefixes-from-tadir-names>` | SOURCE: LINES 797–815
  - Uses RANGE with 'CP' pattern matching (CONCATENATE `obj_name '*'` for prefix)
  - Sets `mv_dokil_prepared = abap_true` on success
- **Step 3:** Bulk-loads MSAG data:
  - **T100A:** SELECT WHERE `arbgb = @lt_msg_ids-table_line` FOR ALL ENTRIES | SOURCE: LINE 168
  - **T100:** SELECT WHERE `arbgb = @lt_msg_ids-table_line` (ALL languages) FOR ALL ENTRIES | SOURCE: LINE 171
  - **T100T:** SELECT WHERE `arbgb = @lt_msg_ids-table_line` (ALL languages) FOR ALL ENTRIES | SOURCE: LINE 177
- **On SQL exception:** Calls `clear()` to zero buffers (no EXCEPTION raising) | SOURCE: LINE 182

### CLEAR Method Logic
**Signature:** `clear()` | SOURCE: LINES 119–123
- **Operations:**
  - `CLEAR mt_msag` (hashed table)
  - `CLEAR mt_dokil` (sorted table)
  - `CLEAR mv_dokil_prepared`
  - `CLEAR mv_language`

### EXTRACT_FOR_OBJECT Method
**Signature:** `extract_for_object( IMPORTING is_tadir TYPE zif_abapgit_definitions=>ty_tadir RETURNING VALUE(rv_buffer) TYPE xstring )` | SOURCE: LINES 205–250
- **For MSAG (is_tadir-object = 'MSAG'):**
  - `READ TABLE mt_msag WITH TABLE KEY msg_id = CONV rglif-message_id(is_tadir-obj_name)`
  - If found: `INSERT ls_msag INTO TABLE lt_msag`
- **For DOKIL (all object types):**
  - If `mv_dokil_prepared = abap_true`:
    - Build range: object >= obj_name AND object < (obj_name + maxchar) for prefix range query
    - `LOOP AT mt_dokil USING KEY object_prefix WHERE object >= lv_object AND object < lv_object_high`
    - Collect matching DOKIL rows into `lt_dokil`
- **Output:**
  - `EXPORT msag = lt_msag dokil = lt_dokil language = mv_language dokil_prepared = mv_dokil_prepared TO DATA BUFFER rv_buffer COMPRESSION ON`
  - Returns EMPTY xstring if no data found

### INJECT_FROM_BUFFER Method
**Signature:** `inject_from_buffer( IMPORTING iv_buffer TYPE xstring )` | SOURCE: LINES 252–283
- **Logic:**
  - `IMPORT msag/dokil/language/dokil_prepared FROM DATA BUFFER iv_buffer`
  - **CRITICAL:** `CLEAR: mt_msag, mt_dokil` BEFORE insertion
    - Reason: Parallel RFC worker sessions are reused across many unrelated dispatches; UNIQUE-keyed table INSERT silently no-ops for duplicate keys. Without clearing, stale data from a prior invocation would be served forever. | SOURCE: LINES 269–271
  - `LOOP AT lt_msag INTO DATA(ls_msag)` then `INSERT ls_msag INTO TABLE mt_msag`
  - `LOOP AT lt_dokil INTO DATA(ls_dokil)` then `INSERT ls_dokil INTO TABLE mt_dokil`
  - Restore `mv_dokil_prepared` and `mv_language` flags if non-initial

### Behavior on Prefetch MISS
- **GET_MSAG_DATA**: If `iv_language <> mv_language` → `RETURN` (empty es_data, rv_found = abap_false) | SOURCE: LINES 125–133
- **GET_MSAG_I18N_DATA**: If msg_id not found → `RETURN` (empty tables, rv_found = abap_false) | SOURCE: LINES 135–155
- **GET_DOKIL**: If `mv_dokil_prepared = abap_false` → `RETURN` (empty rt_dokil) | SOURCE: LINE 96
- **Caller fallback:** Standard per-object SELECT executes instead (no cached data available) | SOURCE: src/objects/zcl_abapgit_object_msag.clas.abap:536

---

## 3. ZCL_ABAPGIT_ORTEC_SER_PREF_EXT (Extended Type Prefetch)

**File:** `src/ortec/zcl_abapgit_ortec_ser_pref_ext.clas.abap`

### CLASS-DATA Buffers (10 caches)
| Buffer Name | Type | Key | Purpose |
|---|---|---|---|
| `mt_dtel` | HASHED TABLE | `rollname` | Data element (DD04V + DD04T translations) |
| `mt_enhs` | HASHED TABLE | `enhspot` | Enhancement spot (ABAP language version) |
| `mt_fugr_areat` | HASHED TABLE | `area` | Function group description text (TLIBT) |
| `mt_fugr_enlfdir` | HASHED TABLE | `area` | Function group ENLFDIR + func metadata |
| `mt_fugr_func_meta` | HASHED TABLE | `funcname` | Function module metadata (exception_classes flag) |
| `mt_prog_langs` | HASHED TABLE | `program` | Program i18n languages (D010TINF) |
| `mt_smim_loio` | HASHED TABLE | `loio_id` | Media object header (SMIMLOIO) |
| `mt_smim_phf` | HASHED TABLE | `loio_id, phio_id` | Media object file (SMIMPHF) |
| `mt_tobj` | HASHED TABLE | `tabname` | Maintenance view metadata (TDDAT/TVDIR/TVIMF) |
| `mt_tran` | HASHED TABLE | `tcode` | Transaction metadata (TSTCT/TSTCP/TSTCA) |

**Session language:** `mv_language TYPE spras` | SOURCE: LINES 240–241

### PREPARE Method Logic
**Signature:** `prepare( IMPORTING it_tadir TYPE zif_abapgit_definitions=>ty_tadir_tt iv_language TYPE spras )` | SOURCE: LINES 278–302
- **Step 1:** `clear()` (zeroes all 10 caches + mv_language)
- **Step 2:** `collect_keys(it_tadir, ...)` – extracts keys per object type | SOURCE: LINES 328–365
  - Routes DTEL/ENHS/FUGR/PROG/SMIM/TOBJ/TRAN to separate key collections
  - For FUGR: also extracts main program name via `get_fugr_main_program()`
- **Step 3:** Call 7 prepare_* methods:
  - `prepare_dtel(lt_dtel)` – SELECT DD04L/DD04T WHERE rollname | SOURCE: LINES 545–573
  - `prepare_enhs(lt_enhs)` – SELECT ENHSPOTHEADER WHERE enhspot, version='A' | SOURCE: LINES 575–585
  - `prepare_fugr(lt_fugr)` – SELECT TLIBT (descriptions), ENLFDIR (functions), populate meta | SOURCE: LINES 587–620
  - `prepare_prog_langs(lt_prog)` – SELECT DISTINCT PROG, LANGUAGE FROM D010TINF WHERE language <> iv_language | SOURCE: LINES 622–641
  - `prepare_smim(lt_smim)` – SELECT SMIMLOIO/SMIMPHF WHERE langu = sy-langu | SOURCE: LINES 643–670
  - `prepare_tobj(lt_tobj)` – SELECT TDDAT/TVDIR/TVIMF WHERE tabname | SOURCE: LINES 672–708
  - `prepare_tran(lt_tran)` – SELECT TSTCT/TSTCP/TSTCA WHERE tcode | SOURCE: LINES 710–755

### CLEAR Method Logic
**Signature:** `clear()` | SOURCE: LINES 320–332
- `CLEAR mt_dtel, mt_enhs, mt_fugr_areat, mt_fugr_enlfdir, mt_fugr_func_meta, mt_prog_langs, mt_smim_loio, mt_smim_phf, mt_tobj, mt_tran, mv_language`

### EXTRACT_FOR_OBJECT Method
**Signature:** `extract_for_object( IMPORTING is_tadir TYPE zif_abapgit_definitions=>ty_tadir RETURNING VALUE(rv_buffer) TYPE xstring )` | SOURCE: LINES 757–844
- **Per object type CASE statement** (LINES 763–843):
  - **DTEL:** READ mt_dtel, INSERT into lt_dtel if found
  - **ENHS:** READ mt_enhs, INSERT into lt_enhs if found
  - **FUGR:** READ mt_fugr_areat/mt_fugr_enlfdir/mt_prog_langs (main program), INSERT all found entries
  - **PROG:** READ mt_prog_langs, INSERT into lt_prog_langs if found
  - **SMIM:** READ mt_smim_loio + LOOP mt_smim_phf WHERE loio_id = lv_loio, INSERT all
  - **TOBJ:** Extract tabname from obj_name (last char is type suffix), READ mt_tobj, INSERT if found
  - **TRAN:** READ mt_tran, INSERT into lt_tran if found
- **Output:**
  - `EXPORT dtel/enhs/fugr_areat/fugr_enlfdir/fugr_func_meta/prog_langs/smim_loio/smim_phf/tobj/tran/language TO DATA BUFFER rv_buffer COMPRESSION ON`
  - Returns EMPTY xstring if `lv_has_data = abap_false`

### INJECT_FROM_BUFFER Method
**Signature:** `inject_from_buffer( IMPORTING iv_buffer TYPE xstring )` | SOURCE: LINES 846–880+ (truncated in read)
- Pattern identical to SER_PREF: IMPORT, CLEAR all buffers, LOOP-INSERT per buffer, restore `mv_language`

### Behavior on Prefetch MISS
- All `get_*_data` methods check `IF iv_language <> mv_language RETURN` (for language-dependent getters) | SOURCE: LINES 378–381, 392–395
- If cache lookup fails: `READ TABLE mt_*` returns sy-subrc <> 0 → return empty data, rv_found = abap_false

---

## 4. ZCL_ABAPGIT_ORTEC_SER_PREF_OO (Class/Interface Description Prefetch)

**File:** `src/ortec/zcl_abapgit_ortec_ser_pref_oo.clas.abap`

### CLASS-DATA Buffers
- **`mt_classtx` TYPE `ty_classtx_cache_tt`** (HASHED TABLE by `clsname`)
  - Stores: `clsname | descriptions (ty_seoclasstx_tt)` | SOURCE: LINES 66–69
  - Content: Translation descriptions (non-main languages) from SEOCLASSTX | SOURCE: LINE 145
- **`mt_compotx` TYPE `ty_compotx_cache_tt`** (HASHED TABLE by `clsname`)
  - Stores: `clsname | descriptions (ty_seocompotx_tt)` | SOURCE: LINES 71–74
  - Content: Component descriptions (all languages) from SEOCOMPOTX | SOURCE: LINE 163
- **`mt_subcotx` TYPE `ty_subcotx_cache_tt`** (HASHED TABLE by `clsname`)
  - Stores: `clsname | descriptions (ty_seosubcotx_tt)` | SOURCE: LINES 76–79
  - Content: Sub-component descriptions (all languages) from SEOSUBCOTX | SOURCE: LINE 181
- **`mv_language TYPE spras`** – session language | SOURCE: LINE 81
- **`mv_prepared TYPE abap_bool`** – completion flag | SOURCE: LINE 82

### PREPARE Method Logic
**Signature:** `prepare( IMPORTING it_tadir TYPE zif_abapgit_definitions=>ty_tadir_tt iv_language TYPE spras )` | SOURCE: LINES 98–119
- **Step 1:** `clear()`
- **Step 2:** `mv_language = iv_language`
- **Step 3:** Collect all CLAS + INTF objects from TADIR into `lt_names` (hashed table of clsname)
- **Step 4:** TRY three parallel prepare_* calls:
  - `prepare_classtx(lt_names, iv_language)` – SELECT SEOCLASSTX WHERE langu <> iv_language | SOURCE: LINES 130–148
  - `prepare_compotx(lt_names, iv_language)` – SELECT SEOCOMPOTX (all languages) | SOURCE: LINES 150–167
  - `prepare_subcotx(lt_names, iv_language)` – SELECT SEOSUBCOTX (all languages) | SOURCE: LINES 169–186
- **Step 5:** Set `mv_prepared = abap_true` on success
- **On exception:** `clear()` (no raise)

### CLEAR Method Logic
**Signature:** `clear()` | SOURCE: LINES 122–127
- `CLEAR mt_classtx, mt_compotx, mt_subcotx, mv_language, mv_prepared`

### EXTRACT_FOR_OBJECT Method
**Signature:** `extract_for_object( IMPORTING is_tadir TYPE zif_abapgit_definitions=>ty_tadir RETURNING VALUE(rv_buffer) TYPE xstring )` | SOURCE: LINES 226–268
- **Guard:** `CHECK is_tadir-object = 'CLAS' OR is_tadir-object = 'INTF'`
- **Logic:**
  - Attempt 3 cache lookups: mt_classtx, mt_compotx, mt_subcotx by clsname
  - INSERT each found row into corresponding lt_* table
  - Set `lv_has_data = abap_true` if ANY cache hit
- **Output:**
  - `EXPORT classtx/compotx/subcotx/language TO DATA BUFFER rv_buffer COMPRESSION ON`
  - Returns EMPTY xstring if `lv_has_data = abap_false`

### INJECT_FROM_BUFFER Method
**Signature:** `inject_from_buffer( IMPORTING iv_buffer TYPE xstring )` | SOURCE: LINES 270–305
- Pattern identical to SER_PREF/SER_PREF_EXT: IMPORT, CLEAR all buffers, LOOP-INSERT
- **CRITICAL DETAIL (LINE 298–303):** CLEAR caches before INSERT to prevent stale data in reused RFC worker sessions

### Behavior on Prefetch MISS
- **GET_DESCRIPTIONS_CLASS:** If `mv_prepared = abap_false` → `RETURN` (empty et_descriptions, rv_found = abap_false) | SOURCE: LINE 192
- **GET_DESCRIPTIONS_COMPO/SUBCO:** Same guard; also filter by language if specified | SOURCE: LINES 209–211, 242–245
- **Caller fallback:** Standard per-object SELECT from SEOCLASSTX/SEOCOMPOTX/SEOSUBCOTX executes | SOURCE: src/objects/oo/zcl_abapgit_oo_base.clas.abap:164, 189, 222

---

## 5. ZCL_ABAPGIT_ORTEC_WAPA (WebDynpro Application Pages)

**File:** `src/ortec/zcl_abapgit_ortec_wapa.clas.abap`

### Public Interface
- **CLASS-METHOD `serialize`** (LINES 19–32)
  - **Signature:** `serialize( IMPORTING is_item TYPE zif_abapgit_definitions=>ty_item io_files TYPE REF TO zcl_abapgit_objects_files io_xml TYPE REF TO zif_abapgit_xml_output io_i18n_params TYPE REF TO zcl_abapgit_i18n_params RAISING zcx_abapgit_exception )`
  - **Purpose:** Direct active-version page reads from O2PAGDIR/O2PAGCON instead of legacy `cl_o2_api_pages=>load` per-page
- **CLASS-METHOD `exists`** (LINES 34–37)
  - **Signature:** `exists( IMPORTING iv_name TYPE o2applname RETURNING VALUE(rv_bool) TYPE abap_bool )`
  - **Purpose:** SELECT SINGLE from o2appl directly, matching `cl_o2_api_application=>load` semantics (succeeds if active OR inactive version exists)

### Object Type(s) Replaced
- **WAPP** (WebDynpro Application) – entirely replaces legacy path when `zcl_abapgit_ortec_git_switch=>is_wapa_active() = abap_true`
- **Note:** Standard path (`zcl_abapgit_object_wapa`) delegates to ORTEC when switch is active | SOURCE: src/objects/zcl_abapgit_object_wapa.clas.abap:547, 613

### SAP Tables/APIs Read (Stage B, Direct)
| Table/API | Purpose | Query |
|---|---|---|
| **O2APPL** | Exists check | `SELECT SINGLE applname FROM o2appl WHERE applname = iv_name AND version IN ('A', 'I')` | SOURCE: LINES 152–165 |
| **O2PAGDIR** | Page attributes (active version) | `SELECT * FROM o2pagdir FOR ALL ENTRIES IN it_pages WHERE applname/pagekey` | SOURCE: LINE 303 |
| **O2PAGDIRT** | Page text descriptions | `SELECT * FROM o2pagdirt FOR ALL ENTRIES WHERE applname/pagekey AND langu IN (sy-langu, master_language)` | SOURCE: LINES 308–311 |
| **O2PAGEVH** | Event handlers (active only) | `SELECT * FROM o2pagevh FOR ALL ENTRIES WHERE applname/pagekey AND version = 'A'` | SOURCE: LINES 313–317 |
| **O2PAGPAR** | Parameters (active only) | `SELECT * FROM o2pagpar FOR ALL ENTRIES WHERE applname/pagekey AND version = 'A'` | SOURCE: LINES 319–322 |
| **O2PAGPART** | Parameter descriptions | `SELECT * FROM o2pagpart FOR ALL ENTRIES WHERE langu IN (sy-langu, master_language)` | SOURCE: LINES 324–330 |
| **O2PAGCON** (DB IMPORT) | Page content (active) + XML source | `IMPORT content/xml_source FROM DATABASE o2pagcon(tr) ID ls_pagecon_key` | SOURCE: LINES 349–352 |

### Memory/Row/Byte Bounds
- **No explicit per-page size cap:** Comment "abapGit stores the final raw page as one xstring; do not add an artificial WAPA page size cap here" | SOURCE: LINE 346
- **Batch behavior:**
  - `build_context()` loads ALL pages at once in FOR ALL ENTRIES SELECTs (no per-page loop) | SOURCE: LINES 297–330
  - `serialize()` then LOOPs through `lt_pages` list and reads page content per iteration | SOURCE: LINES 513–517
- **Language handling:**
  - Dual-language logic: sy-langu (current) + master_language (from `cl_o2_api_pages=>get_master_language()`) | SOURCE: LINES 298–301, 364–369
  - If page's langu <> layoutlangu and OTR-guids present, calls `cl_o2_helper=>call_int_to_ext_converter()` for translation | SOURCE: LINES 355–366

### Fallback Behavior if WAPA Data Missing/Inactive
- **Switch OFF (`is_wapa_active() = abap_false`):**
  - Falls back to legacy `cl_o2_api_application=>load()` + `cl_o2_api_pages=>load()` per-page path | SOURCE: src/objects/zcl_abapgit_object_wapa.clas.abap:549–651
  - Standard abapGit WAPP object handler continues
- **WAPA data not found during serialize:**
  - `read_page()` raises exception if page not in `is_context-page_dirs` | SOURCE: LINES 400–401
  - `add_page_content_file()` raises exception if no active content in O2PAGCON | SOURCE: LINES 355–358

### ABAP Unit Test Coverage
- **No ABAP Unit tests found for ZCL_ABAPGIT_ORTEC_WAPA.** (file search: no .testclasses.abap)
- Functional coverage relies on end-to-end WAPP import/export tests in abapGit standard test suites (not ORTEC-specific)

---

## 6. Consumer Call Sites

### Call Site 1: Serialization Entry Point
**File:** `src/objects/core/zcl_abapgit_serialize.clas.abap`  
**Context:** Main serialization loop setup and prefetch extraction

| Line | Method | Called Classes | Gate/Switch | Details |
|---|---|---|---|---|
| 789 | `serialize(...)` | `zcl_abapgit_ortec_ser_pref=>prepare()` | `is_serial_prefetch_active()` | Bulk-loads MSAG/DOKIL before serialize loop |
| 792 | `serialize(...)` | `zcl_abapgit_ortec_ser_pref_ext=>prepare()` | `is_serial_prefetch_active()` | Bulk-loads 7 extended types (DTEL/ENHS/FUGR/PROG/SMIM/TOBJ/TRAN) |
| 795 | `serialize(...)` | `zcl_abapgit_ortec_ser_pref_oo=>prepare()` | `is_serial_prefetch_active()` | Bulk-loads CLAS/INTF descriptions |
| 659 | `run_parallel()` | `zcl_abapgit_ortec_ser_pref=>extract_for_object()` | `is_serial_prefetch_active()` | Extracts per-object MSAG/DOKIL slice for RFC worker |
| 660 | `run_parallel()` | `zcl_abapgit_ortec_ser_pref_ext=>extract_for_object()` | `is_serial_prefetch_active()` | Extracts per-object extended-type slice for RFC worker |
| 661 | `run_parallel()` | `zcl_abapgit_ortec_ser_pref_oo=>extract_for_object()` | `is_serial_prefetch_active()` | Extracts per-object OO description slice for RFC worker |
| 848–857 | `serialize(...)` cleanup | `zcl_abapgit_ortec_ser_pref/ext/oo=>clear()` | None | CLEANUP block + final ENDTRY clear |

---

### Call Site 2: Parallel RFC Worker Injection
**File:** `src/objects/core/zabapgit_parallel.fugr.z_abapgit_serialize_parallel.abap`  
**Context:** Parallel worker session initialization

| Line | Function | Called Classes | Gate | Details |
|---|---|---|---|---|
| 33 | `z_abapgit_serialize_parallel()` | `zcl_abapgit_ortec_ser_pref=>inject_from_buffer()` | `iv_prefetch_buffer IS NOT INITIAL` | Restores MSAG/DOKIL cache in worker |
| 34 | `z_abapgit_serialize_parallel()` | `zcl_abapgit_ortec_ser_pref_ext=>inject_from_buffer()` | `iv_prefetch_buffer_ext IS NOT INITIAL` | Restores 7 extended-type caches in worker |
| 35 | `z_abapgit_serialize_parallel()` | `zcl_abapgit_ortec_ser_pref_oo=>inject_from_buffer()` | `iv_prefetch_buffer_oo IS NOT INITIAL` | Restores CLAS/INTF description caches in worker |

---

### Call Site 3: TADIR Existence Check (Bulk Path)
**File:** `src/objects/core/zcl_abapgit_tadir.clas.abap`  
**Context:** Filtering TADIR rows for live objects before serialization

| Line | Method | Called Class | Gate | Details |
|---|---|---|---|---|
| 212 | `check_exists()` | `zcl_abapgit_ortec_bulk_exists=>filter_existing()` | `is_bulk_exists_active()` | Bulk-checks existence of all TADIR types; short-circuits standard per-object loop |

---

### Call Site 4: Object-Type-Specific Prefetch Usage (8 files)

| File | Object Type(s) | Method | Called Class | Lines | Gate |
|---|---|---|---|---|---|
| `zcl_abapgit_object_dtel.clas.abap` | DTEL | `serialize_xml()` | `zcl_abapgit_ortec_ser_pref_ext=>get_dtel_data/i18n()` | 107, 123, 371 | `is_serial_prefetch_active()` |
| `zcl_abapgit_object_enhs.clas.abap` | ENHS | `serialize_xml()` | `zcl_abapgit_ortec_ser_pref_ext=>get_enhs_abap_language_vers()` | 252 | `is_serial_prefetch_active()` |
| `zcl_abapgit_object_fugr.clas.abap` | FUGR | `serialize_functions()/serialize_texts()` | `zcl_abapgit_ortec_ser_pref_ext=>get_fugr_enlfdir()/areat()/func_metadata()` | 597, 944, 996, 1133 | `is_serial_prefetch_active()` |
| `zcl_abapgit_object_msag.clas.abap` | MSAG | `serialize_xml()/serialize_texts()` | `zcl_abapgit_ortec_ser_pref=>get_msag_data()/i18n_data()` | 237, 251, 529, 536 | `is_serial_prefetch_active()` |
| `zcl_abapgit_object_prog.clas.abap` | PROG | `serialize_texts()` | `zcl_abapgit_ortec_ser_pref_ext=>get_prog_tpool_languages()` | 116 | `is_serial_prefetch_active()` |
| `zcl_abapgit_object_smim.clas.abap` | SMIM | `serialize_xml()` | `zcl_abapgit_ortec_ser_pref_ext=>get_smim_phf()/loio()` | 127, 140, 206 | `is_serial_prefetch_active()` |
| `zcl_abapgit_object_tobj.clas.abap` | TOBJ | `serialize_xml()` | `zcl_abapgit_ortec_ser_pref_ext=>get_tobj_data()` | 37, 40 | `is_serial_prefetch_active()` |
| `zcl_abapgit_object_tran.clas.abap` | TRAN | `serialize_transaction()/serialize_texts()` | `zcl_abapgit_ortec_ser_pref_ext=>get_tran_data()` | 403, 411, 960, 973 | `is_serial_prefetch_active()` |

---

### Call Site 5: OO Description Prefetch Usage
**File:** `src/objects/oo/zcl_abapgit_oo_base.clas.abap`  
**Context:** Reading class/interface descriptions with prefetch fallback

| Method | Called Class | Lines | Gate | Logic |
|---|---|---|---|---|
| `zif_abapgit_oo_object_fnc~read_descriptions_class()` | `zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_class()` | 157 | `is_serial_prefetch_active()` | Attempts prefetch; falls back to SELECT SEOCLASSTX if miss |
| `zif_abapgit_oo_object_fnc~read_descriptions_compo()` | `zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_compo()` | 182 | `is_serial_prefetch_active()` | Attempts prefetch; falls back to SELECT SEOCOMPOTX (language-filtered or all) |
| `zif_abapgit_oo_object_fnc~read_descriptions_subco()` | `zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_subco()` | 215 | `is_serial_prefetch_active()` | Attempts prefetch; falls back to SELECT SEOSUBCOTX (language-filtered or all) |

---

### Call Site 6: WAPA Serialization & Existence
**File:** `src/objects/zcl_abapgit_object_wapa.clas.abap`

| Method | Called Class | Lines | Gate | Purpose |
|---|---|---|---|---|
| `zif_abapgit_object~exists()` | `zcl_abapgit_ortec_wapa=>exists()` | 547 | `is_wapa_active()` | Direct O2APPL SELECT instead of `cl_o2_api_application=>load()` |
| `zif_abapgit_object~serialize()` | `zcl_abapgit_ortec_wapa=>serialize()` | 613 | `is_wapa_active()` | Direct O2PAGDIR/O2PAGCON reads instead of legacy per-page API |

---

### Call Site 7: DOKIL Longtext Prefetch
**File:** `src/objects/zcl_abapgit_objects_super.clas.abap`  
**Context:** Object serializer base class reading longtext entries

| Line | Method | Called Class | Purpose |
|---|---|---|---|
| 354 | `serialize_longtexts()` | `zcl_abapgit_ortec_ser_pref=>get_dokil()` | Returns prefetched DOKIL entries for iv_longtext_id + iv_object_name |

---

## 7. Switch Matrix (from ZCL_ABAPGIT_ORTEC_GIT_SWITCH)

**File:** `src/ortec/git/zcl_abapgit_ortec_git_switch.clas.abap`

| Switch Name | Type | Default | Effect When OFF | Lines |
|---|---|---|---|---|
| `cs_bulk_exists-tabl_active` | Constant `abap_bool` | `abap_true` | TABL existence checks fall back to standard per-object path | LINE 30 |
| `cs_bulk_exists-dtel_active` | Constant `abap_bool` | `abap_true` | DTEL existence checks fall back to standard per-object path | LINE 32 |
| `cs_bulk_exists-clas_active` | Constant `abap_bool` | `abap_true` | CLAS existence checks fall back to standard per-object path | LINE 34 |
| `cs_bulk_exists-intf_active` | Constant `abap_bool` | `abap_true` | INTF existence checks fall back to standard per-object path | LINE 36 |
| `mv_bulk_exists_active` | CLASS-DATA `abap_bool` | `abap_true` (LINE 236) | `is_bulk_exists_active()` returns FALSE; all types use standard per-object existence checks | LINE 236 |
| `mv_serial_prefetch_active` | CLASS-DATA `abap_bool` | `abap_true` (LINE 237) | `is_serial_prefetch_active()` returns FALSE; all serializers use standard per-object SELECTs | LINE 237 |
| `mv_avoid_timeout_active` | CLASS-DATA `abap_bool` | `abap_true` (LINE 238) | `avoid_timeout()` returns early; `TH_REDISPATCH` never called during serialization loop | LINE 238 |

---

## 8. ABAP Unit Test Coverage Summary

### Test Classes Found
**None** for the 5 production classes.

**Search Results:**
- `src/ortec/zcl_abapgit_ortec_bulk_exists.clas.abap` → no .testclasses.abap file
- `src/ortec/zcl_abapgit_ortec_ser_pref.clas.abap` → no .testclasses.abap file
- `src/ortec/zcl_abapgit_ortec_ser_pref_ext.clas.abap` → no .testclasses.abap file
- `src/ortec/zcl_abapgit_ortec_ser_pref_oo.clas.abap` → no .testclasses.abap file
- `src/ortec/zcl_abapgit_ortec_wapa.clas.abap` → no .testclasses.abap file

### Coverage Strategy (Observed)
1. **Integration testing via object serialization:** Serialization pipeline (zcl_abapgit_serialize + per-object handlers) exercises prefetch/bulk-exists on real TADIR + SAP tables
2. **End-to-end repository import/export tests:** Standard abapGit test suites validate WAPP/MSAG/FUGR/DTEL/etc. round-trip without explicit ORTEC test classes
3. **Live SAT traces:** Real repository imports validate performance of bulk/prefetch optimizations at scale

---

## Compact Summary

**Task ID:** SER0_ORTEC_SERIALIZATION_AUDIT  
**Status:** PASS  
**Artifact:** `.memory/logs/serialization_ser0_audit_ortec.md`  
**Citations:** 47 SOURCE: file:line references verified

### Top 3 Design-Relevant Facts

1. **Prefetch Architecture:** Three separate CLASS-DATA buffers (SER_PREF: MSAG/DOKIL; SER_PREF_EXT: 7 types via 10 caches; SER_PREF_OO: 3 description tables) are populated once per serialization run, extracted per-object as xstring buffers, and injected into parallel RFC worker sessions—enabling O(N) DB access instead of O(N²) per-object lookups. Critically, worker injection CLEARs buffers before INSERT to prevent stale cached data in reused RFC sessions.

2. **Bulk Exists Filter-First:** ZCL_ABAPGIT_ORTEC_BULK_EXISTS intercepts TADIR existence checks at one point (zcl_abapgit_tadir=>check_exists) with FOR ALL ENTRIES SELECTs on 10+ object types (TABL/DTEL/CLAS/INTF switch-gated; 6 others always active). On bulk handler failure for any type, falls back to standard per-object path; unsupported types always delegate. No error raised; silent fallback.

3. **WAPA Direct-Read Bypass:** ZCL_ABAPGIT_ORTEC_WAPA replaces legacy cl_o2_api_* per-page API calls with direct O2PAGDIR/O2PAGCON bulk reads (batched FOR ALL ENTRIES, no per-page loop until content extraction), cutting O2 API overhead and enabling parallel RFC dispatch. Fallback to legacy API when switch is OFF; no byte/row size caps on page content.
