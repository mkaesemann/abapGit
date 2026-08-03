# SER0_STANDARD_SERIALIZATION_AUDIT

## Baseline
- HEAD: 8b56ffb802fca0a4f619088657d84c35949714c4
- Branch: ortec/abapgit_1_133-opt-rework

## 1) Exact current sequential vs parallel serialization contract

### 1.1 run_sequential per object
- SOURCE_CONFIRMED: [src/objects/core/zcl_abapgit_serialize.clas.abap](src/objects/core/zcl_abapgit_serialize.clas.abap#L709-L746)
- Loop body: builds a single `ls_file_item-item` from the TADIR row, fills `obj_type`, `obj_name`, `devclass`, `srcsystem`, `origlang`, and optional ABAP language version, then calls `zcl_abapgit_objects=>serialize(...)` and `add_to_return(...)`.
- Exact calls inside the method: `zcl_abapgit_objects=>serialize(...)` and `add_to_return(...)`.
- Error handling: catches `zcx_abapgit_exception` and logs via `mi_log->add_exception(...)`.

### 1.2 run_parallel per object
- SOURCE_CONFIRMED: [src/objects/core/zcl_abapgit_serialize.clas.abap](src/objects/core/zcl_abapgit_serialize.clas.abap#L635-L707)
- Task naming: `lv_task = |{ iv_task }-{ sy-index }|`.
- RFC contract: `CALL FUNCTION 'Z_ABAPGIT_SERIALIZE_PARALLEL' STARTING NEW TASK lv_task DESTINATION IN GROUP mv_group CALLING on_end_of_task ON END OF TASK ...`.
- Parameters passed into the RFC worker: `is_tadir`, `iv_abap_language_vers`, `iv_language`, `iv_path`, `iv_main_language_only`, `iv_suppress_po_comments`, `it_translation_langs`, `iv_use_lxe`, `iv_prefetch_buffer`, `iv_prefetch_buffer_ext`, `iv_prefetch_buffer_oo`.
- Callback contract: `on_end_of_task` receives `ev_result` and `ev_path` via `RECEIVE RESULTS FROM FUNCTION 'Z_ABAPGIT_SERIALIZE_PARALLEL' IMPORTING ev_result = lv_result ev_path = lv_path ...` and then imports the `xstring` payload back into `ls_file_item` with `IMPORT data = ls_file_item FROM DATA BUFFER lv_result` before calling `add_to_return(...)`.
- One object per RFC task: SOURCE_CONFIRMED yes; the worker FM input is a single `TADIR` row and one object is serialized inside the worker via `zcl_abapgit_objects=>serialize( is_item = ls_item ... )`.

### 1.3 Exact signature of Z_ABAPGIT_SERIALIZE_PARALLEL
- SOURCE_CONFIRMED: [src/objects/core/zabapgit_parallel.fugr.z_abapgit_serialize_parallel.abap](src/objects/core/zabapgit_parallel.fugr.z_abapgit_serialize_parallel.abap#L1-L18)
- Importing parameters: `IS_TADIR`, `IV_ABAP_LANGUAGE_VERS`, `IV_LANGUAGE`, `IV_PATH`, `IV_MAIN_LANGUAGE_ONLY`, `IV_SUPPRESS_PO_COMMENTS`, `IT_TRANSLATION_LANGS`, `IV_USE_LXE`, `IV_PREFETCH_BUFFER`, `IV_PREFETCH_BUFFER_EXT`, `IV_PREFETCH_BUFFER_OO`.
- Exporting parameters: `EV_RESULT`, `EV_PATH`.
- Exceptions: `ERROR`.
- Capacity: strictly single-object; it accepts one `TADIR` row and serializes one object.

### 1.4 Worker-count / server-group controls
- SOURCE_CONFIRMED: [src/objects/core/zcl_abapgit_serialize.clas.abap](src/objects/core/zcl_abapgit_serialize.clas.abap#L367-L385) and [src/objects/core/zcl_abapgit_serialize.clas.abap](src/objects/core/zcl_abapgit_serialize.clas.abap#L305-L335)
- RFC server group comes from `determine_rfc_server_group`, which defaults to `'parallel_generators'` and can be changed via exit hook, then reset to `''` if the server group does not exist.
- Worker count is determined by `determine_max_processes`, which calls `zcl_abapgit_factory=>get_environment( )->init_parallel_processing( mv_group )` and caps the count to 50, then allows an exit hook to alter the final value.
- The class-level `gv_max_processes` holds the computed worker count.

### 1.5 ON_END_OF_TASK failure handling
- SOURCE_CONFIRMED: [src/objects/core/zcl_abapgit_serialize.clas.abap](src/objects/core/zcl_abapgit_serialize.clas.abap#L586-L633)
- It imports `ev_result` and `ev_path` from the completed task.
- On `sy-subrc <> 0`, it logs the RFC communication/system failure message (special-casing the GUI-session limit message), adds a warning that serialization is falling back to sequential for remaining objects, and does not add the object to the return collection.
- On success, it imports the xstring payload back into `ls_file_item` and calls `add_to_return(...)`.

## 2) Exact existing TADIR discovery and filtering order
- SOURCE_CONFIRMED: [src/objects/core/zcl_abapgit_tadir.clas.abap](src/objects/core/zcl_abapgit_tadir.clas.abap#L167-L201), [src/objects/core/zcl_abapgit_tadir.clas.abap](src/objects/core/zcl_abapgit_tadir.clas.abap#L273-L329), and [src/objects/core/zcl_abapgit_tadir.clas.abap](src/objects/core/zcl_abapgit_tadir.clas.abap#L466-L517)
- `build` calls `select_objects(...)`, then `add_local_packages(...)`, `add_namespaces(...)`, `determine_path(...)`.
- `select_objects` builds `et_packages` from subpackages (unless `iv_ignore_subpackages`), prepends the package itself, excludes several object types (`SOTR`, `SOTS`, `SFB1`, `SFB2`, `STOB`), limits to local objects when requested, limits to non-deleted objects when requested, then executes a single `SELECT * FROM tadir ... FOR ALL ENTRIES IN et_packages WHERE devclass = et_packages-table_line AND pgmid = 'R3TR' AND object NOT IN lt_excludes AND delflag IN lt_delflag AND srcsystem IN lt_srcsystem ORDER BY PRIMARY KEY`.
- `zif_abapgit_tadir~read` applies an in-memory filter from `it_filter`, then calls `check_exists(...)` if `iv_check_exists = abap_true`.
- `check_exists` uses the ORTEC bulk-exists hook when active; otherwise it loops over each row and calls `zcl_abapgit_objects=>exists(...)` per row.

## 3) Exact CLAS/INTF/DTEL/DOMA EXISTS() and SERIALIZE() call chains at a “which methods call which” level

### 3.1 Dispatch entry points
- SOURCE_CONFIRMED: [src/objects/zcl_abapgit_objects.clas.abap](src/objects/zcl_abapgit_objects.clas.abap#L395-L451) and [src/objects/zcl_abapgit_objects.clas.abap](src/objects/zcl_abapgit_objects.clas.abap#L964-L986)
- `zcl_abapgit_objects=>create_object(...)` resolves a class name from the object type, creates the concrete handler (`ZCL_ABAPGIT_OBJECT_<TYPE>` or `ZCL_ABAPGIT_OBJECTS_BRIDGE`), and returns `zif_abapgit_object`.
- `zcl_abapgit_objects=>exists(...)` calls `create_object(...)` and then `li_obj->exists(...)`.
- `zcl_abapgit_objects=>is_supported(...)` calls `create_object(...)` and checks whether it raises `zcx_abapgit_type_not_supported`.
- `zcl_abapgit_objects=>changed_by(...)` calls `create_object(...)` and then `li_obj->changed_by(...)`.

### 3.2 CLAS-specific serialization chain
- SOURCE_CONFIRMED: [src/objects/zcl_abapgit_object_clas.clas.abap](src/objects/zcl_abapgit_object_clas.clas.abap#L158-L168), [src/objects/zcl_abapgit_object_clas.clas.abap](src/objects/zcl_abapgit_object_clas.clas.abap#L748-L816), [src/objects/zcl_abapgit_object_clas.clas.abap](src/objects/zcl_abapgit_object_clas.clas.abap#L1057-L1118), and [src/objects/oo/zcl_abapgit_oo_serializer.clas.abap](src/objects/oo/zcl_abapgit_oo_serializer.clas.abap#L217-L340)
- `zcl_abapgit_object_clas~serialize` calls `zif_abapgit_object~exists`, then `mi_object_oriented_object_fct->serialize_abap(...)` for the main source, locals definitions/imports/testclasses/macros, then `serialize_xml(...)`.
- `serialize_xml` calls `mi_object_oriented_object_fct->get_class_properties(...)`, then `serialize_tpool(...)`, `serialize_tpool_i18n(...)`, `serialize_sotr(...)`, `serialize_docu(...)`, `serialize_descr_class(...)`, `serialize_descr_compo(...)`, `serialize_descr_subco(...)`, and `serialize_attr(...)`.
- The OO layer entry point is the factory: [src/objects/oo/zcl_abapgit_oo_class.clas.abap](src/objects/oo/zcl_abapgit_oo_class.clas.abap#L857-L892) and [src/objects/oo/zcl_abapgit_oo_class.clas.abap](src/objects/oo/zcl_abapgit_oo_class.clas.abap#L892-L929) for the `get_class_properties` / `get_includes` methods.
- The OO serializer itself calls `CL_OO_FACTORY` / `CL_OO_SOURCE`-style dynamic APIs in `serialize_abap_new(...)` / `serialize_abap_old(...)` and reads includes via the report API in `read_include(...)`.

### 3.3 Tables / OO APIs touched for CLAS serialization
- SOURCE_CONFIRMED: [src/objects/zcl_abapgit_object_clas.clas.abap](src/objects/zcl_abapgit_object_clas.clas.abap#L748-L816), [src/objects/oo/zcl_abapgit_oo_base.clas.abap](src/objects/oo/zcl_abapgit_oo_base.clas.abap#L153-L239), [src/objects/zcl_abapgit_object_clas.clas.abap](src/objects/zcl_abapgit_object_clas.clas.abap#L892-L926), and [src/objects/zcl_abapgit_object_clas.clas.abap](src/objects/zcl_abapgit_object_clas.clas.abap#L1057-L1118)
- DB/metadata tables touched in the `CLAS` path include `d010tinf`, `dokhl`, `reposrc`, `seoclasstx`, `seocompotx`, `seosubcotx`, `seocompodf`, and `seometarel` (via APACK replacement) plus the OO runtime APIs `cl_oo_classname_service`, `cl_oo_factory` / `cl_oo_source`-style access, and the report/include API through `zcl_abapgit_factory=>get_sap_report( )->read_report(...)`.
- The CLAS existence path uses `mi_object_oriented_object_fct->exists(...)` and `mi_object_oriented_object_fct->read_superclass(...)`.

## 4) ORTEC hook points already wired in today
- SOURCE_CONFIRMED: [src/objects/core/zcl_abapgit_serialize.clas.abap](src/objects/core/zcl_abapgit_serialize.clas.abap#L659-L665), [src/objects/core/zcl_abapgit_serialize.clas.abap](src/objects/core/zcl_abapgit_serialize.clas.abap#L770-L857), [src/objects/core/zabapgit_parallel.fugr.z_abapgit_serialize_parallel.abap](src/objects/core/zabapgit_parallel.fugr.z_abapgit_serialize_parallel.abap#L33-L36), [src/objects/core/zcl_abapgit_tadir.clas.abap](src/objects/core/zcl_abapgit_tadir.clas.abap#L203-L214), [src/objects/oo/zcl_abapgit_oo_base.clas.abap](src/objects/oo/zcl_abapgit_oo_base.clas.abap#L153-L239), [src/objects/zcl_abapgit_objects_super.clas.abap](src/objects/zcl_abapgit_objects_super.clas.abap#L347-L360), and [src/objects/zcl_abapgit_object_dtel.clas.abap](src/objects/zcl_abapgit_object_dtel.clas.abap#L370-L371) (for the broader ORTEC prefetch pattern).
- Exact hook sites in the standard serialization stack today are the prefetch and bulk-exists hooks: `zcl_abapgit_ortec_ser_pref*`, `zcl_abapgit_ortec_ser_pref_oo`, `zcl_abapgit_ortec_git_switch`, and `zcl_abapgit_ortec_bulk_exists`.
- There are no ORTEC call sites in [src/objects/zcl_abapgit_objects.clas.abap](src/objects/zcl_abapgit_objects.clas.abap) itself; the standard object-dispatch class is not currently wired with ORTEC-specific hooks.

## 5) Existing per-object cost estimation / ordering logic
- SOURCE_CONFIRMED: NO. [src/objects/core/zcl_abapgit_serialize.clas.abap](src/objects/core/zcl_abapgit_serialize.clas.abap#L749-L860) shows no per-object cost estimation, no size-based ordering, and no object-weight sort; it simply loops over the TADIR rows in the order already returned by `zcl_abapgit_tadir` and either runs sequentially or spawns a parallel task for each row.
- The TADIR discovery itself is ordered by `devclass pgmid object obj_name` in [src/objects/core/zcl_abapgit_tadir.clas.abap](src/objects/core/zcl_abapgit_tadir.clas.abap#L326-L329), but there is no per-object weighting/size heuristic in the standard serialization path.
