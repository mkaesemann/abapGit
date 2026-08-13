"! <p class="shorttext synchronized">ORTEC FDT0 (BRF+) serialization result cache</p>
"!
"! Caches the unchanged output of zcl_abapgit_objects=>serialize for FDT0
"! (BRF+ application) TADIR objects, keyed by (application_id, signature)
"! where signature is a SHA1 over that application's full owned admin-row
"! graph in FDT_ADMN_0000S. Transparent pass-through for every other
"! object type, and for FDT0 whenever
"! ZCL_ABAPGIT_ORTEC_GIT_SWITCH=>IS_FDT0_CACHE_ACTIVE( ) is off (default).
"! See .memory/logs/fdt0_local_cache_design.md - AC-07 IT-01 is a literal,
"! non-waivable stop condition: the cache must not be enabled outside a
"! test/dev session until a live signature-change proof has been run.
CLASS zcl_abapgit_ortec_fdt0_cache DEFINITION
  PUBLIC
  FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.
    "! 50 MB - a stored payload above this size is never cached; the real,
    "! already-successful serialization result is still returned unchanged.
    CONSTANTS c_max_cache_payload_bytes TYPE i VALUE 52428800.
    "! 48 MB content cap leaves headroom for EXPORT's metadata/compression.
    CONSTANTS c_max_cache_content_bytes TYPE i VALUE 50331648.
    "! Defensive ceiling on COMPUTE_SIGNATURE's own graph SELECT (real
    "! observed maximum on IT8 is 5179 rows for the largest application).
    CONSTANTS c_max_signature_rows TYPE i VALUE 200000.

    "! Drop-in replacement for zcl_abapgit_objects=>serialize at both
    "! ORTEC interception anchors. Byte-identical pass-through unless the
    "! object is FDT0 and the cache feature flag is active.
    CLASS-METHODS serialize
      IMPORTING
        !is_item                TYPE zif_abapgit_definitions=>ty_item
        !io_i18n_params         TYPE REF TO zcl_abapgit_i18n_params
      RETURNING
        VALUE(rs_serialization) TYPE zif_abapgit_objects=>ty_serialization
      RAISING
        zcx_abapgit_exception.

  PROTECTED SECTION.
  PRIVATE SECTION.
    TYPES: BEGIN OF ty_sig_row,
             id           TYPE fdt_admn_0000s-id,
             object_type  TYPE fdt_admn_0000s-object_type,
             version      TYPE fdt_admn_0000s-version,
             ch_timestamp TYPE fdt_admn_0000s-ch_timestamp,
             deleted      TYPE fdt_admn_0000s-deleted,
             tv_state     TYPE fdt_admn_0000s-tv_state,
             tv_timestamp TYPE fdt_admn_0000s-tv_timestamp,
             obsolete     TYPE fdt_admn_0000s-obsolete,
           END OF ty_sig_row.

    "! Mirrors ZCL_ABAPGIT_OBJECT_FDT0's own private GET_APPLICATION_ID
    "! lookup (that method is PRIVATE on a standard class and out of
    "! scope to touch). Initial result means "cache not usable".
    CLASS-METHODS resolve_application_id
      IMPORTING
        !iv_obj_name              TYPE sobj_name
      RETURNING
        VALUE(rv_application_id) TYPE fdt_admn_0000s-application_id.

    "! Full-graph signature over every FDT_ADMN_0000S row sharing
    "! iv_application_id (including the application's own row). Initial
    "! result means "cache not usable" (empty or oversized graph, or a
    "! sha1_string failure) - never raises.
    CLASS-METHODS compute_signature
      IMPORTING
        !iv_application_id TYPE fdt_admn_0000s-application_id
      RETURNING
        VALUE(rv_signature) TYPE char40.

    "! Cache read. Self-heals (deletes) a corrupt or zero-file stored row
    "! and reports it as a miss rather than surfacing an error.
    CLASS-METHODS try_read
      IMPORTING
        !iv_application_id TYPE fdt_admn_0000s-application_id
        !iv_signature      TYPE char40
      EXPORTING
        !es_serialization  TYPE zif_abapgit_objects=>ty_serialization
      RETURNING
        VALUE(rv_found)    TYPE abap_bool.

    "! Cache write (best-effort, never raises). Upserts the current
    "! (application_id, signature) row and purges every other row left
    "! behind for this application_id under a now-superseded signature -
    "! PERF-DESIGN-001: bounds the table to at most one live row per
    "! application for the life of the system.
    CLASS-METHODS store
      IMPORTING
        !iv_application_id TYPE fdt_admn_0000s-application_id
        !iv_signature      TYPE char40
        !iv_obj_name       TYPE sobj_name
        !is_serialization  TYPE zif_abapgit_objects=>ty_serialization.

    CLASS-METHODS get_timestamp
      RETURNING VALUE(rv_ts) TYPE tzntstmpl.

    CLASS-METHODS get_serialization_bytes
      IMPORTING is_serialization TYPE zif_abapgit_objects=>ty_serialization
      RETURNING VALUE(rv_bytes) TYPE int8.
ENDCLASS.



CLASS zcl_abapgit_ortec_fdt0_cache IMPLEMENTATION.

  METHOD serialize.
    DATA lv_application_id TYPE fdt_admn_0000s-application_id.
    DATA lv_signature      TYPE char40.
    DATA lv_found          TYPE abap_bool.

    IF is_item-obj_type <> 'FDT0' OR zcl_abapgit_ortec_git_switch=>is_fdt0_cache_active( ) = abap_false.
      rs_serialization = zcl_abapgit_objects=>serialize(
        is_item        = is_item
        io_i18n_params = io_i18n_params ).
      RETURN.
    ENDIF.

    lv_application_id = resolve_application_id( is_item-obj_name ).
    IF lv_application_id IS NOT INITIAL.
      lv_signature = compute_signature( lv_application_id ).
    ENDIF.

    IF lv_application_id IS NOT INITIAL AND lv_signature IS NOT INITIAL.
      lv_found = try_read(
        EXPORTING
          iv_application_id = lv_application_id
          iv_signature      = lv_signature
        IMPORTING
          es_serialization  = rs_serialization ).
      IF lv_found = abap_true.
        RETURN.
      ENDIF.
    ENDIF.

    rs_serialization = zcl_abapgit_objects=>serialize(
      is_item        = is_item
      io_i18n_params = io_i18n_params ).

    IF lv_application_id IS NOT INITIAL AND lv_signature IS NOT INITIAL.
      store(
        iv_application_id = lv_application_id
        iv_signature      = lv_signature
        iv_obj_name       = is_item-obj_name
        is_serialization  = rs_serialization ).
    ENDIF.
  ENDMETHOD.

  METHOD resolve_application_id.
    CLEAR rv_application_id.
    SELECT SINGLE application_id FROM fdt_admn_0000s INTO @rv_application_id
      WHERE object_type = 'AP'
        AND name         = @iv_obj_name.
  ENDMETHOD.

  METHOD compute_signature.
    DATA lt_rows   TYPE STANDARD TABLE OF ty_sig_row WITH EMPTY KEY.
    DATA lt_parts  TYPE STANDARD TABLE OF string WITH EMPTY KEY.
    DATA lv_concat TYPE string.
    DATA lv_max_rows TYPE i.

    CLEAR rv_signature.
    lv_max_rows = c_max_signature_rows + 1.

    SELECT id, object_type, version, ch_timestamp, deleted, tv_state, tv_timestamp, obsolete
      FROM fdt_admn_0000s
      WHERE application_id = @iv_application_id
      ORDER BY id
      INTO TABLE @lt_rows
      UP TO @lv_max_rows ROWS.

    IF lt_rows IS INITIAL OR lines( lt_rows ) > c_max_signature_rows.
      RETURN.
    ENDIF.

    LOOP AT lt_rows INTO DATA(ls_row).
      APPEND |{ ls_row-id };{ ls_row-object_type };{ ls_row-version };| &&
             |{ ls_row-ch_timestamp };{ ls_row-deleted };{ ls_row-tv_state };| &&
             |{ ls_row-tv_timestamp };{ ls_row-obsolete }| TO lt_parts.
    ENDLOOP.
    CONCATENATE LINES OF lt_parts INTO lv_concat
      SEPARATED BY cl_abap_char_utilities=>newline.

    TRY.
        rv_signature = to_upper( zcl_abapgit_hash=>sha1_string( lv_concat ) ).
      CATCH zcx_abapgit_exception.
        CLEAR rv_signature.
    ENDTRY.
  ENDMETHOD.

  METHOD try_read.
    DATA lv_payload TYPE zaog_fdt_cache-payload.
    DATA lv_now     TYPE tzntstmpl.

    CLEAR es_serialization.
    rv_found = abap_false.

    SELECT SINGLE payload FROM zaog_fdt_cache INTO @lv_payload
      WHERE application_id = @iv_application_id
        AND signature      = @iv_signature.
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    TRY.
        IMPORT data = es_serialization FROM DATA BUFFER lv_payload.
      CATCH cx_sy_import_format_error cx_sy_import_mismatch_error
            cx_sy_compression_error cx_sy_conversion_codepage.
        DELETE FROM zaog_fdt_cache
          WHERE application_id = @iv_application_id
            AND signature      = @iv_signature.
        CLEAR es_serialization.
        RETURN.
    ENDTRY.

    IF es_serialization-files IS INITIAL.
      DELETE FROM zaog_fdt_cache
        WHERE application_id = @iv_application_id
          AND signature      = @iv_signature.
      CLEAR es_serialization.
      RETURN.
    ENDIF.

    rv_found = abap_true.

    lv_now = get_timestamp( ).
    UPDATE zaog_fdt_cache SET last_used_at = @lv_now
      WHERE application_id = @iv_application_id
        AND signature      = @iv_signature.
  ENDMETHOD.

  METHOD store.
    DATA lv_payload TYPE zaog_fdt_cache-payload.
    DATA ls_row     TYPE zaog_fdt_cache.

    IF get_serialization_bytes( is_serialization ) > c_max_cache_content_bytes.
      RETURN.
    ENDIF.

    EXPORT data = is_serialization TO DATA BUFFER lv_payload.

    IF xstrlen( lv_payload ) > c_max_cache_payload_bytes.
      RETURN.
    ENDIF.

    ls_row-application_id = iv_application_id.
    ls_row-signature       = iv_signature.
    ls_row-obj_name        = iv_obj_name.
    ls_row-payload         = lv_payload.
    ls_row-payload_size    = xstrlen( lv_payload ).
    ls_row-created_at      = get_timestamp( ).
    ls_row-last_used_at    = ls_row-created_at.

    MODIFY zaog_fdt_cache FROM ls_row.
    IF sy-subrc = 0.
      " PERF-DESIGN-001: only the current signature is ever useful for a
      " given application - purge every row left behind under an older,
      " now-superseded signature so the table stays bounded to one live
      " row per application for the life of the system.
      DELETE FROM zaog_fdt_cache
        WHERE application_id = @iv_application_id
          AND signature      <> @iv_signature.
    ENDIF.
  ENDMETHOD.


  METHOD get_serialization_bytes.
    LOOP AT is_serialization-files INTO DATA(ls_file).
      rv_bytes = rv_bytes + xstrlen( ls_file-data ).
      IF rv_bytes > c_max_cache_content_bytes.
        RETURN.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.

  METHOD get_timestamp.
    GET TIME STAMP FIELD rv_ts.
  ENDMETHOD.

ENDCLASS.
