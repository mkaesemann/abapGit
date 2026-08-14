CLASS zcl_abapgit_ortec_ssfo_cache DEFINITION
  PUBLIC
  FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.
    CLASS-METHODS serialize
      IMPORTING
                is_item                 TYPE zif_abapgit_definitions=>ty_item
                is_i18n_params          TYPE zif_abapgit_definitions=>ty_i18n_params
      RETURNING VALUE(rs_serialization) TYPE zif_abapgit_objects=>ty_serialization
      RAISING   zcx_abapgit_exception.
  PROTECTED SECTION.
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

    TYPES: BEGIN OF ty_payload,
             payload_version  TYPE i,
             formname         TYPE tdsfname,
             form_language    TYPE spras,
             context_hash     TYPE char40,
             source_signature TYPE char40,
             serialization    TYPE zif_abapgit_objects=>ty_serialization,
           END OF ty_payload.

    CLASS-METHODS resolve_effective_language
      IMPORTING iv_formname TYPE tdsfname
      EXPORTING eo_form     TYPE REF TO cl_ssf_fb_smart_form
                ev_language TYPE sylangu
      RAISING   cx_ssf_fb.
    CLASS-METHODS compute_context_hash
      IMPORTING is_item        TYPE zif_abapgit_definitions=>ty_item
                is_i18n_params TYPE zif_abapgit_definitions=>ty_i18n_params
                iv_language    TYPE sylangu
      RETURNING VALUE(rv_hash) TYPE char40.
    CLASS-METHODS compute_active_signature
      IMPORTING iv_formname         TYPE tdsfname
                iv_language         TYPE spras
      EXPORTING ev_row_count        TYPE i
                ev_input_bytes      TYPE int8
      RETURNING VALUE(rv_signature) TYPE char40.
    CLASS-METHODS try_read
      IMPORTING iv_formname         TYPE tdsfname
                iv_language         TYPE spras
                iv_context_hash     TYPE char40
                iv_source_signature TYPE char40
                is_item             TYPE zif_abapgit_definitions=>ty_item
      EXPORTING es_serialization    TYPE zif_abapgit_objects=>ty_serialization
      RETURNING VALUE(rv_found)     TYPE abap_bool.
    CLASS-METHODS store
      IMPORTING iv_formname         TYPE tdsfname
                iv_language         TYPE spras
                iv_context_hash     TYPE char40
                iv_source_signature TYPE char40
                is_serialization    TYPE zif_abapgit_objects=>ty_serialization.
    CLASS-METHODS purge_to_limits.
    CLASS-METHODS get_serialization_bytes
      IMPORTING is_serialization TYPE zif_abapgit_objects=>ty_serialization
      RETURNING VALUE(rv_bytes)  TYPE int8.
    CLASS-METHODS get_timestamp
      RETURNING VALUE(rv_ts) TYPE tzntstmpl.
ENDCLASS.



CLASS zcl_abapgit_ortec_ssfo_cache IMPLEMENTATION.
  METHOD serialize.
    DATA lo_form TYPE REF TO cl_ssf_fb_smart_form.
    DATA lv_language TYPE sylangu.
    DATA lv_context_hash TYPE char40.
    DATA lv_signature TYPE char40.
    DATA lv_found TYPE abap_bool.
    DATA lv_formname TYPE tdsfname.

    IF is_item-obj_type <> 'SSFO'
       OR zcl_abapgit_ortec_git_switch=>is_ssfo_cache_active( ) = abap_false.
      rs_serialization = zcl_abapgit_objects=>serialize(
        is_item        = is_item
        io_i18n_params = zcl_abapgit_i18n_params=>new( is_params = is_i18n_params ) ).
      RETURN.
    ENDIF.

    lv_formname = is_item-obj_name.

    TRY.
        resolve_effective_language(
          EXPORTING iv_formname = lv_formname
          IMPORTING eo_form     = lo_form
                    ev_language = lv_language ).
        lv_context_hash = compute_context_hash(
          is_item        = is_item
          is_i18n_params = is_i18n_params
          iv_language    = lv_language ).
        lv_signature = compute_active_signature(
          iv_formname = lv_formname
          iv_language = lv_language ).
        IF lv_context_hash IS NOT INITIAL AND lv_signature IS NOT INITIAL.
          lv_found = try_read(
            EXPORTING
              iv_formname         = lv_formname
              iv_language         = lv_language
              iv_context_hash     = lv_context_hash
              iv_source_signature = lv_signature
              is_item             = is_item
            IMPORTING es_serialization = rs_serialization ).
        ENDIF.
      CATCH cx_root.
        CLEAR lv_found.
    ENDTRY.

    IF lo_form IS BOUND.
      CALL METHOD lo_form->dequeue
        EXPORTING
          formname = lv_formname.
      FREE lo_form.
    ENDIF.

    IF lv_found = abap_true.
      RETURN.
    ENDIF.

    rs_serialization = zcl_abapgit_objects=>serialize(
      is_item        = is_item
      io_i18n_params = zcl_abapgit_i18n_params=>new( is_params = is_i18n_params ) ).

    IF lv_context_hash IS INITIAL OR lv_signature IS INITIAL.
      RETURN.
    ENDIF.

    TRY.
        resolve_effective_language(
          EXPORTING iv_formname = lv_formname
          IMPORTING eo_form     = lo_form
                    ev_language = lv_language ).
        DATA(lv_publish_context_hash) = compute_context_hash(
          is_item        = is_item
          is_i18n_params = is_i18n_params
          iv_language    = lv_language ).
        DATA(lv_publish_signature) = compute_active_signature(
          iv_formname = lv_formname
          iv_language = lv_language ).
        IF lv_publish_context_hash = lv_context_hash
           AND lv_publish_signature = lv_signature.
          store(
            iv_formname         = lv_formname
            iv_language         = lv_language
            iv_context_hash     = lv_context_hash
            iv_source_signature = lv_signature
            is_serialization    = rs_serialization ).
        ENDIF.
      CATCH cx_root.
    ENDTRY.

    IF lo_form IS BOUND.
      CALL METHOD lo_form->dequeue
        EXPORTING
          formname = lv_formname.
      FREE lo_form.
    ENDIF.
  ENDMETHOD.

  METHOD resolve_effective_language.
    CLEAR: eo_form, ev_language.
    CREATE OBJECT eo_form.
    eo_form->enqueue(
      EXPORTING
        language_upd_exit       = space
        suppress_language_check = space
        mode                    = 'SHOW'
        formname                = iv_formname
      IMPORTING modification_language = ev_language ).
    IF ev_language IS INITIAL.
      ev_language = sy-langu.
    ENDIF.
  ENDMETHOD.

  METHOD compute_context_hash.
    DATA lv_buffer TYPE xstring.
    DATA: BEGIN OF ls_context,
            payload_version TYPE i,
            mandt           TYPE mandt,
            object_type     TYPE c LENGTH 4,
            item            TYPE zif_abapgit_definitions=>ty_item,
            i18n_params     TYPE zif_abapgit_definitions=>ty_i18n_params,
            language        TYPE sylangu,
          END OF ls_context.

    IF is_item-obj_name IS INITIAL OR is_item-obj_type <> 'SSFO'
       OR sy-mandt IS INITIAL OR iv_language IS INITIAL.
      RETURN.
    ENDIF.
    ls_context-payload_version = c_payload_version.
    ls_context-mandt           = sy-mandt.
    ls_context-object_type     = 'SSFO'.
    ls_context-item            = is_item.
    ls_context-i18n_params     = is_i18n_params.
    ls_context-language        = iv_language.
    TRY.
        EXPORT context = ls_context TO DATA BUFFER lv_buffer.
        rv_hash = to_upper( zcl_abapgit_hash=>sha1_blob( lv_buffer ) ).
      CATCH cx_root.
        CLEAR rv_hash.
    ENDTRY.
  ENDMETHOD.

  METHOD compute_active_signature.
    TYPES ty_stxfcont_tt TYPE STANDARD TABLE OF stxfcont WITH EMPTY KEY.
    TYPES ty_stxfobjt_tt TYPE STANDARD TABLE OF stxfobjt WITH EMPTY KEY.
    TYPES ty_stxftxt_tt TYPE STANDARD TABLE OF stxftxt WITH EMPTY KEY.
    TYPES ty_stxfadmt_tt TYPE STANDARD TABLE OF stxfadmt WITH EMPTY KEY.
    TYPES ty_stxfvart_tt TYPE STANDARD TABLE OF stxfvart WITH EMPTY KEY.
    DATA ls_adm TYPE stxfadm.
    DATA lt_cont TYPE ty_stxfcont_tt.
    DATA lt_objt TYPE ty_stxfobjt_tt.
    DATA lt_txt TYPE ty_stxftxt_tt.
    DATA lt_admt TYPE ty_stxfadmt_tt.
    DATA lt_vart TYPE ty_stxfvart_tt.
    DATA lv_saved_relid TYPE stxfconts-relid.
    DATA lv_limit TYPE i.
    DATA lv_buffer TYPE xstring.
    DATA: BEGIN OF ls_signature,
            marker   TYPE string,
            mandt    TYPE mandt,
            formname TYPE tdsfname,
            language TYPE spras,
            adm      TYPE stxfadm,
            cont     TYPE ty_stxfcont_tt,
            objt     TYPE ty_stxfobjt_tt,
            txt      TYPE ty_stxftxt_tt,
            admt     TYPE ty_stxfadmt_tt,
            vart     TYPE ty_stxfvart_tt,
          END OF ls_signature.

    CLEAR: rv_signature, ev_row_count, ev_input_bytes.
    TRY.
        SELECT SINGLE * FROM stxfadm INTO @ls_adm
          WHERE formname = @iv_formname.
        IF sy-subrc <> 0.
          RETURN.
        ENDIF.
        SELECT SINGLE relid FROM stxfconts INTO @lv_saved_relid
          WHERE relid = 'XX' AND formname = @iv_formname.
        IF sy-subrc = 0.
          RETURN.
        ENDIF.

        lv_limit = c_max_signature_rows + 1.
        SELECT * FROM stxfcont
          WHERE relid = 'XX' AND formname = @iv_formname
          ORDER BY relid, formname, srtf2
          INTO TABLE @lt_cont UP TO @lv_limit ROWS.
        ev_row_count = ev_row_count + lines( lt_cont ).
        IF ev_row_count > c_max_signature_rows.
          RETURN.
        ENDIF.
        lv_limit = c_max_signature_rows - ev_row_count + 1.
        SELECT * FROM stxfobjt
          WHERE formname = @iv_formname
          ORDER BY PRIMARY KEY
          INTO TABLE @lt_objt UP TO @lv_limit ROWS.
        ev_row_count = ev_row_count + lines( lt_objt ).
        IF ev_row_count > c_max_signature_rows.
          RETURN.
        ENDIF.
        lv_limit = c_max_signature_rows - ev_row_count + 1.
        SELECT * FROM stxftxt
          WHERE formname = @iv_formname
          ORDER BY PRIMARY KEY
          INTO TABLE @lt_txt UP TO @lv_limit ROWS.
        ev_row_count = ev_row_count + lines( lt_txt ).
        IF ev_row_count > c_max_signature_rows.
          RETURN.
        ENDIF.
        lv_limit = c_max_signature_rows - ev_row_count + 1.
        SELECT * FROM stxfadmt
          WHERE langu = @iv_language AND formname = @iv_formname
          ORDER BY langu, formname
          INTO TABLE @lt_admt UP TO @lv_limit ROWS.
        ev_row_count = ev_row_count + lines( lt_admt ).
        IF ev_row_count > c_max_signature_rows.
          RETURN.
        ENDIF.
        lv_limit = c_max_signature_rows - ev_row_count + 1.
        SELECT * FROM stxfvart
          WHERE langu = @iv_language AND formname = @iv_formname
          ORDER BY langu, formname, vari
          INTO TABLE @lt_vart UP TO @lv_limit ROWS.
        ev_row_count = ev_row_count + lines( lt_vart ).
        IF ev_row_count > c_max_signature_rows.
          RETURN.
        ENDIF.

        ls_signature-marker   = 'SSFO_ACTIVE_SIGNATURE_V1'.
        ls_signature-mandt    = sy-mandt.
        ls_signature-formname = iv_formname.
        ls_signature-language = iv_language.
        ls_signature-adm      = ls_adm.
        ls_signature-cont     = lt_cont.
        ls_signature-objt     = lt_objt.
        ls_signature-txt      = lt_txt.
        ls_signature-admt     = lt_admt.
        ls_signature-vart     = lt_vart.
        EXPORT signature = ls_signature TO DATA BUFFER lv_buffer.
        ev_input_bytes = xstrlen( lv_buffer ).
        IF ev_input_bytes > c_max_signature_bytes.
          RETURN.
        ENDIF.
        rv_signature = to_upper( zcl_abapgit_hash=>sha1_blob( lv_buffer ) ).
      CATCH cx_root.
        CLEAR rv_signature.
    ENDTRY.
  ENDMETHOD.

  METHOD try_read.
    DATA lv_payload TYPE zaog_ssfo_cache-payload.
    DATA ls_payload TYPE ty_payload.
    DATA lv_now TYPE tzntstmpl.

    CLEAR es_serialization.
    TRY.
        SELECT SINGLE payload FROM zaog_ssfo_cache INTO @lv_payload
          WHERE formname         = @iv_formname
            AND form_lang        = @iv_language
            AND context_hash     = @iv_context_hash
            AND source_signature = @iv_source_signature
            AND payload_version  = @c_payload_version.
        IF sy-subrc <> 0.
          RETURN.
        ENDIF.
        IMPORT payload = ls_payload FROM DATA BUFFER lv_payload.
        IF ls_payload-payload_version <> c_payload_version
           OR ls_payload-formname <> iv_formname
           OR ls_payload-form_language <> iv_language
           OR ls_payload-context_hash <> iv_context_hash
           OR ls_payload-source_signature <> iv_source_signature
           OR ls_payload-serialization-item <> is_item
           OR ls_payload-serialization-files IS INITIAL.
          DELETE FROM zaog_ssfo_cache
            WHERE formname         = @iv_formname
              AND form_lang        = @iv_language
              AND context_hash     = @iv_context_hash
              AND source_signature = @iv_source_signature
              AND payload_version  = @c_payload_version.
          RETURN.
        ENDIF.
        IF get_serialization_bytes( ls_payload-serialization ) > c_max_cache_content_bytes.
          RETURN.
        ENDIF.
        LOOP AT ls_payload-serialization-files INTO DATA(ls_file).
          IF zcl_abapgit_hash=>sha1_blob( ls_file-data ) <> ls_file-sha1.
            RETURN.
          ENDIF.
        ENDLOOP.
        lv_now = get_timestamp( ).
        UPDATE zaog_ssfo_cache SET last_used_at = @lv_now
          WHERE formname         = @iv_formname
            AND form_lang        = @iv_language
            AND context_hash     = @iv_context_hash
            AND source_signature = @iv_source_signature
            AND payload_version  = @c_payload_version.
        IF sy-subrc <> 0.
          RETURN.
        ENDIF.
        es_serialization = ls_payload-serialization.
        rv_found = abap_true.
      CATCH cx_root.
        CLEAR: es_serialization, rv_found.
    ENDTRY.
  ENDMETHOD.

  METHOD store.
    DATA ls_payload TYPE ty_payload.
    DATA lv_buffer TYPE zaog_ssfo_cache-payload.
    DATA ls_row TYPE zaog_ssfo_cache.

    IF get_serialization_bytes( is_serialization ) > c_max_cache_content_bytes.
      RETURN.
    ENDIF.
    TRY.
        ls_payload-payload_version  = c_payload_version.
        ls_payload-formname         = iv_formname.
        ls_payload-form_language    = iv_language.
        ls_payload-context_hash     = iv_context_hash.
        ls_payload-source_signature = iv_source_signature.
        ls_payload-serialization    = is_serialization.
        EXPORT payload = ls_payload TO DATA BUFFER lv_buffer.
        IF xstrlen( lv_buffer ) > c_max_cache_payload_bytes.
          RETURN.
        ENDIF.
        ls_row-formname         = iv_formname.
        ls_row-form_lang        = iv_language.
        ls_row-context_hash     = iv_context_hash.
        ls_row-source_signature = iv_source_signature.
        ls_row-payload_version  = c_payload_version.
        ls_row-payload          = lv_buffer.
        ls_row-payload_size     = xstrlen( lv_buffer ).
        ls_row-created_at       = get_timestamp( ).
        ls_row-last_used_at     = ls_row-created_at.
        MODIFY zaog_ssfo_cache FROM ls_row.
        CLEAR: lv_buffer, ls_payload, ls_row-payload.
        purge_to_limits( ).
      CATCH cx_root.
    ENDTRY.
  ENDMETHOD.

  METHOD purge_to_limits.
    TYPES: BEGIN OF ty_oldest,
             formname     TYPE tdsfname,
             payload_size TYPE i,
           END OF ty_oldest.
    DATA lt_oldest TYPE STANDARD TABLE OF ty_oldest WITH EMPTY KEY.
    DATA lr_formnames TYPE RANGE OF tdsfname.
    DATA lv_count TYPE i.
    DATA lv_total_bytes TYPE int8.
    DATA lv_batch TYPE i.

    TRY.
        SELECT SINGLE COUNT( * ) FROM zaog_ssfo_cache INTO @lv_count.
        SELECT SINGLE SUM( payload_size ) FROM zaog_ssfo_cache INTO @lv_total_bytes.
        WHILE ( lv_count > c_max_cache_rows OR lv_total_bytes > c_max_cache_total_bytes )
              AND lv_batch < c_max_eviction_batches.
          CLEAR: lt_oldest, lr_formnames.
          SELECT formname, payload_size FROM zaog_ssfo_cache
            ORDER BY last_used_at, formname
            INTO TABLE @lt_oldest UP TO @c_eviction_batch_rows ROWS.
          IF lt_oldest IS INITIAL.
            RETURN.
          ENDIF.
          LOOP AT lt_oldest INTO DATA(ls_oldest).
            APPEND VALUE #( sign = 'I' option = 'EQ' low = ls_oldest-formname ) TO lr_formnames.
            lv_count = lv_count - 1.
            lv_total_bytes = lv_total_bytes - ls_oldest-payload_size.
          ENDLOOP.
          DELETE FROM zaog_ssfo_cache WHERE formname IN @lr_formnames.
          lv_batch = lv_batch + 1.
        ENDWHILE.
      CATCH cx_root.
    ENDTRY.
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
