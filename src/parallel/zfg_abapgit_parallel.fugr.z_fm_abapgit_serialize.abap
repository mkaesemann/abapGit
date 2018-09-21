FUNCTION z_fm_abapgit_serialize.
*"----------------------------------------------------------------------
*"*"Local Interface:
*"  IMPORTING
*"     VALUE(IT_TADIR) TYPE  ZPP_ABAPGIT_T_TADIR
*"     VALUE(I_LAST_SERIALIZATION) TYPE  TIMESTAMP
*"     VALUE(I_MASTER_LANGUAGE) TYPE  SPRAS
*"  EXPORTING
*"     VALUE(RT_FILES) TYPE  ZPP_ABAPGIT_T_FILE_ITEM
*"     VALUE(RT_ERRORS) TYPE  ZPP_ABAPGIT_T_SERIAL_ERROR
*"  TABLES
*"      IT_FILTER STRUCTURE  TADIR
*"  EXCEPTIONS
*"      ABAPGIT_EXCEPTION
*"----------------------------------------------------------------------

  DATA: lt_filter       TYPE SORTED TABLE OF tadir
                        WITH NON-UNIQUE KEY object obj_name,
        lv_filter_exist TYPE abap_bool.

  DATA: ls_item     TYPE zif_abapgit_definitions=>ty_item,
        lt_files    TYPE zif_abapgit_definitions=>ty_files_tt,
        lo_progress TYPE REF TO zcl_abapgit_progress,
        lt_cache    TYPE SORTED TABLE OF zif_abapgit_definitions=>ty_file_item
                 WITH NON-UNIQUE KEY item.

  DATA: lo_log TYPE REF TO zcl_abapgit_log.

  "**********************************
  "Process Objects
  "**********************************

  CREATE OBJECT lo_log.

  lt_filter = it_filter[].
  lv_filter_exist = boolc( lines( lt_filter ) > 0 ).

  TRY.

      LOOP AT it_tadir ASSIGNING FIELD-SYMBOL(<ls_tadir>).

        IF lv_filter_exist = abap_true.
          READ TABLE lt_filter TRANSPORTING NO FIELDS WITH KEY object = <ls_tadir>-object
                                                               obj_name = <ls_tadir>-obj_name
                                                      BINARY SEARCH.
          IF sy-subrc <> 0.
            CONTINUE.
          ENDIF.
        ENDIF.

*    lo_progress->show(
*      iv_current = sy-tabix
*      iv_text    = |Serialize { <ls_tadir>-obj_name }| ) ##NO_TEXT.

        ls_item-obj_type = <ls_tadir>-object.
        ls_item-obj_name = <ls_tadir>-obj_name.
        ls_item-devclass = <ls_tadir>-devclass.

**Erst mal Ohne Puffer, denn wenn wir es so schnell genug bekommen brauchen wir den evtl. nicht
**    IF i_last_serialization IS NOT INITIAL. " Try to fetch from cache
**      READ TABLE lt_cache TRANSPORTING NO FIELDS
**        WITH KEY item = ls_item. " type+name+package key
**      " There is something in cache and the object is unchanged
**      IF sy-subrc = 0
**          AND abap_false = zcl_abapgit_objects=>has_changed_since(
**          is_item      = ls_item
**          iv_timestamp = i_last_serialization ).
**        LOOP AT lt_cache ASSIGNING <ls_cache> WHERE item = ls_item.
**          APPEND <ls_cache> TO rt_files.
**        ENDLOOP.
**
**        CONTINUE.
**      ENDIF.
**    ENDIF.
        TRY.
            lt_files = zcl_abapgit_objects=>serialize(
              is_item     = ls_item
              iv_language = i_master_language
              io_log      = lo_log ).
            LOOP AT lt_files ASSIGNING FIELD-SYMBOL(<ls_file>).
              <ls_file>-path = <ls_tadir>-path.
              <ls_file>-sha1 = zcl_abapgit_hash=>sha1(
                iv_type = zif_abapgit_definitions=>c_type-blob
                iv_data = <ls_file>-data ).

              APPEND INITIAL LINE TO rt_files[] ASSIGNING FIELD-SYMBOL(<ls_return>).
              <ls_return>-file = <ls_file>.
              <ls_return>-item = ls_item.
            ENDLOOP.
          CATCH zcx_abapgit_exception INTO DATA(ox_exception).
            "Encountered a serialization error: Log and continue
            INSERT VALUE #( item = ls_item
                            type = 'E'
                            msg  = ox_exception->get_text( ) )
              INTO TABLE rt_errors.
        ENDTRY.

      ENDLOOP.

    CATCH zcx_abapgit_exception INTO ox_exception.
      RAISE abapgit_exception.
  ENDTRY.

ENDFUNCTION.
