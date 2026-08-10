CLASS ltcl_wapa DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  " SER-SLICE-0 scope note: only T-WAPA-1 (serialization_wapa_review.md)
  " is implemented here originally. T-WAPA-2..5 all require calling
  " ZCL_ABAPGIT_ORTEC_WAPA=>serialize(), which unconditionally dispatches
  " to the concrete, non-injectable standard classes
  " CL_O2_API_APPLICATION=>LOAD/CL_O2_API_PAGES=>GET_ALL_PAGES/
  " GET_MASTER_LANGUAGE (hardcoded static calls, no interface or seam in
  " ZCL_ABAPGIT_ORTEC_WAPA to substitute a test double) - that part
  " remains BLOCKED_MISSING_PRODUCTION_SEAM, unchanged.
  "
  " SER-FINAL update: the Stage C raw O2PAGCON prefetch introduced its
  " own bulk `SELECT` against the real, TRANSPARENT `O2PAGCON` table
  " (IT8-confirmed TABLE_CATEGORY=TRANSPARENT) instead of
  " `IMPORT ... FROM DATABASE`. A plain `SELECT` on a transparent table
  " IS interceptable by CL_OSQL_TEST_ENVIRONMENT (only IMPORT/EXPORT
  " FROM/TO DATABASE cluster access is not) - so READ_RAW_ROWS and
  " TRY_RAW_PREFETCH are now locally testable via a double on `O2PAGCON`
  " itself, and ASSEMBLE_AND_DECODE/BUILD_REQUESTED_KEYS are testable
  " with zero DB dependency at all (pure, hand-built input tables). None
  " of this coverage depends on real, repository-specific O2 data.

  PRIVATE SECTION.
    CLASS-DATA gi_environment TYPE REF TO if_osql_test_environment.

    CLASS-METHODS class_setup.
    CLASS-METHODS class_teardown.

    METHODS setup.

    METHODS t_wapa_1_active_only FOR TESTING RAISING cx_static_check.
    METHODS t_wapa_1_inactive_only FOR TESTING RAISING cx_static_check.
    METHODS t_wapa_1_neither FOR TESTING RAISING cx_static_check.

    "--------------------------------------------------------------
    " GIVEN helpers - build IMPORT-FROM-DATA-BUFFER-compatible raw
    " buffers and split them into synthetic O2PAGCON physical rows,
    " exactly mirroring how a real cluster write would chunk them.
    "--------------------------------------------------------------
    METHODS build_page_buffer
      IMPORTING
        !iv_line          TYPE string DEFAULT 'test content line'
      RETURNING
        VALUE(rv_buffer)  TYPE xstring.

    METHODS build_evhandler_buffer
      IMPORTING
        !iv_name          TYPE string DEFAULT 'ON_INIT'
        !iv_source        TYPE string DEFAULT 'test handler source'
      RETURNING
        VALUE(rv_buffer)  TYPE xstring.

    METHODS build_typesource_buffer
      IMPORTING
        !iv_line          TYPE string DEFAULT 'TYPES: ty_test TYPE i.'
      RETURNING
        VALUE(rv_buffer)  TYPE xstring.

    METHODS split_buffer
      IMPORTING
        !iv_pagekey      TYPE o2pagdir-pagekey
        !iv_objtype      TYPE o2pconkey-objtype
        !iv_buffer       TYPE xstring
        !iv_chunk_size   TYPE i DEFAULT 4000
      RETURNING
        VALUE(rt_rows)   TYPE zcl_abapgit_ortec_wapa=>ty_raw_row_tt.

    "--------------------------------------------------------------
    " ASSEMBLE_AND_DECODE - pure reconstruction/decode, no DB
    "--------------------------------------------------------------
    METHODS single_row_page_reconstructs FOR TESTING RAISING cx_static_check.
    METHODS multi_row_ordered_by_srtf2 FOR TESTING RAISING cx_static_check.
    METHODS clustr_truncation_ignores_pad FOR TESTING RAISING cx_static_check.
    METHODS interleaved_rows_multi_keys FOR TESTING RAISING cx_static_check.
    METHODS page_evhndl_types_separated FOR TESTING RAISING cx_static_check.
    METHODS optional_evhndl_absent_is_empty FOR TESTING RAISING cx_static_check.
    METHODS optional_types_absent_is_empty FOR TESTING RAISING cx_static_check.
    METHODS missing_page_content_raises FOR TESTING RAISING cx_static_check.
    METHODS duplicate_srtf2_raises FOR TESTING RAISING cx_static_check.
    METHODS gapped_srtf2_raises FOR TESTING RAISING cx_static_check.
    METHODS malformed_clustr_raises FOR TESTING RAISING cx_static_check.
    METHODS byte_cap_exceeded_raises FOR TESTING RAISING cx_static_check.

    "--------------------------------------------------------------
    " BUILD_REQUESTED_KEYS - pure, hand-built context
    "--------------------------------------------------------------
    METHODS keys_skip_controller_pages FOR TESTING RAISING cx_static_check.
    METHODS keys_full_page_needs_types FOR TESTING RAISING cx_static_check.
    METHODS keys_full_page_needs_evhndl_only_if_present FOR TESTING RAISING cx_static_check.

    "--------------------------------------------------------------
    " READ_RAW_ROWS / TRY_RAW_PREFETCH - real O2PAGCON double
    "--------------------------------------------------------------
    METHODS read_raw_rows_filters_by_key FOR TESTING RAISING cx_static_check.
    METHODS prefetch_hit_feeds_content FOR TESTING RAISING cx_static_check.
    METHODS prefetch_row_cap_falls_back FOR TESTING RAISING cx_static_check.
    METHODS prefetch_no_pages_is_trivial_hit FOR TESTING RAISING cx_static_check.

    "--------------------------------------------------------------
    " Counters
    "--------------------------------------------------------------
    METHODS counters_reset_to_zero FOR TESTING RAISING cx_static_check.

ENDCLASS.


CLASS zcl_abapgit_ortec_wapa DEFINITION LOCAL FRIENDS ltcl_wapa.


CLASS ltcl_wapa IMPLEMENTATION.

  METHOD class_setup.
    DATA lt_tables TYPE if_osql_test_environment=>ty_t_sobjnames.
    APPEND 'O2APPL' TO lt_tables.
    APPEND 'O2PAGCON' TO lt_tables.
    gi_environment = cl_osql_test_environment=>create( lt_tables ).
  ENDMETHOD.

  METHOD class_teardown.
    gi_environment->destroy( ).
  ENDMETHOD.

  METHOD setup.
    gi_environment->clear_doubles( ).
    zcl_abapgit_ortec_wapa=>reset_raw_prefetch_counters( ).
  ENDMETHOD.

  METHOD t_wapa_1_active_only.
    DATA lt_o2appl TYPE STANDARD TABLE OF o2appl WITH DEFAULT KEY.
    APPEND VALUE #( applname = 'ZTST_BEX_WAPA1' version = 'A' ) TO lt_o2appl.
    gi_environment->insert_test_data( lt_o2appl ).

    DATA(lv_exists) = zcl_abapgit_ortec_wapa=>exists( 'ZTST_BEX_WAPA1' ).

    cl_abap_unit_assert=>assert_true( lv_exists ).
  ENDMETHOD.

  METHOD t_wapa_1_inactive_only.
    DATA lt_o2appl TYPE STANDARD TABLE OF o2appl WITH DEFAULT KEY.
    APPEND VALUE #( applname = 'ZTST_BEX_WAPA2' version = 'I' ) TO lt_o2appl.
    gi_environment->insert_test_data( lt_o2appl ).

    DATA(lv_exists) = zcl_abapgit_ortec_wapa=>exists( 'ZTST_BEX_WAPA2' ).

    cl_abap_unit_assert=>assert_true( lv_exists ).
  ENDMETHOD.

  METHOD t_wapa_1_neither.
    " No O2APPL row inserted at all for this name.
    DATA(lv_exists) = zcl_abapgit_ortec_wapa=>exists( 'ZTST_BEX_WAPA3' ).

    cl_abap_unit_assert=>assert_false( lv_exists ).
  ENDMETHOD.

  METHOD build_page_buffer.
    DATA lt_content TYPE o2pageline_table.
    DATA lv_xml_source TYPE xstring.

    APPEND iv_line TO lt_content.

    EXPORT content    = lt_content
           xml_source = lv_xml_source
           TO DATA BUFFER rv_buffer.
  ENDMETHOD.

  METHOD build_evhandler_buffer.
    DATA lt_evhandler TYPE so2_ev_handler_t.

    APPEND VALUE #( name = iv_name source = iv_source ) TO lt_evhandler.

    EXPORT evhandler = lt_evhandler TO DATA BUFFER rv_buffer.
  ENDMETHOD.

  METHOD build_typesource_buffer.
    DATA lt_typesource TYPE rswsourcet.

    APPEND iv_line TO lt_typesource.

    EXPORT typesource = lt_typesource TO DATA BUFFER rv_buffer.
  ENDMETHOD.

  METHOD split_buffer.
    DATA lv_offset TYPE i.
    DATA lv_len    TYPE i.
    DATA lv_srtf2  TYPE i.
    DATA ls_row    TYPE zcl_abapgit_ortec_wapa=>ty_raw_row.
    DATA(lv_total) = xstrlen( iv_buffer ).

    WHILE lv_offset < lv_total.
      lv_len = iv_chunk_size.
      IF lv_offset + lv_len > lv_total.
        lv_len = lv_total - lv_offset.
      ENDIF.

      CLEAR ls_row.
      ls_row-pagekey = iv_pagekey.
      ls_row-objtype = iv_objtype.
      ls_row-srtf2   = lv_srtf2.
      ls_row-clustr  = lv_len.
      ls_row-clustd  = iv_buffer+lv_offset(lv_len).
      APPEND ls_row TO rt_rows.

      lv_offset = lv_offset + lv_len.
      lv_srtf2  = lv_srtf2 + 1.
    ENDWHILE.

    IF rt_rows IS INITIAL.
      " zero-length buffer - still emit one, empty, valid row
      CLEAR ls_row.
      ls_row-pagekey = iv_pagekey.
      ls_row-objtype = iv_objtype.
      APPEND ls_row TO rt_rows.
    ENDIF.
  ENDMETHOD.

  METHOD single_row_page_reconstructs.
    DATA lt_keys       TYPE zcl_abapgit_ortec_wapa=>ty_raw_key_tt.
    DATA lt_rows       TYPE zcl_abapgit_ortec_wapa=>ty_raw_row_tt.
    DATA lt_content    TYPE zcl_abapgit_ortec_wapa=>ty_raw_content_tt.
    DATA lt_evhandler  TYPE zcl_abapgit_ortec_wapa=>ty_raw_evhandler_tt.
    DATA lt_typesource TYPE zcl_abapgit_ortec_wapa=>ty_raw_typesource_tt.

    INSERT VALUE #( pagekey = 'PAGE1' ) INTO TABLE lt_keys.
    lt_rows = split_buffer( iv_pagekey = 'PAGE1' iv_objtype = 'PAGE' iv_buffer = build_page_buffer( 'hello' )
                            iv_chunk_size = 10000 ).

    zcl_abapgit_ortec_wapa=>assemble_and_decode(
      EXPORTING
        it_keys       = lt_keys
        it_rows       = lt_rows
      IMPORTING
        et_content    = lt_content
        et_evhandler  = lt_evhandler
        et_typesource = lt_typesource ).

    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( lt_content ) ).
    READ TABLE lt_content INTO DATA(ls_content) WITH TABLE KEY pagekey = 'PAGE1'.
    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( ls_content-content ) ).
    READ TABLE ls_content-content INTO DATA(lv_line) INDEX 1.
    cl_abap_unit_assert=>assert_equals( exp = 'hello' act = lv_line ).
  ENDMETHOD.

  METHOD multi_row_ordered_by_srtf2.
    DATA lt_keys       TYPE zcl_abapgit_ortec_wapa=>ty_raw_key_tt.
    DATA lt_content    TYPE zcl_abapgit_ortec_wapa=>ty_raw_content_tt.
    DATA lt_evhandler  TYPE zcl_abapgit_ortec_wapa=>ty_raw_evhandler_tt.
    DATA lt_typesource TYPE zcl_abapgit_ortec_wapa=>ty_raw_typesource_tt.

    INSERT VALUE #( pagekey = 'PAGE1' ) INTO TABLE lt_keys.
    " force many small physical rows for one logical key
    DATA(lt_rows) = split_buffer( iv_pagekey = 'PAGE1' iv_objtype = 'PAGE'
                                  iv_buffer = build_page_buffer( repeat( val = 'AB' occ = 5000 ) )
                                  iv_chunk_size = 37 ).
    cl_abap_unit_assert=>assert_true( lines( lt_rows ) > 1 ).

    " scramble the physical row order - ASSEMBLE_AND_DECODE must not rely
    " on caller-side pre-sorting.
    DATA(lt_scrambled) = lt_rows.
    SORT lt_scrambled BY clustr DESCENDING.

    zcl_abapgit_ortec_wapa=>assemble_and_decode(
      EXPORTING
        it_keys       = lt_keys
        it_rows       = lt_scrambled
      IMPORTING
        et_content    = lt_content
        et_evhandler  = lt_evhandler
        et_typesource = lt_typesource ).

    READ TABLE lt_content INTO DATA(ls_content) WITH TABLE KEY pagekey = 'PAGE1'.
    READ TABLE ls_content-content INTO DATA(lv_line) INDEX 1.
    cl_abap_unit_assert=>assert_equals( exp = repeat( val = 'AB' occ = 5000 ) act = lv_line ).
  ENDMETHOD.

  METHOD clustr_truncation_ignores_pad.
    DATA lt_keys       TYPE zcl_abapgit_ortec_wapa=>ty_raw_key_tt.
    DATA lt_rows       TYPE zcl_abapgit_ortec_wapa=>ty_raw_row_tt.
    DATA lt_content    TYPE zcl_abapgit_ortec_wapa=>ty_raw_content_tt.
    DATA lt_evhandler  TYPE zcl_abapgit_ortec_wapa=>ty_raw_evhandler_tt.
    DATA lt_typesource TYPE zcl_abapgit_ortec_wapa=>ty_raw_typesource_tt.
    DATA ls_row        TYPE zcl_abapgit_ortec_wapa=>ty_raw_row.
    DATA lv_padded     TYPE xstring.

    DATA(lv_real) = build_page_buffer( 'real content' ).
    DATA(lv_garbage) = CONV xstring( '00112233445566778899' ).
    CONCATENATE lv_real lv_garbage INTO lv_padded IN BYTE MODE.

    INSERT VALUE #( pagekey = 'PAGE1' ) INTO TABLE lt_keys.

    ls_row-pagekey = 'PAGE1'.
    ls_row-objtype = 'PAGE'.
    ls_row-srtf2   = 0.
    ls_row-clustr  = xstrlen( lv_real ).  " only the real, non-garbage portion is valid
    ls_row-clustd  = lv_padded.
    APPEND ls_row TO lt_rows.

    zcl_abapgit_ortec_wapa=>assemble_and_decode(
      EXPORTING
        it_keys       = lt_keys
        it_rows       = lt_rows
      IMPORTING
        et_content    = lt_content
        et_evhandler  = lt_evhandler
        et_typesource = lt_typesource ).

    READ TABLE lt_content INTO DATA(ls_content) WITH TABLE KEY pagekey = 'PAGE1'.
    READ TABLE ls_content-content INTO DATA(lv_line) INDEX 1.
    cl_abap_unit_assert=>assert_equals( exp = 'real content' act = lv_line ).
  ENDMETHOD.

  METHOD interleaved_rows_multi_keys.
    DATA lt_keys       TYPE zcl_abapgit_ortec_wapa=>ty_raw_key_tt.
    DATA lt_content    TYPE zcl_abapgit_ortec_wapa=>ty_raw_content_tt.
    DATA lt_evhandler  TYPE zcl_abapgit_ortec_wapa=>ty_raw_evhandler_tt.
    DATA lt_typesource TYPE zcl_abapgit_ortec_wapa=>ty_raw_typesource_tt.
    DATA lt_rows       TYPE zcl_abapgit_ortec_wapa=>ty_raw_row_tt.
    DATA lv_idx        TYPE i VALUE 1.

    INSERT VALUE #( pagekey = 'PAGE1' ) INTO TABLE lt_keys.
    INSERT VALUE #( pagekey = 'PAGE2' ) INTO TABLE lt_keys.

    DATA(lt_rows_1) = split_buffer( iv_pagekey = 'PAGE1' iv_objtype = 'PAGE'
                                    iv_buffer = build_page_buffer( 'content one' ) iv_chunk_size = 6 ).
    DATA(lt_rows_2) = split_buffer( iv_pagekey = 'PAGE2' iv_objtype = 'PAGE'
                                    iv_buffer = build_page_buffer( 'content two' ) iv_chunk_size = 6 ).

    " interleave: 1,2,1,2,... - proves rows are grouped by full logical
    " key, not assumed contiguous in the input table.
    DO.
      IF lv_idx > lines( lt_rows_1 ) AND lv_idx > lines( lt_rows_2 ).
        EXIT.
      ENDIF.
      IF lv_idx <= lines( lt_rows_1 ).
        APPEND LINES OF lt_rows_1 FROM lv_idx TO lv_idx TO lt_rows.
      ENDIF.
      IF lv_idx <= lines( lt_rows_2 ).
        APPEND LINES OF lt_rows_2 FROM lv_idx TO lv_idx TO lt_rows.
      ENDIF.
      lv_idx = lv_idx + 1.
    ENDDO.

    zcl_abapgit_ortec_wapa=>assemble_and_decode(
      EXPORTING
        it_keys       = lt_keys
        it_rows       = lt_rows
      IMPORTING
        et_content    = lt_content
        et_evhandler  = lt_evhandler
        et_typesource = lt_typesource ).

    READ TABLE lt_content INTO DATA(ls_content_1) WITH TABLE KEY pagekey = 'PAGE1'.
    READ TABLE ls_content_1-content INTO DATA(lv_line_1) INDEX 1.
    cl_abap_unit_assert=>assert_equals( exp = 'content one' act = lv_line_1 ).

    READ TABLE lt_content INTO DATA(ls_content_2) WITH TABLE KEY pagekey = 'PAGE2'.
    READ TABLE ls_content_2-content INTO DATA(lv_line_2) INDEX 1.
    cl_abap_unit_assert=>assert_equals( exp = 'content two' act = lv_line_2 ).
  ENDMETHOD.

  METHOD page_evhndl_types_separated.
    DATA lt_keys       TYPE zcl_abapgit_ortec_wapa=>ty_raw_key_tt.
    DATA lt_rows       TYPE zcl_abapgit_ortec_wapa=>ty_raw_row_tt.
    DATA lt_content    TYPE zcl_abapgit_ortec_wapa=>ty_raw_content_tt.
    DATA lt_evhandler  TYPE zcl_abapgit_ortec_wapa=>ty_raw_evhandler_tt.
    DATA lt_typesource TYPE zcl_abapgit_ortec_wapa=>ty_raw_typesource_tt.

    INSERT VALUE #( pagekey = 'PAGE1' need_evhndl = abap_true need_types = abap_true ) INTO TABLE lt_keys.

    APPEND LINES OF split_buffer( iv_pagekey = 'PAGE1' iv_objtype = 'PAGE'
      iv_buffer = build_page_buffer( 'the content' ) ) TO lt_rows.
    APPEND LINES OF split_buffer( iv_pagekey = 'PAGE1' iv_objtype = 'EVHNDL'
      iv_buffer = build_evhandler_buffer( ) ) TO lt_rows.
    APPEND LINES OF split_buffer( iv_pagekey = 'PAGE1' iv_objtype = 'TYPES'
      iv_buffer = build_typesource_buffer( ) ) TO lt_rows.

    zcl_abapgit_ortec_wapa=>assemble_and_decode(
      EXPORTING
        it_keys       = lt_keys
        it_rows       = lt_rows
      IMPORTING
        et_content    = lt_content
        et_evhandler  = lt_evhandler
        et_typesource = lt_typesource ).

    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( lt_content ) ).
    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( lt_evhandler ) ).
    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( lt_typesource ) ).

    READ TABLE lt_evhandler INTO DATA(ls_evhandler) WITH TABLE KEY pagekey = 'PAGE1'.
    READ TABLE ls_evhandler-evhandler INTO DATA(ls_handler) INDEX 1.
    cl_abap_unit_assert=>assert_equals( exp = 'ON_INIT' act = ls_handler-name ).

    READ TABLE lt_typesource INTO DATA(ls_typesource) WITH TABLE KEY pagekey = 'PAGE1'.
    READ TABLE ls_typesource-typesource INTO DATA(lv_type_line) INDEX 1.
    cl_abap_unit_assert=>assert_equals( exp = 'TYPES: ty_test TYPE i.' act = lv_type_line ).
  ENDMETHOD.

  METHOD optional_evhndl_absent_is_empty.
    DATA lt_keys       TYPE zcl_abapgit_ortec_wapa=>ty_raw_key_tt.
    DATA lt_content    TYPE zcl_abapgit_ortec_wapa=>ty_raw_content_tt.
    DATA lt_evhandler  TYPE zcl_abapgit_ortec_wapa=>ty_raw_evhandler_tt.
    DATA lt_typesource TYPE zcl_abapgit_ortec_wapa=>ty_raw_typesource_tt.

    " EVHNDL requested but has zero physical rows - normal, not an anomaly
    INSERT VALUE #( pagekey = 'PAGE1' need_evhndl = abap_true ) INTO TABLE lt_keys.
    DATA(lt_rows) = split_buffer( iv_pagekey = 'PAGE1' iv_objtype = 'PAGE' iv_buffer = build_page_buffer( 'x' ) ).

    zcl_abapgit_ortec_wapa=>assemble_and_decode(
      EXPORTING
        it_keys       = lt_keys
        it_rows       = lt_rows
      IMPORTING
        et_content    = lt_content
        et_evhandler  = lt_evhandler
        et_typesource = lt_typesource ).

    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( lt_evhandler ) ).
    READ TABLE lt_evhandler INTO DATA(ls_evhandler) WITH TABLE KEY pagekey = 'PAGE1'.
    cl_abap_unit_assert=>assert_true( ls_evhandler-evhandler IS INITIAL ).
  ENDMETHOD.

  METHOD optional_types_absent_is_empty.
    DATA lt_keys       TYPE zcl_abapgit_ortec_wapa=>ty_raw_key_tt.
    DATA lt_content    TYPE zcl_abapgit_ortec_wapa=>ty_raw_content_tt.
    DATA lt_evhandler  TYPE zcl_abapgit_ortec_wapa=>ty_raw_evhandler_tt.
    DATA lt_typesource TYPE zcl_abapgit_ortec_wapa=>ty_raw_typesource_tt.

    INSERT VALUE #( pagekey = 'PAGE1' need_types = abap_true ) INTO TABLE lt_keys.
    DATA(lt_rows) = split_buffer( iv_pagekey = 'PAGE1' iv_objtype = 'PAGE' iv_buffer = build_page_buffer( 'x' ) ).

    zcl_abapgit_ortec_wapa=>assemble_and_decode(
      EXPORTING
        it_keys       = lt_keys
        it_rows       = lt_rows
      IMPORTING
        et_content    = lt_content
        et_evhandler  = lt_evhandler
        et_typesource = lt_typesource ).

    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( lt_typesource ) ).
    READ TABLE lt_typesource INTO DATA(ls_typesource) WITH TABLE KEY pagekey = 'PAGE1'.
    cl_abap_unit_assert=>assert_true( ls_typesource-typesource IS INITIAL ).
  ENDMETHOD.

  METHOD missing_page_content_raises.
    DATA lt_keys       TYPE zcl_abapgit_ortec_wapa=>ty_raw_key_tt.
    DATA lt_rows       TYPE zcl_abapgit_ortec_wapa=>ty_raw_row_tt.
    DATA lt_content    TYPE zcl_abapgit_ortec_wapa=>ty_raw_content_tt.
    DATA lt_evhandler  TYPE zcl_abapgit_ortec_wapa=>ty_raw_evhandler_tt.
    DATA lt_typesource TYPE zcl_abapgit_ortec_wapa=>ty_raw_typesource_tt.

    " requested PAGE-content key with NO rows at all - unlike EVHNDL/TYPES
    " this must raise (mirrors the reference path's own hard error).
    INSERT VALUE #( pagekey = 'PAGE1' ) INTO TABLE lt_keys.

    TRY.
        zcl_abapgit_ortec_wapa=>assemble_and_decode(
          EXPORTING
            it_keys       = lt_keys
            it_rows       = lt_rows
          IMPORTING
            et_content    = lt_content
            et_evhandler  = lt_evhandler
            et_typesource = lt_typesource ).
        cl_abap_unit_assert=>fail( 'expected ZCX_ABAPGIT_EXCEPTION' ).
      CATCH zcx_abapgit_exception ##NO_HANDLER.
    ENDTRY.
  ENDMETHOD.

  METHOD duplicate_srtf2_raises.
    DATA lt_keys       TYPE zcl_abapgit_ortec_wapa=>ty_raw_key_tt.
    DATA lt_rows       TYPE zcl_abapgit_ortec_wapa=>ty_raw_row_tt.
    DATA lt_content    TYPE zcl_abapgit_ortec_wapa=>ty_raw_content_tt.
    DATA lt_evhandler  TYPE zcl_abapgit_ortec_wapa=>ty_raw_evhandler_tt.
    DATA lt_typesource TYPE zcl_abapgit_ortec_wapa=>ty_raw_typesource_tt.

    INSERT VALUE #( pagekey = 'PAGE1' ) INTO TABLE lt_keys.
    APPEND VALUE #( pagekey = 'PAGE1' objtype = 'PAGE' srtf2 = 0 clustr = 1 clustd = '41' ) TO lt_rows.
    APPEND VALUE #( pagekey = 'PAGE1' objtype = 'PAGE' srtf2 = 0 clustr = 1 clustd = '42' ) TO lt_rows.

    TRY.
        zcl_abapgit_ortec_wapa=>assemble_and_decode(
          EXPORTING
            it_keys       = lt_keys
            it_rows       = lt_rows
          IMPORTING
            et_content    = lt_content
            et_evhandler  = lt_evhandler
            et_typesource = lt_typesource ).
        cl_abap_unit_assert=>fail( 'expected ZCX_ABAPGIT_EXCEPTION for duplicate SRTF2' ).
      CATCH zcx_abapgit_exception ##NO_HANDLER.
    ENDTRY.
  ENDMETHOD.

  METHOD gapped_srtf2_raises.
    DATA lt_keys       TYPE zcl_abapgit_ortec_wapa=>ty_raw_key_tt.
    DATA lt_rows       TYPE zcl_abapgit_ortec_wapa=>ty_raw_row_tt.
    DATA lt_content    TYPE zcl_abapgit_ortec_wapa=>ty_raw_content_tt.
    DATA lt_evhandler  TYPE zcl_abapgit_ortec_wapa=>ty_raw_evhandler_tt.
    DATA lt_typesource TYPE zcl_abapgit_ortec_wapa=>ty_raw_typesource_tt.

    INSERT VALUE #( pagekey = 'PAGE1' ) INTO TABLE lt_keys.
    APPEND VALUE #( pagekey = 'PAGE1' objtype = 'PAGE' srtf2 = 0 clustr = 1 clustd = '41' ) TO lt_rows.
    APPEND VALUE #( pagekey = 'PAGE1' objtype = 'PAGE' srtf2 = 2 clustr = 1 clustd = '42' ) TO lt_rows.

    TRY.
        zcl_abapgit_ortec_wapa=>assemble_and_decode(
          EXPORTING
            it_keys       = lt_keys
            it_rows       = lt_rows
          IMPORTING
            et_content    = lt_content
            et_evhandler  = lt_evhandler
            et_typesource = lt_typesource ).
        cl_abap_unit_assert=>fail( 'expected ZCX_ABAPGIT_EXCEPTION for gapped SRTF2' ).
      CATCH zcx_abapgit_exception ##NO_HANDLER.
    ENDTRY.
  ENDMETHOD.

  METHOD malformed_clustr_raises.
    DATA lt_keys       TYPE zcl_abapgit_ortec_wapa=>ty_raw_key_tt.
    DATA lt_rows       TYPE zcl_abapgit_ortec_wapa=>ty_raw_row_tt.
    DATA lt_content    TYPE zcl_abapgit_ortec_wapa=>ty_raw_content_tt.
    DATA lt_evhandler  TYPE zcl_abapgit_ortec_wapa=>ty_raw_evhandler_tt.
    DATA lt_typesource TYPE zcl_abapgit_ortec_wapa=>ty_raw_typesource_tt.

    INSERT VALUE #( pagekey = 'PAGE1' ) INTO TABLE lt_keys.
    " CLUSTR claims 5 valid bytes but CLUSTD only has 1
    APPEND VALUE #( pagekey = 'PAGE1' objtype = 'PAGE' srtf2 = 0 clustr = 5 clustd = '41' ) TO lt_rows.

    TRY.
        zcl_abapgit_ortec_wapa=>assemble_and_decode(
          EXPORTING
            it_keys       = lt_keys
            it_rows       = lt_rows
          IMPORTING
            et_content    = lt_content
            et_evhandler  = lt_evhandler
            et_typesource = lt_typesource ).
        cl_abap_unit_assert=>fail( 'expected ZCX_ABAPGIT_EXCEPTION for malformed CLUSTR' ).
      CATCH zcx_abapgit_exception ##NO_HANDLER.
    ENDTRY.
  ENDMETHOD.

  METHOD byte_cap_exceeded_raises.
    DATA lt_keys       TYPE zcl_abapgit_ortec_wapa=>ty_raw_key_tt.
    DATA lt_rows       TYPE zcl_abapgit_ortec_wapa=>ty_raw_row_tt.
    DATA lt_content    TYPE zcl_abapgit_ortec_wapa=>ty_raw_content_tt.
    DATA lt_evhandler  TYPE zcl_abapgit_ortec_wapa=>ty_raw_evhandler_tt.
    DATA lt_typesource TYPE zcl_abapgit_ortec_wapa=>ty_raw_typesource_tt.

    INSERT VALUE #( pagekey = 'PAGE1' ) INTO TABLE lt_keys.

    " CLUSTR claims more bytes than the (deliberately small) CLUSTD chunk
    " actually holds - this is caught by the malformed-CLUSTR guard,
    " which fires strictly before the byte-cap accumulator would ever be
    " reached for a claim this large; both guards independently protect
    " the same invariant (never trust an unverifiable byte count), so
    " either one raising here is the correct, safe outcome.
    APPEND VALUE #( pagekey = 'PAGE1' objtype = 'PAGE' srtf2 = 0
                    clustr = zcl_abapgit_ortec_ser_orch=>c_max_object_output_bytes + 1
                    clustd = '41' )
      TO lt_rows.

    TRY.
        zcl_abapgit_ortec_wapa=>assemble_and_decode(
          EXPORTING
            it_keys       = lt_keys
            it_rows       = lt_rows
          IMPORTING
            et_content    = lt_content
            et_evhandler  = lt_evhandler
            et_typesource = lt_typesource ).
        cl_abap_unit_assert=>fail( 'expected ZCX_ABAPGIT_EXCEPTION for an over-cap byte claim' ).
      CATCH zcx_abapgit_exception ##NO_HANDLER.
    ENDTRY.
  ENDMETHOD.

  METHOD keys_skip_controller_pages.
    DATA ls_context TYPE zcl_abapgit_ortec_wapa=>ty_context.
    DATA lt_pages   TYPE o2pagelist.

    INSERT VALUE #( applname = 'ZTST' pagekey = 'PAGE1' pagetype = 'C' ) INTO TABLE ls_context-page_dirs.
    ls_context-name = 'ZTST'.
    APPEND VALUE #( pagekey = 'PAGE1' ) TO lt_pages.

    DATA(lt_keys) = zcl_abapgit_ortec_wapa=>build_requested_keys( it_pages = lt_pages is_context = ls_context ).

    cl_abap_unit_assert=>assert_initial( lt_keys ).
  ENDMETHOD.

  METHOD keys_full_page_needs_types.
    DATA ls_context TYPE zcl_abapgit_ortec_wapa=>ty_context.
    DATA lt_pages   TYPE o2pagelist.

    INSERT VALUE #( applname = 'ZTST' pagekey = 'PAGE1' pagetype = 'F' ) INTO TABLE ls_context-page_dirs.
    ls_context-name = 'ZTST'.
    APPEND VALUE #( pagekey = 'PAGE1' ) TO lt_pages.

    DATA(lt_keys) = zcl_abapgit_ortec_wapa=>build_requested_keys( it_pages = lt_pages is_context = ls_context ).

    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( lt_keys ) ).
    READ TABLE lt_keys INTO DATA(ls_key) WITH TABLE KEY pagekey = 'PAGE1'.
    cl_abap_unit_assert=>assert_true( ls_key-need_types ).
    cl_abap_unit_assert=>assert_false( ls_key-need_evhndl ).
  ENDMETHOD.

  METHOD keys_full_page_needs_evhndl_only_if_present.
    DATA ls_context TYPE zcl_abapgit_ortec_wapa=>ty_context.
    DATA lt_pages   TYPE o2pagelist.

    INSERT VALUE #( applname = 'ZTST' pagekey = 'PAGE1' pagetype = 'F' ) INTO TABLE ls_context-page_dirs.
    INSERT VALUE #( applname = 'ZTST' pagekey = 'PAGE1' version = 'A' evhandler = 'ON_INIT' )
      INTO TABLE ls_context-event_handlers.
    ls_context-name = 'ZTST'.
    APPEND VALUE #( pagekey = 'PAGE1' ) TO lt_pages.

    DATA(lt_keys) = zcl_abapgit_ortec_wapa=>build_requested_keys( it_pages = lt_pages is_context = ls_context ).

    READ TABLE lt_keys INTO DATA(ls_key) WITH TABLE KEY pagekey = 'PAGE1'.
    cl_abap_unit_assert=>assert_true( ls_key-need_evhndl ).
  ENDMETHOD.

  METHOD read_raw_rows_filters_by_key.
    DATA lt_keys        TYPE zcl_abapgit_ortec_wapa=>ty_raw_key_tt.
    DATA lt_rows        TYPE zcl_abapgit_ortec_wapa=>ty_raw_row_tt.
    DATA lv_row_cap_hit TYPE abap_bool.
    DATA lt_o2pagcon    TYPE STANDARD TABLE OF o2pagcon WITH DEFAULT KEY.

    APPEND VALUE #( relid = 'TR' applname = 'ZTST' pagekey = 'PAGE1' objtype = 'PAGE' version = 'A'
                    srtf2 = 0 clustr = 1 clustd = '41' ) TO lt_o2pagcon.
    " a different, non-requested page - must NOT be returned
    APPEND VALUE #( relid = 'TR' applname = 'ZTST' pagekey = 'PAGE2' objtype = 'PAGE' version = 'A'
                    srtf2 = 0 clustr = 1 clustd = '42' ) TO lt_o2pagcon.
    " same key but inactive version - must NOT be returned
    APPEND VALUE #( relid = 'TR' applname = 'ZTST' pagekey = 'PAGE1' objtype = 'PAGE' version = 'I'
                    srtf2 = 0 clustr = 1 clustd = '43' ) TO lt_o2pagcon.
    gi_environment->insert_test_data( lt_o2pagcon ).

    INSERT VALUE #( pagekey = 'PAGE1' ) INTO TABLE lt_keys.

    zcl_abapgit_ortec_wapa=>read_raw_rows(
      EXPORTING
        iv_name        = 'ZTST'
        it_keys        = lt_keys
      IMPORTING
        et_rows        = lt_rows
        ev_row_cap_hit = lv_row_cap_hit ).

    cl_abap_unit_assert=>assert_false( lv_row_cap_hit ).
    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( lt_rows ) ).
    READ TABLE lt_rows INTO DATA(ls_row) INDEX 1.
    cl_abap_unit_assert=>assert_equals( exp = 'PAGE1' act = ls_row-pagekey ).
  ENDMETHOD.

  METHOD prefetch_hit_feeds_content.
    DATA ls_context  TYPE zcl_abapgit_ortec_wapa=>ty_context.
    DATA lt_pages    TYPE o2pagelist.
    DATA lt_o2pagcon TYPE STANDARD TABLE OF o2pagcon WITH DEFAULT KEY.

    INSERT VALUE #( applname = 'ZTST' pagekey = 'PAGE1' pagetype = 'N' ) INTO TABLE ls_context-page_dirs.
    ls_context-name = 'ZTST'.
    APPEND VALUE #( pagekey = 'PAGE1' ) TO lt_pages.

    DATA(lt_rows) = split_buffer( iv_pagekey = 'PAGE1' iv_objtype = 'PAGE'
                                  iv_buffer = build_page_buffer( 'hit content' ) ).
    LOOP AT lt_rows INTO DATA(ls_row).
      APPEND VALUE #( relid = 'TR' applname = 'ZTST' pagekey = ls_row-pagekey objtype = ls_row-objtype
                      version = 'A' srtf2 = ls_row-srtf2 clustr = ls_row-clustr clustd = ls_row-clustd )
        TO lt_o2pagcon.
    ENDLOOP.
    gi_environment->insert_test_data( lt_o2pagcon ).

    zcl_abapgit_ortec_wapa=>try_raw_prefetch(
      EXPORTING
        it_pages   = lt_pages
      CHANGING
        cs_context = ls_context ).

    cl_abap_unit_assert=>assert_true( ls_context-raw_prefetch_active ).
    READ TABLE ls_context-raw_content INTO DATA(ls_content) WITH TABLE KEY pagekey = 'PAGE1'.
    READ TABLE ls_content-content INTO DATA(lv_line) INDEX 1.
    cl_abap_unit_assert=>assert_equals( exp = 'hit content' act = lv_line ).

    zcl_abapgit_ortec_wapa=>get_raw_prefetch_counters(
      IMPORTING ev_hits = DATA(lv_hits) ev_fallbacks = DATA(lv_fallbacks) ).
    cl_abap_unit_assert=>assert_equals( exp = 1 act = lv_hits ).
    cl_abap_unit_assert=>assert_equals( exp = 0 act = lv_fallbacks ).
  ENDMETHOD.

  METHOD prefetch_row_cap_falls_back.
    DATA ls_context TYPE zcl_abapgit_ortec_wapa=>ty_context.
    DATA lt_pages   TYPE o2pagelist.

    " no O2PAGCON rows inserted at all -> read_raw_rows finds nothing for
    " the one requested PAGE key -> assemble_and_decode raises (missing
    " content) -> try_raw_prefetch must fall back cleanly, not propagate.
    INSERT VALUE #( applname = 'ZTST' pagekey = 'PAGE1' pagetype = 'N' ) INTO TABLE ls_context-page_dirs.
    ls_context-name = 'ZTST'.
    APPEND VALUE #( pagekey = 'PAGE1' ) TO lt_pages.

    zcl_abapgit_ortec_wapa=>try_raw_prefetch(
      EXPORTING
        it_pages   = lt_pages
      CHANGING
        cs_context = ls_context ).

    cl_abap_unit_assert=>assert_false( ls_context-raw_prefetch_active ).

    zcl_abapgit_ortec_wapa=>get_raw_prefetch_counters(
      IMPORTING ev_hits = DATA(lv_hits) ev_fallbacks = DATA(lv_fallbacks) ).
    cl_abap_unit_assert=>assert_equals( exp = 0 act = lv_hits ).
    cl_abap_unit_assert=>assert_equals( exp = 1 act = lv_fallbacks ).
  ENDMETHOD.

  METHOD prefetch_no_pages_is_trivial_hit.
    DATA ls_context TYPE zcl_abapgit_ortec_wapa=>ty_context.
    DATA lt_pages   TYPE o2pagelist.

    " an all-controller WAPA - nothing ever needs O2PAGCON content at all.
    INSERT VALUE #( applname = 'ZTST' pagekey = 'PAGE1' pagetype = 'C' ) INTO TABLE ls_context-page_dirs.
    ls_context-name = 'ZTST'.
    APPEND VALUE #( pagekey = 'PAGE1' ) TO lt_pages.

    zcl_abapgit_ortec_wapa=>try_raw_prefetch(
      EXPORTING
        it_pages   = lt_pages
      CHANGING
        cs_context = ls_context ).

    cl_abap_unit_assert=>assert_true( ls_context-raw_prefetch_active ).
    cl_abap_unit_assert=>assert_initial( ls_context-raw_content ).
  ENDMETHOD.

  METHOD counters_reset_to_zero.
    zcl_abapgit_ortec_wapa=>get_raw_prefetch_counters(
      IMPORTING ev_hits = DATA(lv_hits) ev_fallbacks = DATA(lv_fallbacks) ).
    cl_abap_unit_assert=>assert_equals( exp = 0 act = lv_hits ).
    cl_abap_unit_assert=>assert_equals( exp = 0 act = lv_fallbacks ).
  ENDMETHOD.

ENDCLASS.
