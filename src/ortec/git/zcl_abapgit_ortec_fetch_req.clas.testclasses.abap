CLASS ltcl_fetch_req DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  PRIVATE SECTION.
    CONSTANTS c_tip   TYPE zif_abapgit_git_definitions=>ty_sha1 VALUE 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'.
    CONSTANTS c_have1 TYPE zif_abapgit_git_definitions=>ty_sha1 VALUE 'BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB'.
    CONSTANTS c_have2 TYPE zif_abapgit_git_definitions=>ty_sha1 VALUE 'CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC'.
    CONSTANTS c_blob1 TYPE zif_abapgit_git_definitions=>ty_sha1 VALUE 'DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD'.
    CONSTANTS c_blob2 TYPE zif_abapgit_git_definitions=>ty_sha1 VALUE 'EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE'.

    CONSTANTS c_caps_full TYPE string
      VALUE 'multi_ack side-band-64k filter thin-pack ofs-delta allow-reachable-sha1-in-want allow-tip-sha1-in-want'.
    CONSTANTS c_caps_none TYPE string VALUE 'multi_ack side-band-64k'.

    METHODS assert_no_deepen
      IMPORTING iv_buffer TYPE string.

    METHODS blobless_requires_filter_want FOR TESTING RAISING cx_static_check.
    METHODS blobless_no_haves_no_deepen FOR TESTING RAISING cx_static_check.
    METHODS blobless_missing_filter_raises FOR TESTING RAISING cx_static_check.

    METHODS thin_wants_and_haves FOR TESTING RAISING cx_static_check.
    METHODS thin_advertised_and_haves_used FOR TESTING RAISING cx_static_check.
    METHODS thin_no_haves_no_thin_capa FOR TESTING RAISING cx_static_check.
    METHODS thin_caps_absent_no_thin_capa FOR TESTING RAISING cx_static_check.

    METHODS self_contained_uses_haves FOR TESTING RAISING cx_static_check.
    METHODS self_contained_never_thin FOR TESTING RAISING cx_static_check.

    METHODS materialize_wants_and_bounds FOR TESTING RAISING cx_static_check.
    METHODS materialize_missing_capa_raise FOR TESTING RAISING cx_static_check.
    METHODS materialize_over_max_raises FOR TESTING RAISING cx_static_check.
    METHODS materialize_empty_raises FOR TESTING RAISING cx_static_check.
    METHODS capability_intersection FOR TESTING RAISING cx_static_check.

    METHODS recovery_minimal_and_no_haves FOR TESTING RAISING cx_static_check.

    METHODS parse_capability_extracts_line FOR TESTING RAISING cx_static_check.
    METHODS parse_capabilities_no_null FOR TESTING RAISING cx_static_check.
    METHODS parse_capability_no_trailng_nl FOR TESTING RAISING cx_static_check.

ENDCLASS.


CLASS ltcl_fetch_req IMPLEMENTATION.

  METHOD assert_no_deepen.
    cl_abap_unit_assert=>assert_false( xsdbool( iv_buffer CS 'deepen' ) ).
  ENDMETHOD.

  METHOD blobless_requires_filter_want.
    DATA lt_want TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND c_tip TO lt_want.

    DATA(ls_request) = zcl_abapgit_ortec_fetch_req=>build_request(
      iv_mode        = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-initial_branch_blobless
      it_want_hashes = lt_want
      iv_server_caps = c_caps_full ).

    cl_abap_unit_assert=>assert_true( xsdbool( ls_request-buffer CS |want { c_tip }| ) ).
    cl_abap_unit_assert=>assert_true( xsdbool( ls_request-buffer CS 'side-band-64k no-progress multi_ack filter' ) ).
    cl_abap_unit_assert=>assert_true( xsdbool( ls_request-buffer CS 'filter blob:none' ) ).
    cl_abap_unit_assert=>assert_equals( exp = 'blob:none' act = ls_request-used_filter ).
  ENDMETHOD.

  METHOD blobless_no_haves_no_deepen.
    DATA lt_want TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND c_tip TO lt_want.

    DATA(ls_request) = zcl_abapgit_ortec_fetch_req=>build_request(
      iv_mode        = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-initial_branch_blobless
      it_want_hashes = lt_want
      iv_server_caps = c_caps_full ).

    assert_no_deepen( ls_request-buffer ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'have ' ) ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'shallow ' ) ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'thin-pack' ) ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'ofs-delta' ) ).
    cl_abap_unit_assert=>assert_equals( exp = 0 act = ls_request-have_count ).
  ENDMETHOD.

  METHOD blobless_missing_filter_raises.
    DATA lt_want TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND c_tip TO lt_want.

    TRY.
        zcl_abapgit_ortec_fetch_req=>build_request(
          iv_mode        = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-initial_branch_blobless
          it_want_hashes = lt_want
          iv_server_caps = c_caps_none ).
        cl_abap_unit_assert=>fail( 'Expected zcx_abapgit_ortec_git' ).
      CATCH zcx_abapgit_ortec_git INTO DATA(lx_ortec).
        cl_abap_unit_assert=>assert_true( lx_ortec->mv_unsupported_capability ).
        cl_abap_unit_assert=>assert_equals( exp = 'filter' act = lx_ortec->mv_missing_capability ).
    ENDTRY.
  ENDMETHOD.

  METHOD thin_wants_and_haves.
    DATA lt_want  TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_haves TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND c_tip TO lt_want.
    APPEND c_have1 TO lt_haves.
    APPEND c_have2 TO lt_haves.

    DATA(ls_request) = zcl_abapgit_ortec_fetch_req=>build_request(
      iv_mode            = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-incremental_thin
      it_want_hashes     = lt_want
      it_certified_haves = lt_haves
      iv_server_caps     = c_caps_full ).

    cl_abap_unit_assert=>assert_true( xsdbool( ls_request-buffer CS |want { c_tip }| ) ).
    cl_abap_unit_assert=>assert_true( xsdbool( ls_request-buffer CS |have { c_have1 }| ) ).
    cl_abap_unit_assert=>assert_true( xsdbool( ls_request-buffer CS |have { c_have2 }| ) ).
    cl_abap_unit_assert=>assert_equals( exp = 2 act = ls_request-have_count ).
    assert_no_deepen( ls_request-buffer ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'shallow ' ) ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'filter' ) ).
  ENDMETHOD.

  METHOD thin_advertised_and_haves_used.
    DATA lt_want  TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_haves TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND c_tip TO lt_want.
    APPEND c_have1 TO lt_haves.

    DATA(ls_request) = zcl_abapgit_ortec_fetch_req=>build_request(
      iv_mode            = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-incremental_thin
      it_want_hashes     = lt_want
      it_certified_haves = lt_haves
      iv_server_caps     = c_caps_full ).

    cl_abap_unit_assert=>assert_true( ls_request-used_thin ).
    cl_abap_unit_assert=>assert_true( xsdbool( ls_request-buffer CS 'thin-pack ofs-delta' ) ).
  ENDMETHOD.

  METHOD thin_no_haves_no_thin_capa.
    DATA lt_want TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND c_tip TO lt_want.

    DATA(ls_request) = zcl_abapgit_ortec_fetch_req=>build_request(
      iv_mode        = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-incremental_thin
      it_want_hashes = lt_want
      iv_server_caps = c_caps_full ).

    cl_abap_unit_assert=>assert_false( ls_request-used_thin ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'thin-pack' ) ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'ofs-delta' ) ).
  ENDMETHOD.

  METHOD thin_caps_absent_no_thin_capa.
    DATA lt_want  TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_haves TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND c_tip TO lt_want.
    APPEND c_have1 TO lt_haves.

    DATA(ls_request) = zcl_abapgit_ortec_fetch_req=>build_request(
      iv_mode            = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-incremental_thin
      it_want_hashes     = lt_want
      it_certified_haves = lt_haves
      iv_server_caps     = c_caps_none ).

    cl_abap_unit_assert=>assert_false( ls_request-used_thin ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'thin-pack' ) ).
  ENDMETHOD.

  METHOD self_contained_uses_haves.
    DATA lt_want  TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_haves TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND c_tip TO lt_want.
    APPEND c_have1 TO lt_haves.

    DATA(ls_request) = zcl_abapgit_ortec_fetch_req=>build_request(
      iv_mode            = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-incremental_self_contained
      it_want_hashes     = lt_want
      it_certified_haves = lt_haves
      iv_server_caps     = c_caps_full ).

    cl_abap_unit_assert=>assert_true( xsdbool( ls_request-buffer CS |have { c_have1 }| ) ).
    cl_abap_unit_assert=>assert_equals( exp = 1 act = ls_request-have_count ).
  ENDMETHOD.

  METHOD self_contained_never_thin.
    DATA lt_want  TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_haves TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND c_tip TO lt_want.
    APPEND c_have1 TO lt_haves.

    DATA(ls_request) = zcl_abapgit_ortec_fetch_req=>build_request(
      iv_mode            = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-incremental_self_contained
      it_want_hashes     = lt_want
      it_certified_haves = lt_haves
      iv_server_caps     = c_caps_full ).

    cl_abap_unit_assert=>assert_false( ls_request-used_thin ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'thin-pack' ) ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'ofs-delta' ) ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'filter' ) ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'shallow ' ) ).
    assert_no_deepen( ls_request-buffer ).
  ENDMETHOD.

  METHOD materialize_wants_and_bounds.
    DATA lt_want TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND c_blob1 TO lt_want.
    APPEND c_blob2 TO lt_want.

    DATA(ls_request) = zcl_abapgit_ortec_fetch_req=>build_request(
      iv_mode        = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-materialize_blobs
      it_want_hashes = lt_want
      iv_server_caps = c_caps_full ).

    cl_abap_unit_assert=>assert_true( xsdbool( ls_request-buffer CS |want { c_blob1 }| ) ).
    cl_abap_unit_assert=>assert_true( xsdbool( ls_request-buffer CS |want { c_blob2 }| ) ).
    cl_abap_unit_assert=>assert_true( xsdbool( ls_request-buffer CS 'allow-reachable-sha1-in-want' ) ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'have ' ) ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'shallow ' ) ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'filter' ) ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'thin-pack' ) ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'ofs-delta' ) ).
    assert_no_deepen( ls_request-buffer ).
    cl_abap_unit_assert=>assert_equals( exp = 0 act = ls_request-have_count ).
  ENDMETHOD.

  METHOD materialize_missing_capa_raise.
    DATA lt_want TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND c_blob1 TO lt_want.

    TRY.
        zcl_abapgit_ortec_fetch_req=>build_request(
          iv_mode        = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-materialize_blobs
          it_want_hashes = lt_want
          iv_server_caps = c_caps_none ).
        cl_abap_unit_assert=>fail( 'Expected zcx_abapgit_ortec_git' ).
      CATCH zcx_abapgit_ortec_git INTO DATA(lx_ortec).
        cl_abap_unit_assert=>assert_true( lx_ortec->mv_unsupported_capability ).
        cl_abap_unit_assert=>assert_equals(
          exp = 'allow-reachable-sha1-in-want'
          act = lx_ortec->mv_missing_capability ).
    ENDTRY.
  ENDMETHOD.

  METHOD materialize_over_max_raises.
    DATA lt_want TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DO zcl_abapgit_ortec_fetch_req=>c_materialize_batch_max + 1 TIMES.
      APPEND c_tip TO lt_want.
    ENDDO.

    TRY.
        zcl_abapgit_ortec_fetch_req=>build_request(
          iv_mode        = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-materialize_blobs
          it_want_hashes = lt_want
          iv_server_caps = c_caps_full ).
        cl_abap_unit_assert=>fail( 'Expected zcx_abapgit_ortec_git' ).
      CATCH zcx_abapgit_ortec_git INTO DATA(lx_ortec).
        cl_abap_unit_assert=>assert_false( lx_ortec->mv_unsupported_capability ).
    ENDTRY.
  ENDMETHOD.

  METHOD materialize_empty_raises.
    DATA lt_want TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    TRY.
        zcl_abapgit_ortec_fetch_req=>build_request(
          iv_mode        = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-materialize_blobs
          it_want_hashes = lt_want
          iv_server_caps = c_caps_full ).
        cl_abap_unit_assert=>fail( 'Expected zcx_abapgit_ortec_git' ).
      CATCH zcx_abapgit_ortec_git INTO DATA(lx_ortec).
        cl_abap_unit_assert=>assert_false( lx_ortec->mv_unsupported_capability ).
    ENDTRY.
  ENDMETHOD.

  METHOD capability_intersection.
    " Variant B D2 TIME_OUT fix design §13: the existing suite only covers
    " "both capabilities advertised" (materialize_wants_and_bounds) and
    " "neither advertised" (materialize_missing_capa_raise) - this fills
    " the one missing combination, allow-tip-sha1-in-want ONLY.
    DATA lt_want TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    CONSTANTS lc_caps_tip_only TYPE string
      VALUE 'multi_ack side-band-64k allow-tip-sha1-in-want'.
    APPEND c_blob1 TO lt_want.

    DATA(ls_request) = zcl_abapgit_ortec_fetch_req=>build_request(
      iv_mode        = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-materialize_blobs
      it_want_hashes = lt_want
      iv_server_caps = lc_caps_tip_only ).

    cl_abap_unit_assert=>assert_true( xsdbool( ls_request-buffer CS |want { c_blob1 }| ) ).
    cl_abap_unit_assert=>assert_true( xsdbool( ls_request-buffer CS 'allow-tip-sha1-in-want' ) ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'allow-reachable-sha1-in-want' ) ).
  ENDMETHOD.

  METHOD recovery_minimal_and_no_haves.
    DATA lt_want  TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_haves TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND c_tip TO lt_want.
    APPEND c_have1 TO lt_haves.

    DATA(ls_request) = zcl_abapgit_ortec_fetch_req=>build_request(
      iv_mode            = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-recovery_branch_full
      it_want_hashes     = lt_want
      it_certified_haves = lt_haves
      iv_server_caps     = c_caps_full ).

    cl_abap_unit_assert=>assert_true(
      xsdbool( ls_request-buffer CS |want { c_tip } side-band-64k no-progress multi_ack| ) ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'have ' ) ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'shallow ' ) ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'filter' ) ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'thin-pack' ) ).
    cl_abap_unit_assert=>assert_false( xsdbool( ls_request-buffer CS 'ofs-delta' ) ).
    assert_no_deepen( ls_request-buffer ).
    cl_abap_unit_assert=>assert_equals( exp = 0 act = ls_request-have_count ).
  ENDMETHOD.

  METHOD parse_capability_extracts_line.
    DATA lv_null TYPE c LENGTH 1.
    lv_null = zcl_abapgit_git_utils=>get_null( ).

    DATA(lv_ref_data) = |0000tip{ lv_null }multi_ack side-band-64k filter{ cl_abap_char_utilities=>newline }0000|.

    DATA(lv_caps) = zcl_abapgit_ortec_fetch_req=>parse_capabilities( lv_ref_data ).

    cl_abap_unit_assert=>assert_equals(
      exp = 'multi_ack side-band-64k filter'
      act = lv_caps ).
  ENDMETHOD.

  METHOD parse_capabilities_no_null.
    DATA(lv_caps) = zcl_abapgit_ortec_fetch_req=>parse_capabilities( 'no null byte here' ).
    cl_abap_unit_assert=>assert_initial( lv_caps ).
  ENDMETHOD.

  METHOD parse_capability_no_trailng_nl.
    DATA lv_null TYPE c LENGTH 1.
    lv_null = zcl_abapgit_git_utils=>get_null( ).

    DATA(lv_ref_data) = |0000tip{ lv_null }multi_ack filter|.

    DATA(lv_caps) = zcl_abapgit_ortec_fetch_req=>parse_capabilities( lv_ref_data ).

    cl_abap_unit_assert=>assert_equals(
      exp = 'multi_ack filter'
      act = lv_caps ).
  ENDMETHOD.

ENDCLASS.
