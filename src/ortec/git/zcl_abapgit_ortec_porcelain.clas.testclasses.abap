CLASS zcl_abapgit_ortec_porcelain DEFINITION LOCAL FRIENDS ltcl_porcelain.

CLASS ltcl_porcelain DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  PRIVATE SECTION.
    METHODS setup.
    METHODS teardown.

    METHODS one_blob_one_path    FOR TESTING RAISING cx_static_check.
    METHODS same_blob_two_paths  FOR TESTING RAISING cx_static_check.
    METHODS dup_blob_obj_once    FOR TESTING RAISING cx_static_check.
    METHODS unrelated_blob_skip  FOR TESTING RAISING cx_static_check.
    METHODS non_blob_obj_skip    FOR TESTING RAISING cx_static_check.
    METHODS blob_data_sha_kept   FOR TESTING RAISING cx_static_check.
ENDCLASS.

CLASS ltcl_porcelain IMPLEMENTATION.
  METHOD setup.
  ENDMETHOD.

  METHOD teardown.
  ENDMETHOD.

  METHOD one_blob_one_path.
    DATA lt_manifest TYPE zif_abapgit_git_definitions=>ty_expanded_tt.
    DATA ls_manifest LIKE LINE OF lt_manifest.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object  LIKE LINE OF lt_objects.
    DATA lt_files   TYPE zif_abapgit_git_definitions=>ty_files_tt.
    DATA lv_data    TYPE xstring.

    lv_data = zcl_abapgit_convert=>string_to_xstring_utf8( 'a' ).
    ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-sha1 = zcl_abapgit_hash=>sha1_blob( lv_data ).
    ls_object-data = lv_data.
    APPEND ls_object TO lt_objects.

    ls_manifest-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_manifest-path = '/'.
    ls_manifest-name = 'a.txt'.
    ls_manifest-sha1 = ls_object-sha1.
    APPEND ls_manifest TO lt_manifest.

    zcl_abapgit_ortec_porcelain=>materialize_from_manifest(
      EXPORTING
        it_objects       = lt_objects
        it_blob_manifest = lt_manifest
      CHANGING
        ct_files         = lt_files ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_files ) exp = 1 ).
    cl_abap_unit_assert=>assert_equals( act = lt_files[ 1 ]-sha1 exp = ls_object-sha1 ).
  ENDMETHOD.

  METHOD same_blob_two_paths.
    DATA lt_manifest TYPE zif_abapgit_git_definitions=>ty_expanded_tt.
    DATA ls_manifest LIKE LINE OF lt_manifest.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object  LIKE LINE OF lt_objects.
    DATA lt_files   TYPE zif_abapgit_git_definitions=>ty_files_tt.
    DATA lv_data    TYPE xstring.

    lv_data = zcl_abapgit_convert=>string_to_xstring_utf8( 'same' ).
    ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-sha1 = zcl_abapgit_hash=>sha1_blob( lv_data ).
    ls_object-data = lv_data.
    APPEND ls_object TO lt_objects.

    ls_manifest-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_manifest-path = '/dir1/'.
    ls_manifest-name = 'a.txt'.
    ls_manifest-sha1 = ls_object-sha1.
    APPEND ls_manifest TO lt_manifest.

    ls_manifest-path = '/dir2/'.
    ls_manifest-name = 'b.txt'.
    APPEND ls_manifest TO lt_manifest.

    zcl_abapgit_ortec_porcelain=>materialize_from_manifest(
      EXPORTING
        it_objects       = lt_objects
        it_blob_manifest = lt_manifest
      CHANGING
        ct_files         = lt_files ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_files ) exp = 2 ).
  ENDMETHOD.

  METHOD dup_blob_obj_once.
    DATA lt_manifest TYPE zif_abapgit_git_definitions=>ty_expanded_tt.
    DATA ls_manifest LIKE LINE OF lt_manifest.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object  LIKE LINE OF lt_objects.
    DATA lt_files   TYPE zif_abapgit_git_definitions=>ty_files_tt.
    DATA lv_data    TYPE xstring.

    lv_data = zcl_abapgit_convert=>string_to_xstring_utf8( 'dup' ).
    ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-sha1 = zcl_abapgit_hash=>sha1_blob( lv_data ).
    ls_object-data = lv_data.
    APPEND ls_object TO lt_objects.

    ls_manifest-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_manifest-path = '/'.
    ls_manifest-name = 'a.txt'.
    ls_manifest-sha1 = ls_object-sha1.
    APPEND ls_manifest TO lt_manifest.

    ls_manifest-name = 'b.txt'.
    APPEND ls_manifest TO lt_manifest.

    zcl_abapgit_ortec_porcelain=>materialize_from_manifest(
      EXPORTING
        it_objects       = lt_objects
        it_blob_manifest = lt_manifest
      CHANGING
        ct_files         = lt_files ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_files ) exp = 2 ).
  ENDMETHOD.

  METHOD unrelated_blob_skip.
    DATA lt_manifest TYPE zif_abapgit_git_definitions=>ty_expanded_tt.
    DATA ls_manifest LIKE LINE OF lt_manifest.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object  LIKE LINE OF lt_objects.
    DATA lt_files   TYPE zif_abapgit_git_definitions=>ty_files_tt.
    DATA lv_data    TYPE xstring.

    lv_data = zcl_abapgit_convert=>string_to_xstring_utf8( 'blob' ).
    ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-sha1 = zcl_abapgit_hash=>sha1_blob( lv_data ).
    ls_object-data = lv_data.
    APPEND ls_object TO lt_objects.

    ls_manifest-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_manifest-path = '/'.
    ls_manifest-name = 'a.txt'.
    ls_manifest-sha1 = '1111111111111111111111111111111111111111'.
    APPEND ls_manifest TO lt_manifest.

    zcl_abapgit_ortec_porcelain=>materialize_from_manifest(
      EXPORTING
        it_objects       = lt_objects
        it_blob_manifest = lt_manifest
      CHANGING
        ct_files         = lt_files ).

    cl_abap_unit_assert=>assert_initial( act = lt_files ).
  ENDMETHOD.

  METHOD non_blob_obj_skip.
    DATA lt_manifest TYPE zif_abapgit_git_definitions=>ty_expanded_tt.
    DATA ls_manifest LIKE LINE OF lt_manifest.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object  LIKE LINE OF lt_objects.
    DATA lt_files   TYPE zif_abapgit_git_definitions=>ty_files_tt.

    ls_object-type = zif_abapgit_git_definitions=>c_type-tree.
    ls_object-sha1 = '1111111111111111111111111111111111111111'.
    APPEND ls_object TO lt_objects.

    ls_manifest-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_manifest-path = '/'.
    ls_manifest-name = 'a.txt'.
    ls_manifest-sha1 = ls_object-sha1.
    APPEND ls_manifest TO lt_manifest.

    zcl_abapgit_ortec_porcelain=>materialize_from_manifest(
      EXPORTING
        it_objects       = lt_objects
        it_blob_manifest = lt_manifest
      CHANGING
        ct_files         = lt_files ).

    cl_abap_unit_assert=>assert_initial( act = lt_files ).
  ENDMETHOD.

  METHOD blob_data_sha_kept.
    DATA lt_manifest TYPE zif_abapgit_git_definitions=>ty_expanded_tt.
    DATA ls_manifest LIKE LINE OF lt_manifest.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object  LIKE LINE OF lt_objects.
    DATA lt_files   TYPE zif_abapgit_git_definitions=>ty_files_tt.
    DATA lv_data    TYPE xstring.

    lv_data = zcl_abapgit_convert=>string_to_xstring_utf8( 'data' ).
    ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-sha1 = zcl_abapgit_hash=>sha1_blob( lv_data ).
    ls_object-data = lv_data.
    APPEND ls_object TO lt_objects.

    ls_manifest-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_manifest-path = '/'.
    ls_manifest-name = 'a.txt'.
    ls_manifest-sha1 = ls_object-sha1.
    APPEND ls_manifest TO lt_manifest.

    zcl_abapgit_ortec_porcelain=>materialize_from_manifest(
      EXPORTING
        it_objects       = lt_objects
        it_blob_manifest = lt_manifest
      CHANGING
        ct_files         = lt_files ).

    cl_abap_unit_assert=>assert_equals( act = lt_files[ 1 ]-data exp = lv_data ).
    cl_abap_unit_assert=>assert_equals( act = lt_files[ 1 ]-sha1 exp = ls_object-sha1 ).
  ENDMETHOD.
ENDCLASS.

