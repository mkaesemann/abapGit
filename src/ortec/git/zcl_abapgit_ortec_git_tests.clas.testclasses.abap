CLASS ltcl_obj_store DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOG_TEST_01'.
    METHODS setup. METHODS teardown.
    METHODS store_and_get FOR TESTING RAISING cx_static_check.
    METHODS not_found FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_obj_store IMPLEMENTATION.
  METHOD setup. DELETE FROM zaog_obj_store WHERE repo_key = mc_repo. ENDMETHOD.
  METHOD teardown. DELETE FROM zaog_obj_store WHERE repo_key = mc_repo. ROLLBACK WORK. ENDMETHOD.
  METHOD store_and_get.
    DATA lv TYPE xstring. DATA ls TYPE zif_abapgit_definitions=>ty_object. lv = '48656C6C6F'.
    zcl_abapgit_ortec_obj_store=>store_object( iv_repo_key = mc_repo iv_sha1 = 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d' iv_type = 'blob' iv_data = lv ).
    ls = zcl_abapgit_ortec_obj_store=>get_object( iv_repo_key = mc_repo iv_sha1 = 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d' ).
    cl_abap_unit_assert=>assert_equals( act = ls-sha1 exp = 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d' ).
  ENDMETHOD.
  METHOD not_found.
    TRY. zcl_abapgit_ortec_obj_store=>get_object( iv_repo_key = mc_repo iv_sha1 = 'ffffffffffffffffffffffffffffffffffffffff' ). cl_abap_unit_assert=>fail( ). CATCH zcx_abapgit_ortec_git. ENDTRY.
  ENDMETHOD.
ENDCLASS.
CLASS ltcl_switch DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION. METHODS no_dump FOR TESTING.
ENDCLASS.
CLASS ltcl_switch IMPLEMENTATION.
  METHOD no_dump. zcl_abapgit_ortec_git_switch=>reset( ). DATA lv TYPE abap_bool. lv = zcl_abapgit_ortec_git_switch=>is_active( ). ENDMETHOD.
ENDCLASS.
