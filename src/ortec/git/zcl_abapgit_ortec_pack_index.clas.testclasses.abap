CLASS ltcl_pack_index DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOG_TEST_IX'.
    CONSTANTS mc_pack TYPE c LENGTH 32 VALUE 'TESTPACK00000000000000000000001A'.
    METHODS setup. METHODS teardown.
    METHODS store_and_get FOR TESTING RAISING cx_static_check.
    METHODS mark_decoded  FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_pack_index IMPLEMENTATION.
  METHOD setup. zcl_abapgit_ortec_pack_index=>cleanup_repo( mc_repo ). ENDMETHOD.
  METHOD teardown. zcl_abapgit_ortec_pack_index=>cleanup_repo( mc_repo ). ROLLBACK WORK. ENDMETHOD.
  METHOD store_and_get.
    DATA lt_e TYPE zcl_abapgit_ortec_pack_index=>tty_index_entries. DATA ls_e TYPE zcl_abapgit_ortec_pack_index=>ty_index_entry.
    ls_e-obj_index = 1. ls_e-obj_sha1 = 'aa11223344556677889900aabbccddeeff001122'. ls_e-obj_type = 'blob'. ls_e-uncomp_len = 100. ls_e-dec_status = 'P'. APPEND ls_e TO lt_e.
    ls_e-obj_index = 2. ls_e-obj_sha1 = 'bb11223344556677889900aabbccddeeff001122'. ls_e-obj_type = 'commit'. ls_e-dec_status = 'D'. APPEND ls_e TO lt_e.
    zcl_abapgit_ortec_pack_index=>store_entries( iv_repo_key = mc_repo iv_pack_id = mc_pack it_entries = lt_e ).
    DATA lt_p TYPE zcl_abapgit_ortec_pack_index=>tty_index_entries.
    lt_p = zcl_abapgit_ortec_pack_index=>get_pending( iv_repo_key = mc_repo iv_pack_id = mc_pack ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_p ) exp = 1 msg = 'Only 1 pending' ).
  ENDMETHOD.
  METHOD mark_decoded.
    DATA lt_e TYPE zcl_abapgit_ortec_pack_index=>tty_index_entries. DATA ls_e TYPE zcl_abapgit_ortec_pack_index=>ty_index_entry.
    ls_e-obj_index = 1. ls_e-obj_type = 'blob'. ls_e-dec_status = 'P'. APPEND ls_e TO lt_e.
    zcl_abapgit_ortec_pack_index=>store_entries( iv_repo_key = mc_repo iv_pack_id = mc_pack it_entries = lt_e ).
    zcl_abapgit_ortec_pack_index=>mark_decoded( iv_repo_key = mc_repo iv_pack_id = mc_pack iv_obj_index = 1 iv_obj_sha1 = 'cc11223344556677889900aabbccddeeff001122' ).
    DATA lt_p TYPE zcl_abapgit_ortec_pack_index=>tty_index_entries.
    lt_p = zcl_abapgit_ortec_pack_index=>get_pending( iv_repo_key = mc_repo iv_pack_id = mc_pack ).
    cl_abap_unit_assert=>assert_initial( act = lt_p msg = 'No pending after mark' ).
  ENDMETHOD.
ENDCLASS.
