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
  METHOD no_dump. DATA lv TYPE abap_bool. lv = zcl_abapgit_ortec_git_switch=>is_active_for_repo( 'https://dummy.test/repo.git' ). ENDMETHOD.
ENDCLASS.

CLASS ltcl_repo_state DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_url TYPE string VALUE 'https://test.example.com/repo.git'.
    METHODS setup. METHODS teardown.
    METHODS get_or_create_key        FOR TESTING RAISING cx_static_check.
    METHODS get_or_create_idempotent FOR TESTING RAISING cx_static_check.
    METHODS state_roundtrip          FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_repo_state IMPLEMENTATION.
  METHOD setup.
    DATA lv_key TYPE c LENGTH 12.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    IF lv_key IS NOT INITIAL. zcl_abapgit_ortec_repo_state=>clear_state( lv_key ). ENDIF.
  ENDMETHOD.
  METHOD teardown.
    DATA lv_key TYPE c LENGTH 12.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    IF lv_key IS NOT INITIAL.
      zcl_abapgit_ortec_repo_state=>clear_state( lv_key ).
      DELETE FROM zaog_obj_store WHERE repo_key = lv_key.
    ENDIF. ROLLBACK WORK.
  ENDMETHOD.
  METHOD get_or_create_key.
    DATA lv_key TYPE c LENGTH 12.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    cl_abap_unit_assert=>assert_not_initial( act = lv_key msg = 'Key must be generated' ).
  ENDMETHOD.
  METHOD get_or_create_idempotent.
    DATA lv1 TYPE c LENGTH 12. DATA lv2 TYPE c LENGTH 12.
    lv1 = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    lv2 = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    cl_abap_unit_assert=>assert_equals( act = lv1 exp = lv2 msg = 'Must be idempotent' ).
  ENDMETHOD.
  METHOD state_roundtrip.
    DATA lv_key TYPE c LENGTH 12. DATA ls_state TYPE zcl_abapgit_ortec_repo_state=>ty_state.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    zcl_abapgit_ortec_repo_state=>update_after_fetch( iv_repo_key = lv_key
      iv_branch_name = 'refs/heads/main' iv_url = mc_url
      iv_commit = 'aabbccddee00112233445566778899aabbccddee' ).
    DATA lv_found TYPE c LENGTH 12.
    lv_found = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( mc_url ).
    cl_abap_unit_assert=>assert_equals( act = lv_found exp = lv_key msg = 'DB lookup should find key' ).
    ls_state = zcl_abapgit_ortec_repo_state=>get_state( iv_repo_key = lv_key iv_branch_name = 'refs/heads/main' ).
    cl_abap_unit_assert=>assert_equals( act = ls_state-fetch_commit
      exp = 'aabbccddee00112233445566778899aabbccddee' msg = 'Commit must match' ).
  ENDMETHOD.
ENDCLASS.

CLASS ltcl_persist_flow DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_url TYPE string VALUE 'https://test-persist.example.com/repo.git'.
    METHODS setup. METHODS teardown.
    METHODS persist_creates_state FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_persist_flow IMPLEMENTATION.
  METHOD setup.
    DATA lv_key TYPE c LENGTH 12.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    IF lv_key IS NOT INITIAL.
      zcl_abapgit_ortec_repo_state=>clear_state( lv_key ).
      DELETE FROM zaog_obj_store WHERE repo_key = lv_key.
    ENDIF.
  ENDMETHOD.
  METHOD teardown.
    DATA lv_key TYPE c LENGTH 12.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    IF lv_key IS NOT INITIAL.
      zcl_abapgit_ortec_repo_state=>clear_state( lv_key ).
      DELETE FROM zaog_obj_store WHERE repo_key = lv_key.
    ENDIF. ROLLBACK WORK.
  ENDMETHOD.
  METHOD persist_creates_state.
    DATA lt_obj TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_key TYPE c LENGTH 12.
    ls_obj-sha1 = 'aabbccddee00112233445566778899aabbccddee'. ls_obj-type = 'commit'. ls_obj-data = '436F6D6D6974'. APPEND ls_obj TO lt_obj.
    ls_obj-sha1 = '1122334455667788990011223344556677889900'. ls_obj-type = 'blob'. ls_obj-data = '48656C6C6F'. APPEND ls_obj TO lt_obj.
    TRY.
        zcl_abapgit_ortec_fastpath=>persist_pull_result( iv_url = mc_url iv_branch_name = 'refs/heads/main'
          iv_commit = 'aabbccddee00112233445566778899aabbccddee' it_objects = lt_obj ).
      CATCH zcx_abapgit_ortec_git. RETURN.
    ENDTRY.
    lv_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( mc_url ).
    IF lv_key IS INITIAL. RETURN. ENDIF.
    cl_abap_unit_assert=>assert_equals( exp = abap_true msg = 'Commit stored'
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = lv_key iv_sha1 = 'aabbccddee00112233445566778899aabbccddee' ) ).
    cl_abap_unit_assert=>assert_equals( exp = abap_true msg = 'Blob stored'
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = lv_key iv_sha1 = '1122334455667788990011223344556677889900' ) ).
    DATA ls_state TYPE zcl_abapgit_ortec_repo_state=>ty_state.
    ls_state = zcl_abapgit_ortec_repo_state=>get_state( iv_repo_key = lv_key iv_branch_name = 'refs/heads/main' ).
    cl_abap_unit_assert=>assert_equals( act = ls_state-fetch_commit
      exp = 'aabbccddee00112233445566778899aabbccddee' msg = 'State commit must match' ).
  ENDMETHOD.
ENDCLASS.

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

"! Tests for {@link ZCL_ABAPGIT_ORTEC_PACK_DEC}.
"! Verifies the full decode_and_persist and crash-resume flow.
"! All data is isolated by MC_REPO. Tests access real DB tables
"! (no SQL test doubles needed; cleanup is done in setup/teardown).
CLASS ltcl_pack_decoder DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    "! Repo key used to isolate all test DB writes
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOG_TEST_PD'.
    "! Fixed pack ID used when simulating a crashed session
    CONSTANTS mc_pack TYPE c LENGTH 32 VALUE 'TESTPACK00000000000000000000001A'.
    METHODS setup.
    METHODS teardown.
    "! decode_and_persist with pre-decoded objects: verifies fast path writes
    "! all five tables and cleans up raw_pack on success.
    METHODS decode_populates_all FOR TESTING RAISING cx_static_check.
    "! decode_and_persist without pre-decoded objects: exercises resumable_decode
    "! (full decompression) and verifies the same post-conditions.
    METHODS decode_from_pack     FOR TESTING RAISING cx_static_check.
    "! resume_decode when active session + raw_pack exist: re-decodes and
    "! persists all objects, marks session complete, removes raw_pack.
    METHODS resume_after_partial FOR TESTING RAISING cx_static_check.
    "! resume_decode with no active session: must return empty, no side effects.
    METHODS resume_no_session    FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_pack_decoder IMPLEMENTATION.
  METHOD setup.
    DELETE FROM zaog_obj_store  WHERE repo_key = mc_repo.
    DELETE FROM zaog_pack_meta  WHERE repo_key = mc_repo.
    DELETE FROM zaog_pack_idx   WHERE repo_key = mc_repo.
    DELETE FROM zaog_fetch_sess WHERE repo_key = mc_repo.
    DELETE FROM zaog_raw_pack   WHERE repo_key = mc_repo.
  ENDMETHOD.
  METHOD teardown.
    DELETE FROM zaog_obj_store  WHERE repo_key = mc_repo.
    DELETE FROM zaog_pack_meta  WHERE repo_key = mc_repo.
    DELETE FROM zaog_pack_idx   WHERE repo_key = mc_repo.
    DELETE FROM zaog_fetch_sess WHERE repo_key = mc_repo.
    DELETE FROM zaog_raw_pack   WHERE repo_key = mc_repo.
    ROLLBACK WORK.
  ENDMETHOD.

  METHOD decode_populates_all.
    DATA lt_obj  TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj  TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_pack TYPE xstring.
    ls_obj-sha1 = 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d'.
    ls_obj-type = 'blob'. ls_obj-data = '48656C6C6F'. ls_obj-index = 1.
    APPEND ls_obj TO lt_obj.
    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).
    DATA lt_res TYPE zif_abapgit_definitions=>ty_objects_tt.
    lt_res = zcl_abapgit_ortec_pack_dec=>decode_and_persist(
      iv_data    = lv_pack
      iv_repo_key = mc_repo
      it_objects  = lt_obj ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_res ) exp = 1 msg = '1 object returned' ).
    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = mc_repo iv_sha1 = 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d' )
      msg = 'Object must be in obj_store' ).
    DATA ls_meta TYPE zaog_pack_meta.
    SELECT SINGLE * FROM zaog_pack_meta INTO ls_meta WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_subrc( msg = 'pack_meta must exist' ).
    cl_abap_unit_assert=>assert_equals( act = ls_meta-status exp = 'C' msg = 'pack_meta complete' ).
    DATA lv_idx TYPE i.
    SELECT COUNT(*) FROM zaog_pack_idx INTO lv_idx WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_idx exp = 1 msg = 'pack_idx has 1 entry' ).
    DATA ls_sess TYPE zaog_fetch_sess.
    SELECT SINGLE * FROM zaog_fetch_sess INTO ls_sess WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_subrc( msg = 'fetch_sess must exist' ).
    cl_abap_unit_assert=>assert_equals( act = ls_sess-status exp = 'C' msg = 'session complete' ).
    DATA lv_raw TYPE i.
    SELECT COUNT(*) FROM zaog_raw_pack INTO lv_raw WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_raw exp = 0 msg = 'raw_pack cleaned up' ).
  ENDMETHOD.

  METHOD decode_from_pack.
    " it_objects intentionally NOT supplied: exercises resumable_decode.
    DATA lt_obj  TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj  TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_pack TYPE xstring.
    DATA lt_res  TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_sha1 TYPE zif_abapgit_git_definitions=>ty_sha1.
    ls_obj-data  = '48656C6C6F'. " ASCII: Hello
    ls_obj-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_obj-sha1  = zcl_abapgit_hash=>sha1( iv_type = ls_obj-type iv_data = ls_obj-data ).
    ls_obj-index = 1. APPEND ls_obj TO lt_obj.
    lv_sha1 = ls_obj-sha1.
    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).
    lt_res = zcl_abapgit_ortec_pack_dec=>decode_and_persist(
      iv_data     = lv_pack
      iv_repo_key = mc_repo ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_res ) exp = 1 msg = '1 object decoded from pack' ).
    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = mc_repo iv_sha1 = lv_sha1 )
      msg = 'Decoded object in obj_store' ).
    DATA ls_sess TYPE zaog_fetch_sess.
    SELECT SINGLE * FROM zaog_fetch_sess INTO ls_sess WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_subrc( msg = 'Session exists' ).
    cl_abap_unit_assert=>assert_equals( act = ls_sess-status exp = 'C' msg = 'Session complete' ).
    DATA lv_raw TYPE i.
    SELECT COUNT(*) FROM zaog_raw_pack INTO lv_raw WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_raw exp = 0 msg = 'raw_pack cleaned up after decode' ).
  ENDMETHOD.

  METHOD resume_after_partial.
    " Simulate crash: raw_pack stored + active session, nothing in obj_store yet.
    DATA lt_obj  TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj  TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_pack TYPE xstring.
    DATA ls_raw  TYPE zaog_raw_pack.
    DATA ls_sess TYPE zaog_fetch_sess.
    DATA lt_res  TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_sha1 TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_ts   TYPE timestampl.
    ls_obj-data  = '48656C6C6F'. " ASCII: Hello
    ls_obj-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_obj-sha1  = zcl_abapgit_hash=>sha1( iv_type = ls_obj-type iv_data = ls_obj-data ).
    ls_obj-index = 1. APPEND ls_obj TO lt_obj.
    lv_sha1 = ls_obj-sha1.
    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).
    " Insert crash-state into DB
    ls_raw-repo_key = mc_repo. ls_raw-pack_id = mc_pack. ls_raw-raw_data = lv_pack.
    MODIFY zaog_raw_pack FROM ls_raw.
    GET TIME STAMP FIELD lv_ts.
    ls_sess-session_id = 'RESSESTEST000000000000000000001A'.
    ls_sess-repo_key   = mc_repo. ls_sess-pack_id    = mc_pack.
    ls_sess-phase      = 'D'.     ls_sess-obj_done   = 0. ls_sess-obj_total = 1.
    ls_sess-status     = 'A'.     ls_sess-created_at = lv_ts. ls_sess-updated_at = lv_ts.
    INSERT zaog_fetch_sess FROM ls_sess.
    COMMIT WORK.
    " Resume
    lt_res = zcl_abapgit_ortec_pack_dec=>resume_decode( mc_repo ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_res ) exp = 1 msg = '1 object after resume' ).
    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = mc_repo iv_sha1 = lv_sha1 )
      msg = 'Object in obj_store after resume' ).
    SELECT SINGLE * FROM zaog_fetch_sess INTO ls_sess WHERE repo_key = mc_repo AND status = 'C'.
    cl_abap_unit_assert=>assert_subrc( msg = 'Session must be complete after resume' ).
    DATA lv_raw TYPE i.
    SELECT COUNT(*) FROM zaog_raw_pack INTO lv_raw WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_raw exp = 0 msg = 'raw_pack cleaned up after resume' ).
  ENDMETHOD.

  METHOD resume_no_session.
    DATA lt_res TYPE zif_abapgit_definitions=>ty_objects_tt.
    lt_res = zcl_abapgit_ortec_pack_dec=>resume_decode( mc_repo ).
    cl_abap_unit_assert=>assert_initial( act = lt_res msg = 'No session = empty result' ).
  ENDMETHOD.
ENDCLASS.

CLASS ltcl_fetch_neg DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_url TYPE string VALUE 'https://test-neg.example.com/repo.git'.
    METHODS setup. METHODS teardown.
    METHODS no_state_no_haves FOR TESTING RAISING cx_static_check.
    METHODS want_excluded     FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_fetch_neg IMPLEMENTATION.
  METHOD setup.
    DATA lv_key TYPE c LENGTH 12.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    IF lv_key IS NOT INITIAL. zcl_abapgit_ortec_repo_state=>clear_state( lv_key ). DELETE FROM zaog_obj_store WHERE repo_key = lv_key. ENDIF.
  ENDMETHOD.
  METHOD teardown.
    DATA lv_key TYPE c LENGTH 12.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    IF lv_key IS NOT INITIAL. zcl_abapgit_ortec_repo_state=>clear_state( lv_key ). DELETE FROM zaog_obj_store WHERE repo_key = lv_key. ENDIF.
    ROLLBACK WORK.
  ENDMETHOD.
  METHOD no_state_no_haves.
    DATA lt_wants TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_haves TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND 'aaaa000000000000000000000000000000000001' TO lt_wants.
    TRY. lt_haves = zcl_abapgit_ortec_fetch_neg=>get_have_commits( iv_url = mc_url it_want_hashes = lt_wants ). CATCH zcx_abapgit_ortec_git. ENDTRY.
    cl_abap_unit_assert=>assert_initial( act = lt_haves msg = 'No state = no haves' ).
  ENDMETHOD.
  METHOD want_excluded.
    DATA lv_key TYPE c LENGTH 12.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    TRY.
        zcl_abapgit_ortec_repo_state=>update_after_fetch( iv_repo_key = lv_key iv_branch_name = 'refs/heads/main'
          iv_url = mc_url iv_commit = 'eeee000000000000000000000000000000000003' ).
        zcl_abapgit_ortec_obj_store=>store_object( iv_repo_key = lv_key iv_sha1 = 'eeee000000000000000000000000000000000003' iv_type = 'commit' iv_data = 'CC' ).
      CATCH zcx_abapgit_ortec_git. RETURN.
    ENDTRY. COMMIT WORK.
    DATA lt_wants TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND 'eeee000000000000000000000000000000000003' TO lt_wants.
    DATA lt_haves TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    TRY. lt_haves = zcl_abapgit_ortec_fetch_neg=>get_have_commits( iv_url = mc_url it_want_hashes = lt_wants ). CATCH zcx_abapgit_ortec_git. RETURN. ENDTRY.
    READ TABLE lt_haves WITH KEY table_line = 'eeee000000000000000000000000000000000003' TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( exp = 4 msg = 'Want SHA excluded from haves' ).
  ENDMETHOD.
ENDCLASS.

CLASS ltcl_git_roundtrip DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION. METHODS encode_decode FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_git_roundtrip IMPLEMENTATION.
  METHOD encode_decode.
    DATA lt_obj TYPE zif_abapgit_definitions=>ty_objects_tt. DATA ls_obj TYPE zif_abapgit_definitions=>ty_object.
    ls_obj-sha1 = zcl_abapgit_hash=>sha1_blob( CONV xstring( '48656C6C6F' ) ).
    ls_obj-type = 'blob'. ls_obj-data = '48656C6C6F'. ls_obj-index = 1. APPEND ls_obj TO lt_obj.
    DATA lv_pack TYPE xstring.
    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).
    cl_abap_unit_assert=>assert_not_initial( act = lv_pack ).
    DATA lt_dec TYPE zif_abapgit_definitions=>ty_objects_tt.
    lt_dec = zcl_abapgit_git_pack=>decode( lv_pack ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_dec ) exp = 1 ).
    cl_abap_unit_assert=>assert_equals( act = lt_dec[ 1 ]-sha1 exp = ls_obj-sha1 ).
    cl_abap_unit_assert=>assert_equals( act = lt_dec[ 1 ]-data exp = '48656C6C6F' ).
  ENDMETHOD.
ENDCLASS.
