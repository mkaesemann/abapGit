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
