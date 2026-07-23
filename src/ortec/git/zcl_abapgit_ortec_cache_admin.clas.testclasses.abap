CLASS ltcl_cache_admin DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_CADMIN'.
    METHODS setup. METHODS teardown.
    METHODS overview_aggregates_counts FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_cache_admin IMPLEMENTATION.
  METHOD setup.
    DELETE FROM zaog_obj_store  WHERE repo_key = mc_repo.
    DELETE FROM zaog_obj_index  WHERE repo_key = mc_repo.
    DELETE FROM zaog_pack_meta  WHERE repo_key = mc_repo.
    DELETE FROM zaog_commit_hist WHERE repo_key = mc_repo.
    DELETE FROM zaog_fetch_sess WHERE repo_key = mc_repo.
    DELETE FROM zaog_repo_state WHERE repo_key = mc_repo.
  ENDMETHOD.
  METHOD teardown.
    DELETE FROM zaog_obj_store  WHERE repo_key = mc_repo.
    DELETE FROM zaog_obj_index  WHERE repo_key = mc_repo.
    DELETE FROM zaog_pack_meta  WHERE repo_key = mc_repo.
    DELETE FROM zaog_commit_hist WHERE repo_key = mc_repo.
    DELETE FROM zaog_fetch_sess WHERE repo_key = mc_repo.
    DELETE FROM zaog_repo_state WHERE repo_key = mc_repo.
    ROLLBACK WORK.
  ENDMETHOD.
  METHOD overview_aggregates_counts.
    DATA ls_state  TYPE zaog_repo_state.
    DATA ls_idx    TYPE zaog_obj_index.
    DATA ls_pack   TYPE zaog_pack_meta.
    DATA ls_commit TYPE zaog_commit_hist.
    DATA ls_sess   TYPE zaog_fetch_sess.
    DATA lv_ts     TYPE timestampl.
    DATA lv_data   TYPE xstring.

    TRY.

        GET TIME STAMP FIELD lv_ts.

        ls_state-repo_key    = mc_repo.
        ls_state-branch_name = 'refs/heads/main'.
        ls_state-remote_url  = 'https://test-cache-admin.example.com/repo.git'.
        ls_state-curr_commit = 'aaaa000000000000000000000000000000000001'.
        ls_state-is_shallow  = abap_false.
        MODIFY zaog_repo_state FROM ls_state.

        lv_data = '48656C6C6F'.
        zcl_abapgit_ortec_obj_store=>store_object(
          iv_repo_key = mc_repo iv_sha1 = 'bbbb000000000000000000000000000000000001'
          iv_type = zif_abapgit_git_definitions=>c_type-blob iv_data = lv_data ).
        zcl_abapgit_ortec_obj_store=>store_object(
          iv_repo_key = mc_repo iv_sha1 = 'bbbb000000000000000000000000000000000002'
          iv_type = zif_abapgit_git_definitions=>c_type-blob iv_data = lv_data ).

        ls_idx-repo_key    = mc_repo.
        ls_idx-commit_sha1 = 'aaaa000000000000000000000000000000000001'.
        ls_idx-obj_type    = 'PROG'.
        ls_idx-obj_name    = 'ZTEST'.
        ls_idx-path_hash   = zcl_abapgit_hash=>sha1_string( '/' ).
        ls_idx-file_path   = '/'.
        MODIFY zaog_obj_index FROM ls_idx.

        ls_pack-repo_key   = mc_repo.
        ls_pack-pack_id    = 'TESTPACK00000000000000000000CADM'.
        ls_pack-total_size = 2097152. " exactly 2 MB, so pack_mb_disk asserts cleanly
        ls_pack-status     = 'C'.
        ls_pack-raw_stored = abap_true.
        ls_pack-received_at = lv_ts.
        MODIFY zaog_pack_meta FROM ls_pack.

        ls_commit-repo_key    = mc_repo.
        ls_commit-commit_sha1 = 'aaaa000000000000000000000000000000000001'.
        ls_commit-branch_name = 'refs/heads/main'.
        ls_commit-fetched_at  = lv_ts.
        MODIFY zaog_commit_hist FROM ls_commit.

        ls_sess-session_id = 'CADMINTEST000000000000000000001A'.
        ls_sess-repo_key   = mc_repo.
        ls_sess-branch_name = 'refs/heads/main'.
        ls_sess-phase      = 'D'.
        ls_sess-status     = 'A'.
        ls_sess-created_at = lv_ts.
        ls_sess-updated_at = lv_ts.
        MODIFY zaog_fetch_sess FROM ls_sess.

        COMMIT WORK AND WAIT.

      CATCH cx_root INTO DATA(lx_diag_seed).
        cl_abap_unit_assert=>fail( |DIAG SEED { cl_abap_classdescr=>get_class_name( lx_diag_seed ) }: { lx_diag_seed->get_text( ) }| ).
    ENDTRY.

    TRY.
        DATA(lt_overview) = zcl_abapgit_ortec_cache_admin=>get_overview( ).
      CATCH cx_root INTO DATA(lx_diag_sql).
        cl_abap_unit_assert=>fail( |DIAG { cl_abap_classdescr=>get_class_name( lx_diag_sql ) }: { lx_diag_sql->get_text( ) }| ).
    ENDTRY.
    READ TABLE lt_overview INTO DATA(ls_overview) WITH KEY repo_key = mc_repo.
    cl_abap_unit_assert=>assert_subrc( msg = 'Overview must contain the seeded test repo' ).

    cl_abap_unit_assert=>assert_equals( act = ls_overview-remote_url
      exp = 'https://test-cache-admin.example.com/repo.git' msg = 'remote_url must match' ).
    cl_abap_unit_assert=>assert_equals( act = ls_overview-obj_count exp = 2
      msg = 'obj_count must count both stored blobs' ).
    cl_abap_unit_assert=>assert_equals( act = ls_overview-idx_entries exp = 1
      msg = 'idx_entries must count the seeded index row' ).
    cl_abap_unit_assert=>assert_equals( act = ls_overview-pack_count exp = 1
      msg = 'pack_count must count the seeded pack' ).
    cl_abap_unit_assert=>assert_equals( act = ls_overview-pack_mb_disk exp = '2.00'
      msg = 'pack_mb_disk must reflect the 2 MB raw_stored pack' ).
    cl_abap_unit_assert=>assert_equals( act = ls_overview-commit_count exp = 1
      msg = 'commit_count must count the seeded commit' ).
    cl_abap_unit_assert=>assert_equals( act = ls_overview-open_sessions exp = 1
      msg = 'open_sessions must count the seeded active session' ).
  ENDMETHOD.
ENDCLASS.
