CLASS ltcl_cache_admin DEFINITION
  FOR TESTING
  RISK LEVEL HARMLESS
  DURATION SHORT
  FINAL.

  PRIVATE SECTION.

    CONSTANTS c_repo TYPE
      zcl_abapgit_ortec_repo_state=>ty_repo_key
      VALUE 'ZAOGT_CADMIN'.

    CONSTANTS c_other_repo TYPE
      zcl_abapgit_ortec_repo_state=>ty_repo_key
      VALUE 'ZAOGT_CADOTH'.

    CONSTANTS c_branch TYPE string
      VALUE 'refs/heads/main'.

    CONSTANTS c_url TYPE string
      VALUE 'https://test-cache-admin.example.com/repo.git'.

    CONSTANTS c_other_url TYPE string
      VALUE 'https://test-cache-admin.example.com/other.git'.

    CONSTANTS c_context_hash TYPE zif_abapgit_git_definitions=>ty_sha1
      VALUE '1111111111111111111111111111111111111111'.

    METHODS setup.
    METHODS teardown.
    METHODS cleanup.

    METHODS seed_repo
      IMPORTING
        iv_repo_key   TYPE zcl_abapgit_ortec_repo_state=>ty_repo_key
        iv_with_state TYPE abap_bool DEFAULT abap_true
        iv_with_url   TYPE abap_bool DEFAULT abap_true
      RAISING
        cx_static_check.

    METHODS overview_aggregates_counts
      FOR TESTING
      RAISING cx_static_check.

    METHODS clear_deletes_all
      FOR TESTING
      RAISING cx_static_check.

    METHODS clear_without_url
      FOR TESTING
      RAISING cx_static_check.

    METHODS clear_orphaned_data
      FOR TESTING
      RAISING cx_static_check.

    METHODS clear_keeps_other_repo
      FOR TESTING
      RAISING cx_static_check.

    METHODS clear_unknown_raises
      FOR TESTING
      RAISING cx_static_check.

    METHODS clear_initial_raises
      FOR TESTING
      RAISING cx_static_check.

    METHODS format_includes_hist
        FOR TESTING.

    METHODS f4_includes_orphan
      FOR TESTING
      RAISING cx_static_check.

    METHODS overview_large_sizes
      FOR TESTING
      RAISING cx_static_check.

    " Package E E3-TEST (design doc §5, INV-E3-T-1/2/3): CONFIRMED_CURRENT
    " regression coverage - no defect found. E3-T-02 (obj_store tier
    " precedence) is already adequately covered by f4_includes_orphan
    " above; these three add isolated single-tier fixtures not covered by
    " the existing seed_repo-based tests.
    METHODS f4_repo_state_only
      FOR TESTING
      RAISING cx_static_check.

    METHODS f4_commit_hist_only
      FOR TESTING
      RAISING cx_static_check.

    METHODS f4_dedup_prefers_state
      FOR TESTING
      RAISING cx_static_check.

    " Slice 1c (IMPL-A3): AR-1-05/AR-2-03 cache-admin lock unification -
    " clear_repo now also deletes ZAOG_OBJ_COVER/ZAOG_OBJ_PIDX under the
    " canonical zcl_abapgit_ortec_pack_raw repo-scoped mutex.
    METHODS seed_filter_rows
      IMPORTING
        iv_repo_key TYPE zcl_abapgit_ortec_repo_state=>ty_repo_key
      RAISING
        cx_static_check.

    METHODS clear_repo_deletes_derived
      FOR TESTING
      RAISING cx_static_check.

    METHODS clear_repo_forces_rewalk
      FOR TESTING
      RAISING cx_static_check.

    METHODS clear_repo_blocks_on_pack_lock
      FOR TESTING
      RAISING cx_static_check.

ENDCLASS.


CLASS ltcl_cache_admin IMPLEMENTATION.

  METHOD setup.
    cleanup( ).
  ENDMETHOD.


  METHOD teardown.
    cleanup( ).
  ENDMETHOD.


  METHOD cleanup.

    ROLLBACK WORK.

    DELETE FROM zaog_obj_index
      WHERE repo_key = c_repo
         OR repo_key = c_other_repo.

    DELETE FROM zaog_obj_cover
      WHERE repo_key = c_repo
         OR repo_key = c_other_repo.

    DELETE FROM zaog_obj_pidx
      WHERE repo_key = c_repo
         OR repo_key = c_other_repo.

    DELETE FROM zaog_pack_idx
      WHERE repo_key = c_repo
         OR repo_key = c_other_repo.

    DELETE FROM zaog_raw_pack
      WHERE repo_key = c_repo
         OR repo_key = c_other_repo.

    DELETE FROM zaog_pack_meta
      WHERE repo_key = c_repo
         OR repo_key = c_other_repo.

    DELETE FROM zaog_fetch_sess
      WHERE repo_key = c_repo
         OR repo_key = c_other_repo.

    DELETE FROM zaog_commit_hist
      WHERE repo_key = c_repo
         OR repo_key = c_other_repo.

    DELETE FROM zaog_obj_store
      WHERE repo_key = c_repo
         OR repo_key = c_other_repo.

    DELETE FROM zaog_repo_state
      WHERE repo_key = c_repo
         OR repo_key = c_other_repo.

    COMMIT WORK AND WAIT.

    zcl_abapgit_ortec_obj_store=>invalidate_cache( ).

  ENDMETHOD.


  METHOD seed_repo.

    DATA ls_state   TYPE zaog_repo_state.
    DATA ls_index   TYPE zaog_obj_index.
    DATA ls_pack    TYPE zaog_pack_meta.
    DATA ls_commit  TYPE zaog_commit_hist.
    DATA ls_session TYPE zaog_fetch_sess.
    DATA lv_ts      TYPE timestampl.
    DATA lv_data    TYPE xstring.
    DATA lv_sha1    TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_url     TYPE string.

    GET TIME STAMP FIELD lv_ts.

    IF iv_repo_key = c_repo.
      lv_url = c_url.
    ELSE.
      lv_url = c_other_url.
    ENDIF.

    IF iv_with_state = abap_true.

      ls_state-repo_key    = iv_repo_key.
      ls_state-branch_name = c_branch.

      IF iv_with_url = abap_true.
        ls_state-remote_url = lv_url.
        ls_state-url_hash =
          zcl_abapgit_hash=>sha1_string( lv_url ).
      ENDIF.

      ls_state-curr_commit =
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'.
      ls_state-fetch_commit =
        'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'.
      ls_state-fetch_ts   = lv_ts.
      ls_state-is_shallow = abap_false.
      ls_state-deepen_lvl = 0.
      ls_state-changed_by = sy-uname.
      ls_state-changed_at = lv_ts.
      ls_state-snap_state =
        zcl_abapgit_ortec_mat_state=>cs_snap_state-complete.

      MODIFY zaog_repo_state FROM ls_state.

      cl_abap_unit_assert=>assert_subrc(
        exp = 0
        msg = 'Failed to seed ZAOG_REPO_STATE' ).

    ENDIF.

    lv_data =
      zcl_abapgit_convert=>string_to_xstring_utf8(
        |cache-admin-{ iv_repo_key }| ).

    lv_sha1 = zcl_abapgit_hash=>sha1_blob( lv_data ).

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = iv_repo_key
      iv_sha1     = lv_sha1
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_data ).

    CLEAR ls_index.
    ls_index-repo_key    = iv_repo_key.
    ls_index-commit_sha1 =
      'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'.
    ls_index-obj_type  = 'PROG'.
    ls_index-obj_name  = 'ZCACHE_ADMIN'.
    ls_index-path_hash = zcl_abapgit_hash=>sha1_string( '/' ).
    ls_index-file_path = '/'.

    MODIFY zaog_obj_index FROM ls_index.

    cl_abap_unit_assert=>assert_subrc(
      exp = 0
      msg = 'Failed to seed ZAOG_OBJ_INDEX' ).

    CLEAR ls_pack.
    ls_pack-repo_key = iv_repo_key.

    IF iv_repo_key = c_repo.
      ls_pack-pack_id = 'CADMINTEST000000000000000000001A'.
    ELSE.
      ls_pack-pack_id = 'CADMINTEST000000000000000000001B'.
    ENDIF.

    ls_pack-total_size  = 2097152.
    ls_pack-status      = 'C'.
    ls_pack-raw_stored  = abap_true.
    ls_pack-received_at = lv_ts.

    MODIFY zaog_pack_meta FROM ls_pack.

    cl_abap_unit_assert=>assert_subrc(
      exp = 0
      msg = 'Failed to seed ZAOG_PACK_META' ).

    CLEAR ls_commit.
    ls_commit-repo_key    = iv_repo_key.
    ls_commit-commit_sha1 =
      'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'.
    ls_commit-branch_name = c_branch.
    ls_commit-fetched_at  = lv_ts.
    ls_commit-hist_level  =
      zcl_abapgit_ortec_mat_state=>cs_hist_level-full_complete.
    ls_commit-snap_state  =
      zcl_abapgit_ortec_mat_state=>cs_snap_state-complete.

    MODIFY zaog_commit_hist FROM ls_commit.

    cl_abap_unit_assert=>assert_subrc(
      exp = 0
      msg = 'Failed to seed ZAOG_COMMIT_HIST' ).

    CLEAR ls_session.

    IF iv_repo_key = c_repo.
      ls_session-session_id =
        'CADMINTEST000000000000000000001A'.
    ELSE.
      ls_session-session_id =
        'CADMINTEST000000000000000000001B'.
    ENDIF.

    ls_session-repo_key    = iv_repo_key.
    ls_session-branch_name = c_branch.
    ls_session-phase       = 'D'.
    ls_session-status      = 'A'.
    ls_session-created_at  = lv_ts.
    ls_session-updated_at  = lv_ts.

    MODIFY zaog_fetch_sess FROM ls_session.

    cl_abap_unit_assert=>assert_subrc(
      exp = 0
      msg = 'Failed to seed ZAOG_FETCH_SESS' ).

    COMMIT WORK AND WAIT.

    zcl_abapgit_ortec_obj_store=>invalidate_cache( ).

  ENDMETHOD.


  METHOD overview_aggregates_counts.

    seed_repo(
      iv_repo_key = c_repo ).

    DATA(lv_data) =
      zcl_abapgit_convert=>string_to_xstring_utf8(
        'cache-admin-second-object' ).

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = c_repo
      iv_sha1     = zcl_abapgit_hash=>sha1_blob( lv_data )
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_data ).

    DATA(lt_overview) =
      zcl_abapgit_ortec_cache_admin=>get_overview( ).

    READ TABLE lt_overview
      INTO DATA(ls_overview)
      WITH KEY repo_key = c_repo.

    cl_abap_unit_assert=>assert_subrc(
      exp = 0
      msg = 'Overview must contain the seeded test repo' ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_overview-remote_url
      exp = c_url
      msg = 'REMOTE_URL must match' ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_overview-obj_count
      exp = 2
      msg = 'OBJ_COUNT must include both objects' ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_overview-idx_entries
      exp = 1
      msg = 'IDX_ENTRIES must count the index row' ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_overview-pack_count
      exp = 1
      msg = 'PACK_COUNT must count the seeded pack' ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_overview-pack_mb_disk
      exp = '2.00'
      msg = 'PACK_MB_DISK must reflect two MB' ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_overview-commit_count
      exp = 1
      msg = 'COMMIT_COUNT must count the certificate' ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_overview-open_sessions
      exp = 1
      msg = 'OPEN_SESSIONS must count the active session' ).

  ENDMETHOD.


  METHOD clear_deletes_all.

    seed_repo(
      iv_repo_key = c_repo ).

    DATA(ls_result) =
      zcl_abapgit_ortec_cache_admin=>clear_repo(
        iv_repo_key = c_repo ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_result-repo_key
      exp = c_repo ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_result-obj_store
      exp = 1 ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_result-obj_index
      exp = 1 ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_result-pack_meta
      exp = 1 ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_result-fetch_sess
      exp = 1 ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_result-commit_hist
      exp = 1 ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_result-repo_state
      exp = 1 ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_result-pack_idx
      exp = 0 ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_result-raw_pack
      exp = 0 ).

    SELECT COUNT(*)
      FROM zaog_obj_store
      WHERE repo_key = @c_repo
      INTO @DATA(lv_obj_count).

    SELECT COUNT(*)
      FROM zaog_obj_index
      WHERE repo_key = @c_repo
      INTO @DATA(lv_idx_count).

    SELECT COUNT(*)
      FROM zaog_pack_idx
      WHERE repo_key = @c_repo
      INTO @DATA(lv_pack_idx_count).

    SELECT COUNT(*)
      FROM zaog_pack_meta
      WHERE repo_key = @c_repo
      INTO @DATA(lv_pack_count).

    SELECT COUNT(*)
      FROM zaog_raw_pack
      WHERE repo_key = @c_repo
      INTO @DATA(lv_raw_count).

    SELECT COUNT(*)
      FROM zaog_fetch_sess
      WHERE repo_key = @c_repo
      INTO @DATA(lv_sess_count).

    SELECT COUNT(*)
      FROM zaog_commit_hist
      WHERE repo_key = @c_repo
      INTO @DATA(lv_hist_count).

    SELECT COUNT(*)
      FROM zaog_repo_state
      WHERE repo_key = @c_repo
      INTO @DATA(lv_state_count).

    cl_abap_unit_assert=>assert_equals(
      act = lv_obj_count
      exp = 0 ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_idx_count
      exp = 0 ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_pack_idx_count
      exp = 0 ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_pack_count
      exp = 0 ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_raw_count
      exp = 0 ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_sess_count
      exp = 0 ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_hist_count
      exp = 0 ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_state_count
      exp = 0 ).

  ENDMETHOD.


  METHOD clear_without_url.

    seed_repo(
      iv_repo_key  = c_repo
      iv_with_state = abap_true
      iv_with_url   = abap_false ).

    SELECT SINGLE remote_url
      FROM zaog_repo_state
      WHERE repo_key    = @c_repo
        AND branch_name = @c_branch
      INTO @DATA(lv_url).

    cl_abap_unit_assert=>assert_initial(
      act = lv_url
      msg = 'Fixture must have no REMOTE_URL' ).

    DATA(ls_result) =
      zcl_abapgit_ortec_cache_admin=>clear_repo(
        iv_repo_key = c_repo ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_result-obj_store
      exp = 1 ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_result-commit_hist
      exp = 1 ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_result-repo_state
      exp = 1 ).

    SELECT COUNT(*)
      FROM zaog_obj_store
      WHERE repo_key = @c_repo
      INTO @DATA(lv_objects).

    SELECT COUNT(*)
      FROM zaog_commit_hist
      WHERE repo_key = @c_repo
      INTO @DATA(lv_history).

    SELECT COUNT(*)
      FROM zaog_repo_state
      WHERE repo_key = @c_repo
      INTO @DATA(lv_state).

    cl_abap_unit_assert=>assert_equals(
      act = lv_objects
      exp = 0 ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_history
      exp = 0 ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_state
      exp = 0 ).

  ENDMETHOD.


  METHOD clear_orphaned_data.

    seed_repo(
      iv_repo_key   = c_repo
      iv_with_state = abap_false ).

    SELECT COUNT(*)
      FROM zaog_repo_state
      WHERE repo_key = @c_repo
      INTO @DATA(lv_before_state).

    cl_abap_unit_assert=>assert_equals(
      act = lv_before_state
      exp = 0
      msg = 'Fixture must not contain repository state' ).

    DATA(ls_result) =
      zcl_abapgit_ortec_cache_admin=>clear_repo(
        iv_repo_key = c_repo ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_result-obj_store
      exp = 1 ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_result-commit_hist
      exp = 1 ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_result-repo_state
      exp = 0 ).

    SELECT COUNT(*)
      FROM zaog_obj_store
      WHERE repo_key = @c_repo
      INTO @DATA(lv_objects).

    SELECT COUNT(*)
      FROM zaog_commit_hist
      WHERE repo_key = @c_repo
      INTO @DATA(lv_history).

    cl_abap_unit_assert=>assert_equals(
      act = lv_objects
      exp = 0 ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_history
      exp = 0 ).

  ENDMETHOD.


  METHOD clear_keeps_other_repo.

    seed_repo(
      iv_repo_key = c_repo ).

    seed_repo(
      iv_repo_key = c_other_repo ).

    zcl_abapgit_ortec_cache_admin=>clear_repo(
      iv_repo_key = c_repo ).

    SELECT COUNT(*)
      FROM zaog_obj_store
      WHERE repo_key = @c_other_repo
      INTO @DATA(lv_other_objects).

    SELECT COUNT(*)
      FROM zaog_commit_hist
      WHERE repo_key = @c_other_repo
      INTO @DATA(lv_other_history).

    SELECT COUNT(*)
      FROM zaog_repo_state
      WHERE repo_key = @c_other_repo
      INTO @DATA(lv_other_state).

    cl_abap_unit_assert=>assert_equals(
      act = lv_other_objects
      exp = 1 ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_other_history
      exp = 1 ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_other_state
      exp = 1 ).

  ENDMETHOD.


  METHOD clear_unknown_raises.

    DATA lv_raised TYPE abap_bool.

    TRY.
        zcl_abapgit_ortec_cache_admin=>clear_repo(
          iv_repo_key = 'ZAOGT_UNKNWN' ).

      CATCH zcx_abapgit_ortec_git INTO DATA(lx_error).
        lv_raised = abap_true.

        cl_abap_unit_assert=>assert_true(
          act = xsdbool(
            lx_error->get_text( ) CS 'no cached data' ) ).
    ENDTRY.

    cl_abap_unit_assert=>assert_equals(
      act = lv_raised
      exp = abap_true ).

  ENDMETHOD.


  METHOD clear_initial_raises.

    DATA lv_raised TYPE abap_bool.

    TRY.
        zcl_abapgit_ortec_cache_admin=>clear_repo(
          iv_repo_key = space ).

      CATCH zcx_abapgit_ortec_git INTO DATA(lx_error).
        lv_raised = abap_true.

        cl_abap_unit_assert=>assert_true(
          act = xsdbool(
            lx_error->get_text( ) CS 'repository key required' ) ).
    ENDTRY.

    cl_abap_unit_assert=>assert_equals(
      act = lv_raised
      exp = abap_true ).

  ENDMETHOD.


  METHOD format_includes_hist.

    DATA ls_result TYPE
      zcl_abapgit_ortec_cache_admin=>ty_clear_result.

    ls_result-repo_key    = c_repo.
    ls_result-obj_store   = 2.
    ls_result-obj_index   = 3.
    ls_result-pack_idx    = 4.
    ls_result-pack_meta   = 5.
    ls_result-raw_pack    = 6.
    ls_result-fetch_sess  = 7.
    ls_result-commit_hist = 8.
    ls_result-repo_state  = 9.

    DATA(lv_message) =
      zcl_abapgit_ortec_cache_admin=>format_clear_result(
        is_result = ls_result ).

    cl_abap_unit_assert=>assert_true(
      act = xsdbool(
        lv_message CS 'COMMIT_HIST=8' ) ).

    cl_abap_unit_assert=>assert_true(
      act = xsdbool(
        lv_message CS '44 row(s) removed' ) ).

  ENDMETHOD.


  METHOD f4_includes_orphan.

    seed_repo(
      iv_repo_key   = c_repo
      iv_with_state = abap_false ).

    DATA(lt_values) =
      zcl_abapgit_ortec_cache_admin=>get_repo_f4_values( ).

    READ TABLE lt_values
      INTO DATA(ls_value)
      WITH KEY repo_key = c_repo.

    cl_abap_unit_assert=>assert_subrc(
      exp = 0
      msg = 'F4 must include an orphaned cache repository' ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_value-branch_name
      exp = '<orphaned cache>' ).

  ENDMETHOD.


  METHOD overview_large_sizes.

    DATA ls_state TYPE zaog_repo_state.
    DATA ls_object TYPE zaog_obj_store.
    DATA ls_pack TYPE zaog_pack_meta.
    DATA lv_ts TYPE timestampl.
    DATA lv_expected_bytes TYPE p LENGTH 15 DECIMALS 0.

    GET TIME STAMP FIELD lv_ts.

    ls_state-repo_key    = c_repo.
    ls_state-branch_name = c_branch.
    ls_state-remote_url  = c_url.
    ls_state-url_hash =
      zcl_abapgit_hash=>sha1_string( c_url ).
    ls_state-changed_by = sy-uname.
    ls_state-changed_at = lv_ts.

    MODIFY zaog_repo_state FROM ls_state.

    cl_abap_unit_assert=>assert_subrc(
      exp = 0
      msg = 'Failed*to seed repository state' ).

    " Three rows of one billion bytes each exceed the signed INT4 sum limit.
    CLEAR ls_object.
    ls_object-repo_key  = c_repo.
    ls_object-obj_sha1  =
      '1111111111111111111111111111111111111111'.
    ls_object-obj_type  =
      zif_abapgit_git_definitions=>c_type-blob.
    ls_object-obj_size  = 1000000000.
    ls_object-status    = 'R'.
    ls_object-created_at = lv_ts.

    MODIFY zaog_obj_store FROM ls_object.

    ls_object-obj_sha1 =
     '2222222222222222222222222222222222222222'.

    MODIFY zaog_obj_store FROM ls_object.

    ls_object-obj_sha1 =
      '3333333333333333333333333333333333333333'.

    MODIFY zaog_obj_store FROM ls_object.

    " Also cover PACK_META aggregation beyond the INT4 limit.
    CLEAR ls_pack.
    ls_pack-repo_key    = c_repo.
    ls_pack-pack_id     =
      'CADMINLARGE000000000000000000001'.
    ls_pack-total_size  = 1500000000.
    ls_pack-status      = '*C'.
    ls_pack-raw_stored  = abap_true.
    ls_pack-received_at = lv_ts.

    MODIFY zaog_pack_meta FROM ls_pack.

    ls_pack-pack_id =
      'CADMINLARGE000000000000000000002'.

    MODIFY zaog_pack_meta FROM ls_pack.

    COMMIT WORK AND WAIT.

    DATA(lt_overview) =
      zcl_abapgit_ortec_cache_admin=>get_overview( ).

    READ TABLE lt_overview
      INTO DATA(ls_overview)
      WITH KEY repo_key = c_repo.

    cl_abap_unit_assert=>assert_subrc(
      exp = 0
      msg = 'Overview must contain large-size fixture' ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_overview-obj_count
      exp = 3 ).

    " 3,000,000,000 bytes must have been summed without INT4 overflow.
    lv_expected_bytes = 3000000000.

    cl_abap_unit_assert=>assert_true(
      act = xsdbool(
        ls_overview-obj_size_mb > 2861
        AND ls_overview-obj_size_mb < 2862 )
      msg = 'Object byte sum must exceed INT4 without overflow' ).

    " Two packs with 1.5 billion bytes each give the same total.
    cl_abap_unit_assert=>assert_true(
      act = xsdbool(
        ls_overview-pack_mb_disk > 2861
        AND ls_overview-pack_mb_disk < 2862 )
      msg = 'Pack byte sum must exceed INT4 without overflow' ).

  ENDMETHOD.

  METHOD f4_repo_state_only.
    " E3-T-01 (design §5): isolated fixture - ONLY ZAOG_REPO_STATE seeded
    " for c_repo (no obj_store/obj_index/commit_hist rows), unlike
    " seed_repo which always creates all of them. Proves tier 1
    " (repo_state) alone produces a correct, non-orphan F4 row with the
    " refs/heads/ prefix stripped.
    DATA ls_state TYPE zaog_repo_state.
    DATA lv_ts    TYPE timestampl.

    GET TIME STAMP FIELD lv_ts.

    ls_state-repo_key     = c_repo.
    ls_state-branch_name  = c_branch.
    ls_state-remote_url   = c_url.
    ls_state-url_hash     = zcl_abapgit_hash=>sha1_string( c_url ).
    ls_state-curr_commit  = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'.
    ls_state-fetch_commit = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'.
    ls_state-fetch_ts     = lv_ts.
    ls_state-is_shallow   = abap_false.
    ls_state-deepen_lvl   = 0.
    ls_state-changed_by   = sy-uname.
    ls_state-changed_at   = lv_ts.
    ls_state-snap_state   = zcl_abapgit_ortec_mat_state=>cs_snap_state-complete.

    MODIFY zaog_repo_state FROM ls_state.
    cl_abap_unit_assert=>assert_subrc(
      exp = 0
      msg = 'Failed to seed ZAOG_REPO_STATE' ).

    DATA(lt_values) = zcl_abapgit_ortec_cache_admin=>get_repo_f4_values( ).

    READ TABLE lt_values INTO DATA(ls_value) WITH KEY repo_key = c_repo.
    cl_abap_unit_assert=>assert_subrc(
      exp = 0
      msg = 'F4 must include the repo_state-only repository' ).
    cl_abap_unit_assert=>assert_equals(
      act = ls_value-branch_name
      exp = 'main'
      msg = 'refs/heads/ prefix must be stripped for a real repo_state tier row' ).
    cl_abap_unit_assert=>assert_equals(
      act = ls_value-remote_url
      exp = c_url ).
  ENDMETHOD.

  METHOD f4_commit_hist_only.
    " E3-T-03 (design §5): isolated fixture - ONLY ZAOG_COMMIT_HIST seeded
    " (no repo_state, no obj_store) - proves tier 3's distinct
    " '<orphaned certificate>' label (which differs from tier 2's
    " '<orphaned cache>' label).
    DATA ls_commit TYPE zaog_commit_hist.
    DATA lv_ts     TYPE timestampl.

    GET TIME STAMP FIELD lv_ts.

    ls_commit-repo_key    = c_repo.
    ls_commit-commit_sha1 = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'.
    ls_commit-branch_name = c_branch.
    ls_commit-fetched_at  = lv_ts.
    ls_commit-hist_level  = zcl_abapgit_ortec_mat_state=>cs_hist_level-full_complete.
    ls_commit-snap_state  = zcl_abapgit_ortec_mat_state=>cs_snap_state-complete.

    MODIFY zaog_commit_hist FROM ls_commit.
    cl_abap_unit_assert=>assert_subrc(
      exp = 0
      msg = 'Failed to seed ZAOG_COMMIT_HIST' ).

    DATA(lt_values) = zcl_abapgit_ortec_cache_admin=>get_repo_f4_values( ).

    READ TABLE lt_values INTO DATA(ls_value) WITH KEY repo_key = c_repo.
    cl_abap_unit_assert=>assert_subrc(
      exp = 0
      msg = 'F4 must include a commit-hist-only orphaned certificate' ).
    cl_abap_unit_assert=>assert_equals(
      act = ls_value-branch_name
      exp = '<orphaned certificate>'
      msg = 'Tier 3 label must be distinct from tier 2''s <orphaned cache>' ).
  ENDMETHOD.

  METHOD f4_dedup_prefers_state.
    " E3-T-04 (design §5): seed_repo populates ALL three tiers for the SAME
    " repo_key - dedup logic must produce exactly ONE row (the real
    " repo_state tier wins, not an orphan label).
    seed_repo( iv_repo_key = c_repo iv_with_state = abap_true ).

    DATA(lt_values) = zcl_abapgit_ortec_cache_admin=>get_repo_f4_values( ).

    " ty_repo_f4_tt is a STANDARD table WITH DEFAULT KEY (production type,
    " out of scope to change here) - FILTER requires an explicit SORTED/
    " HASHED key and is a real syntax error against this type (IT8 SLIN
    " message GDY, confirmed 2026-07-29). The fixture is provably tiny
    " (test-only rows for at most 2 repo keys), so a plain LOOP AT ... WHERE
    " + READ TABLE ... WITH KEY (both valid on any standard table without a
    " secondary key) is the narrowest correct fix.
    DATA lv_match_count TYPE i.
    LOOP AT lt_values TRANSPORTING NO FIELDS WHERE repo_key = c_repo.
      lv_match_count = lv_match_count + 1.
    ENDLOOP.

    cl_abap_unit_assert=>assert_equals(
      act = lv_match_count
      exp = 1
      msg = 'A repo present in all three tiers must be deduplicated to exactly one F4 row' ).

    READ TABLE lt_values INTO DATA(ls_match) WITH KEY repo_key = c_repo.
    cl_abap_unit_assert=>assert_subrc(
      exp = 0
      msg = 'F4 row for the deduplicated repo must exist' ).
    cl_abap_unit_assert=>assert_equals(
      act = ls_match-branch_name
      exp = 'main'
      msg = 'The real repo_state tier must win the dedup, not an orphan label' ).
  ENDMETHOD.


  METHOD seed_filter_rows.

    DATA ls_marker TYPE zaog_obj_index.
    DATA ls_cover  TYPE zaog_obj_cover.
    DATA ls_pidx   TYPE zaog_obj_pidx.
    DATA lv_ts     TYPE timestampl.

    GET TIME STAMP FIELD lv_ts.

    " $IDX/__READY__ completion marker (STRICT is_index_ready mode).
    ls_marker-repo_key     = iv_repo_key.
    ls_marker-commit_sha1  = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'.
    ls_marker-obj_type     = '$IDX'.
    ls_marker-obj_name     = '__READY__'.
    ls_marker-path_hash    = '0000000000000000000000000000000000000000'.
    ls_marker-idx_status   = 'R'.
    ls_marker-context_hash = c_context_hash.

    MODIFY zaog_obj_index FROM ls_marker.

    cl_abap_unit_assert=>assert_subrc(
      exp = 0
      msg = 'Failed to seed ZAOG_OBJ_INDEX marker row' ).

    ls_cover-repo_key          = iv_repo_key.
    ls_cover-commit_sha1       = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'.
    ls_cover-obj_type          = 'PROG'.
    ls_cover-obj_name          = 'ZCACHE_ADMIN'.
    ls_cover-context_hash      = c_context_hash.
    ls_cover-resolution_status = zcl_abapgit_ortec_obj_cover=>cs_resolution-found.
    ls_cover-file_count        = 1.
    ls_cover-walk_hist_level   = zcl_abapgit_ortec_mat_state=>cs_hist_level-full_complete.
    ls_cover-resolved_at       = lv_ts.

    MODIFY zaog_obj_cover FROM ls_cover.

    cl_abap_unit_assert=>assert_subrc(
      exp = 0
      msg = 'Failed to seed ZAOG_OBJ_COVER row' ).

    ls_pidx-repo_key    = iv_repo_key.
    ls_pidx-commit_sha1 = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'.
    ls_pidx-obj_type    = 'PROG'.
    ls_pidx-obj_name    = 'ZCACHE_ADMIN'.
    ls_pidx-context_hash = c_context_hash.
    ls_pidx-path_hash   = zcl_abapgit_hash=>sha1_string( '/' ).
    ls_pidx-file_path   = '/'.
    ls_pidx-idx_status  = 'R'.

    MODIFY zaog_obj_pidx FROM ls_pidx.

    cl_abap_unit_assert=>assert_subrc(
      exp = 0
      msg = 'Failed to seed ZAOG_OBJ_PIDX row' ).

    COMMIT WORK AND WAIT.

  ENDMETHOD.


  METHOD clear_repo_deletes_derived.

    seed_repo( iv_repo_key = c_repo ).
    seed_filter_rows( c_repo ).

    DATA(ls_result) =
      zcl_abapgit_ortec_cache_admin=>clear_repo(
        iv_repo_key = c_repo ).

    " seed_repo already inserts one ZAOG_OBJ_INDEX row of its own, plus the
    " $IDX/__READY__ marker from seed_filter_rows.
    cl_abap_unit_assert=>assert_equals(
      act = ls_result-obj_index
      exp = 2
      msg = 'OBJ_INDEX counter must include the seeded row and the marker' ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_result-obj_cover
      exp = 1
      msg = 'OBJ_COVER counter must reflect the deleted coverage row' ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_result-obj_pidx
      exp = 1
      msg = 'OBJ_PIDX counter must reflect the deleted partial-index row' ).

    SELECT COUNT(*)
      FROM zaog_obj_index
      WHERE repo_key = @c_repo
      INTO @DATA(lv_idx_count).

    SELECT COUNT(*)
      FROM zaog_obj_cover
      WHERE repo_key = @c_repo
      INTO @DATA(lv_cover_count).

    SELECT COUNT(*)
      FROM zaog_obj_pidx
      WHERE repo_key = @c_repo
      INTO @DATA(lv_pidx_count).

    cl_abap_unit_assert=>assert_equals(
      act = lv_idx_count
      exp = 0
      msg = 'ZAOG_OBJ_INDEX must be empty after clear_repo' ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_cover_count
      exp = 0
      msg = 'ZAOG_OBJ_COVER must be empty after clear_repo' ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_pidx_count
      exp = 0
      msg = 'ZAOG_OBJ_PIDX must be empty after clear_repo' ).

  ENDMETHOD.


  METHOD clear_repo_forces_rewalk.

    seed_repo( iv_repo_key = c_repo ).
    seed_filter_rows( c_repo ).

    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_obj_index=>is_index_ready(
              iv_repo_key     = c_repo
              iv_commit       = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
              iv_context_hash = c_context_hash )
      exp = abap_true
      msg = 'Fixture must start with a ready index under this context' ).

    zcl_abapgit_ortec_cache_admin=>clear_repo(
      iv_repo_key = c_repo ).

    " A subsequent filtered read must never trust orphaned coverage: the
    " completion marker is gone, so is_index_ready reports NOT ready and
    " the caller re-walks instead of returning a false empty result.
    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_obj_index=>is_index_ready(
              iv_repo_key     = c_repo
              iv_commit       = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
              iv_context_hash = c_context_hash )
      exp = abap_false
      msg = 'Index must not be reported ready after clear_repo' ).

    SELECT COUNT(*)
      FROM zaog_obj_cover
      WHERE repo_key = @c_repo
      INTO @DATA(lv_cover_count).

    SELECT COUNT(*)
      FROM zaog_obj_pidx
      WHERE repo_key = @c_repo
      INTO @DATA(lv_pidx_count).

    cl_abap_unit_assert=>assert_equals(
      act = lv_cover_count
      exp = 0
      msg = 'No coverage row may survive clear_repo to be (wrongly) trusted' ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_pidx_count
      exp = 0
      msg = 'No partial-index row may survive clear_repo to be (wrongly) trusted' ).

  ENDMETHOD.


  METHOD clear_repo_blocks_on_pack_lock.

    seed_repo( iv_repo_key = c_repo ).
    seed_filter_rows( c_repo ).

    DATA(lv_lock_id) =
      zcl_abapgit_ortec_pack_raw=>acquire_repo_lock(
        iv_repo_key = c_repo ).

    DATA lx_caught TYPE REF TO zcx_abapgit_ortec_git.

    TRY.
        zcl_abapgit_ortec_cache_admin=>clear_repo(
          iv_repo_key = c_repo ).

        cl_abap_unit_assert=>fail(
          'clear_repo must not proceed while the pack-raw repo lock is held' ).

      CATCH zcx_abapgit_ortec_git INTO lx_caught.
        " Expected: acquire_repo_lock inside clear_repo times out while the
        " mutex is held by this test, and clear_repo fails cleanly instead
        " of clearing under a concurrent writer.
    ENDTRY.

    zcl_abapgit_ortec_pack_raw=>release_repo_lock( lv_lock_id ).

    cl_abap_unit_assert=>assert_bound(
      act = lx_caught
      msg = 'clear_repo must raise when it cannot acquire the pack-raw lock' ).

    SELECT COUNT(*)
      FROM zaog_obj_cover
      WHERE repo_key = @c_repo
      INTO @DATA(lv_cover_count).

    cl_abap_unit_assert=>assert_equals(
      act = lv_cover_count
      exp = 1
      msg = 'A blocked clear_repo must not have deleted the coverage row' ).

  ENDMETHOD.
ENDCLASS.
