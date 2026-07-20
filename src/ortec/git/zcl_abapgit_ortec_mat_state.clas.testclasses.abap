CLASS ltcl_mat_state DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  PRIVATE SECTION.
    CONSTANTS: c_repo1 TYPE zcl_abapgit_ortec_mat_state=>ty_repo_key VALUE 'ZAOG_TST_01',
               c_repo2 TYPE zcl_abapgit_ortec_mat_state=>ty_repo_key VALUE 'ZAOG_TST_02',
               c_sha1  TYPE zif_abapgit_git_definitions=>ty_sha1 VALUE 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA',
               c_branch TYPE string VALUE 'refs/heads/main'.

    CLASS-DATA gi_environment TYPE REF TO if_osql_test_environment.

    CLASS-METHODS class_setup.
    CLASS-METHODS class_teardown.

    METHODS setup.

    METHODS get_state_returns_initial FOR TESTING RAISING cx_static_check.
    METHODS get_state_legacy_row FOR TESTING RAISING cx_static_check.

    METHODS begin_attempt_creates_row FOR TESTING RAISING cx_static_check.
    METHODS begin_attempt_no_downgrade FOR TESTING RAISING cx_static_check.
    METHODS begin_attempt_sets_pending FOR TESTING RAISING cx_static_check.
    METHODS begin_attempt_keeps_complete FOR TESTING RAISING cx_static_check.

    METHODS mark_graph_complete_ok FOR TESTING RAISING cx_static_check.
    METHODS mark_graph_complete_stale FOR TESTING RAISING cx_static_check.
    METHODS mark_graph_no_attempt FOR TESTING RAISING cx_static_check.
    METHODS mark_graph_idempotent FOR TESTING RAISING cx_static_check.

    METHODS publish_snapshot_ok FOR TESTING RAISING cx_static_check.
    METHODS publish_snapshot_stale FOR TESTING RAISING cx_static_check.
    METHODS publish_from_unknown FOR TESTING RAISING cx_static_check.

    METHODS mark_full_complete_ok FOR TESTING RAISING cx_static_check.
    METHODS mark_full_complete_stale FOR TESTING RAISING cx_static_check.
    METHODS mark_full_requires_graph FOR TESTING RAISING cx_static_check.

    METHODS invalidate_commit_resets_row FOR TESTING RAISING cx_static_check.
    METHODS invalidate_commit_cascades FOR TESTING RAISING cx_static_check.

    METHODS clean_attempts_clears_stale FOR TESTING RAISING cx_static_check.
    METHODS clean_attempts_keeps_fresh FOR TESTING RAISING cx_static_check.

ENDCLASS.


CLASS ltcl_mat_state IMPLEMENTATION.

  METHOD class_setup.
    DATA lt_tables TYPE if_osql_test_environment=>ty_t_sobjnames.
    APPEND 'ZAOG_COMMIT_HIST' TO lt_tables.
    APPEND 'ZAOG_REPO_STATE' TO lt_tables.
    gi_environment = cl_osql_test_environment=>create( lt_tables ).
  ENDMETHOD.

  METHOD class_teardown.
    gi_environment->destroy( ).
  ENDMETHOD.

  METHOD setup.
    gi_environment->clear_doubles( ).
  ENDMETHOD.

  METHOD get_state_returns_initial.
    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    cl_abap_unit_assert=>assert_initial( ls_state-hist_level ).
    cl_abap_unit_assert=>assert_initial( ls_state-snap_state ).
  ENDMETHOD.

  METHOD get_state_legacy_row.
    " A row from before this feature existed: hist_level/snap_state are
    " space, exactly like any other never-touched CHAR1 field. get_state
    " must map this as-is, not reinterpret it as an error.
    DATA lt_hist TYPE STANDARD TABLE OF zaog_commit_hist WITH DEFAULT KEY.
    DATA ls_hist TYPE zaog_commit_hist.

    ls_hist-repo_key    = c_repo1.
    ls_hist-commit_sha1 = c_sha1.
    ls_hist-branch_name = c_branch.
    GET TIME STAMP FIELD ls_hist-fetched_at.
    APPEND ls_hist TO lt_hist.
    gi_environment->insert_test_data( lt_hist ).

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    cl_abap_unit_assert=>assert_initial( ls_state-hist_level ).
    cl_abap_unit_assert=>assert_initial( ls_state-snap_state ).
    cl_abap_unit_assert=>assert_equals( act = zcl_abapgit_ortec_mat_state=>is_graph_have_eligible(
                                                 iv_repo_key = c_repo1
                                                 iv_commit   = c_sha1 )
                                         exp = abap_false ).
    cl_abap_unit_assert=>assert_equals( act = zcl_abapgit_ortec_mat_state=>is_full_have_eligible(
                                                 iv_repo_key = c_repo1
                                                 iv_commit   = c_sha1 )
                                         exp = abap_false ).
  ENDMETHOD.

  METHOD begin_attempt_creates_row.
    DATA(lv_attempt) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    cl_abap_unit_assert=>assert_not_initial( lv_attempt ).

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    cl_abap_unit_assert=>assert_equals( act = ls_state-hist_level
                                         exp = zcl_abapgit_ortec_mat_state=>cs_hist_level-unknown ).
    cl_abap_unit_assert=>assert_equals( act = ls_state-snap_state
                                         exp = zcl_abapgit_ortec_mat_state=>cs_snap_state-pending ).
    cl_abap_unit_assert=>assert_equals( act = ls_state-attempt_id
                                         exp = lv_attempt ).
  ENDMETHOD.

  METHOD begin_attempt_no_downgrade.
    DATA(lv_attempt1) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    zcl_abapgit_ortec_mat_state=>mark_graph_complete(
      iv_repo_key   = c_repo1
      iv_commit     = c_sha1
      iv_attempt_id = lv_attempt1 ).

    " Starting a fresh attempt on an already-graph-complete commit must
    " not reset hist_level back to UNKNOWN.
    zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    cl_abap_unit_assert=>assert_equals( act = ls_state-hist_level
                                         exp = zcl_abapgit_ortec_mat_state=>cs_hist_level-graph_complete ).
  ENDMETHOD.

  METHOD begin_attempt_sets_pending.
    zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    cl_abap_unit_assert=>assert_equals( act = ls_state-snap_state
                                         exp = zcl_abapgit_ortec_mat_state=>cs_snap_state-pending ).
  ENDMETHOD.

  METHOD begin_attempt_keeps_complete.
    DATA(lv_attempt1) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    zcl_abapgit_ortec_mat_state=>mark_graph_complete(
      iv_repo_key   = c_repo1
      iv_commit     = c_sha1
      iv_attempt_id = lv_attempt1 ).

    zcl_abapgit_ortec_mat_state=>publish_snapshot_complete(
      iv_repo_key    = c_repo1
      iv_branch_name = c_branch
      iv_commit      = c_sha1
      iv_attempt_id  = lv_attempt1 ).

    " A new attempt must not downgrade an already-COMPLETE snap_state
    " back to PENDING.
    zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    cl_abap_unit_assert=>assert_equals( act = ls_state-snap_state
                                         exp = zcl_abapgit_ortec_mat_state=>cs_snap_state-complete ).
  ENDMETHOD.

  METHOD mark_graph_complete_ok.
    DATA(lv_attempt) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    zcl_abapgit_ortec_mat_state=>mark_graph_complete(
      iv_repo_key   = c_repo1
      iv_commit     = c_sha1
      iv_attempt_id = lv_attempt ).

    cl_abap_unit_assert=>assert_equals( act = zcl_abapgit_ortec_mat_state=>is_graph_have_eligible(
                                                 iv_repo_key = c_repo1
                                                 iv_commit   = c_sha1 )
                                         exp = abap_true ).
  ENDMETHOD.

  METHOD mark_graph_complete_stale.
    zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    TRY.
        zcl_abapgit_ortec_mat_state=>mark_graph_complete(
          iv_repo_key   = c_repo1
          iv_commit     = c_sha1
          iv_attempt_id = 'STALE_ATTEMPT_ID_NOT_REAL' ).
        cl_abap_unit_assert=>fail( 'Expected raise on stale attempt ID' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.

  METHOD mark_graph_no_attempt.
    TRY.
        zcl_abapgit_ortec_mat_state=>mark_graph_complete(
          iv_repo_key   = c_repo1
          iv_commit     = c_sha1
          iv_attempt_id = 'NO_ROW_EXISTS_AT_ALL_XXXXX' ).
        cl_abap_unit_assert=>fail( 'Expected raise when no attempt is in progress' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.

  METHOD mark_graph_idempotent.
    DATA(lv_attempt) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    zcl_abapgit_ortec_mat_state=>mark_graph_complete(
      iv_repo_key   = c_repo1
      iv_commit     = c_sha1
      iv_attempt_id = lv_attempt ).

    " Second call, even with a bogus attempt ID, must be a no-op since
    " hist_level is already GRAPH_COMPLETE - not raise stale-attempt.
    zcl_abapgit_ortec_mat_state=>mark_graph_complete(
      iv_repo_key   = c_repo1
      iv_commit     = c_sha1
      iv_attempt_id = 'SOME_OTHER_BOGUS_ATTEMPT_ID' ).

    cl_abap_unit_assert=>assert_equals( act = zcl_abapgit_ortec_mat_state=>is_graph_have_eligible(
                                                 iv_repo_key = c_repo1
                                                 iv_commit   = c_sha1 )
                                         exp = abap_true ).
  ENDMETHOD.

  METHOD publish_snapshot_ok.
    DATA(lv_attempt) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    zcl_abapgit_ortec_mat_state=>mark_graph_complete(
      iv_repo_key   = c_repo1
      iv_commit     = c_sha1
      iv_attempt_id = lv_attempt ).

    zcl_abapgit_ortec_mat_state=>publish_snapshot_complete(
      iv_repo_key    = c_repo1
      iv_branch_name = c_branch
      iv_commit      = c_sha1
      iv_attempt_id  = lv_attempt ).

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).
    cl_abap_unit_assert=>assert_equals( act = ls_state-snap_state
                                         exp = zcl_abapgit_ortec_mat_state=>cs_snap_state-complete ).

    DATA lv_branch TYPE c LENGTH 255.
    DATA ls_repo TYPE zaog_repo_state.
    lv_branch = c_branch.
    SELECT SINGLE * FROM zaog_repo_state INTO ls_repo
      WHERE repo_key = c_repo1 AND branch_name = lv_branch.
    cl_abap_unit_assert=>assert_subrc( exp = 0 ).
    cl_abap_unit_assert=>assert_equals( act = ls_repo-fetch_commit exp = c_sha1 ).
    cl_abap_unit_assert=>assert_equals( act = ls_repo-snap_state
                                         exp = zcl_abapgit_ortec_mat_state=>cs_snap_state-complete ).
  ENDMETHOD.

  METHOD publish_snapshot_stale.
    DATA(lv_attempt) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    zcl_abapgit_ortec_mat_state=>mark_graph_complete(
      iv_repo_key   = c_repo1
      iv_commit     = c_sha1
      iv_attempt_id = lv_attempt ).

    TRY.
        zcl_abapgit_ortec_mat_state=>publish_snapshot_complete(
          iv_repo_key    = c_repo1
          iv_branch_name = c_branch
          iv_commit      = c_sha1
          iv_attempt_id  = 'STALE_ATTEMPT_ID_NOT_REAL' ).
        cl_abap_unit_assert=>fail( 'Expected raise on stale attempt ID' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.

  METHOD publish_from_unknown.
    zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    " hist_level is still UNKNOWN (graph never certified) - publishing a
    " snapshot must be rejected regardless of attempt ID correctness.
    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    TRY.
        zcl_abapgit_ortec_mat_state=>publish_snapshot_complete(
          iv_repo_key    = c_repo1
          iv_branch_name = c_branch
          iv_commit      = c_sha1
          iv_attempt_id  = ls_state-attempt_id ).
        cl_abap_unit_assert=>fail( 'Expected raise: snapshot cannot precede graph' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.

  METHOD mark_full_complete_ok.
    DATA(lv_attempt) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    zcl_abapgit_ortec_mat_state=>mark_graph_complete(
      iv_repo_key   = c_repo1
      iv_commit     = c_sha1
      iv_attempt_id = lv_attempt ).

    zcl_abapgit_ortec_mat_state=>mark_full_complete(
      iv_repo_key   = c_repo1
      iv_commit     = c_sha1
      iv_attempt_id = lv_attempt ).

    cl_abap_unit_assert=>assert_equals( act = zcl_abapgit_ortec_mat_state=>is_full_have_eligible(
                                                 iv_repo_key = c_repo1
                                                 iv_commit   = c_sha1 )
                                         exp = abap_true ).
  ENDMETHOD.

  METHOD mark_full_complete_stale.
    DATA(lv_attempt) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    zcl_abapgit_ortec_mat_state=>mark_graph_complete(
      iv_repo_key   = c_repo1
      iv_commit     = c_sha1
      iv_attempt_id = lv_attempt ).

    TRY.
        zcl_abapgit_ortec_mat_state=>mark_full_complete(
          iv_repo_key   = c_repo1
          iv_commit     = c_sha1
          iv_attempt_id = 'STALE_ATTEMPT_ID_NOT_REAL' ).
        cl_abap_unit_assert=>fail( 'Expected raise on stale attempt ID' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.

  METHOD mark_full_requires_graph.
    DATA(lv_attempt) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    TRY.
        zcl_abapgit_ortec_mat_state=>mark_full_complete(
          iv_repo_key   = c_repo1
          iv_commit     = c_sha1
          iv_attempt_id = lv_attempt ).
        cl_abap_unit_assert=>fail( 'Expected raise: full completion requires graph-complete' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.

  METHOD invalidate_commit_resets_row.
    DATA(lv_attempt) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    zcl_abapgit_ortec_mat_state=>mark_graph_complete(
      iv_repo_key   = c_repo1
      iv_commit     = c_sha1
      iv_attempt_id = lv_attempt ).

    zcl_abapgit_ortec_mat_state=>invalidate_commit(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    cl_abap_unit_assert=>assert_equals( act = ls_state-hist_level
                                         exp = zcl_abapgit_ortec_mat_state=>cs_hist_level-unknown ).
    cl_abap_unit_assert=>assert_equals( act = ls_state-snap_state
                                         exp = zcl_abapgit_ortec_mat_state=>cs_snap_state-invalid ).
    cl_abap_unit_assert=>assert_initial( ls_state-attempt_id ).
  ENDMETHOD.

  METHOD invalidate_commit_cascades.
    DATA lv_branch TYPE c LENGTH 255.
    DATA ls_repo1 TYPE zaog_repo_state.
    DATA ls_repo2 TYPE zaog_repo_state.

    lv_branch = c_branch.

    " Two branches of the SAME repo point at the commit being invalidated.
    ls_repo1-repo_key    = c_repo1.
    ls_repo1-branch_name = lv_branch.
    ls_repo1-fetch_commit = c_sha1.
    ls_repo1-snap_state   = zcl_abapgit_ortec_mat_state=>cs_snap_state-complete.
    MODIFY zaog_repo_state FROM ls_repo1.

    ls_repo2-repo_key     = c_repo1.
    ls_repo2-branch_name  = 'refs/heads/dev'.
    ls_repo2-fetch_commit = c_sha1.
    ls_repo2-snap_state   = zcl_abapgit_ortec_mat_state=>cs_snap_state-complete.
    MODIFY zaog_repo_state FROM ls_repo2.

    " A branch of a DIFFERENT repo pointing at the same SHA1 value must
    " NOT be touched - the cascade is scoped by repo_key.
    DATA ls_repo_other TYPE zaog_repo_state.
    ls_repo_other-repo_key     = c_repo2.
    ls_repo_other-branch_name  = lv_branch.
    ls_repo_other-fetch_commit = c_sha1.
    ls_repo_other-snap_state   = zcl_abapgit_ortec_mat_state=>cs_snap_state-complete.
    MODIFY zaog_repo_state FROM ls_repo_other.

    zcl_abapgit_ortec_mat_state=>invalidate_commit(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    DATA ls_check TYPE zaog_repo_state.

    SELECT SINGLE * FROM zaog_repo_state INTO ls_check
      WHERE repo_key = c_repo1 AND branch_name = lv_branch.
    cl_abap_unit_assert=>assert_equals( act = ls_check-snap_state
                                         exp = zcl_abapgit_ortec_mat_state=>cs_snap_state-invalid ).

    SELECT SINGLE * FROM zaog_repo_state INTO ls_check
      WHERE repo_key = c_repo1 AND branch_name = 'refs/heads/dev'.
    cl_abap_unit_assert=>assert_equals( act = ls_check-snap_state
                                         exp = zcl_abapgit_ortec_mat_state=>cs_snap_state-invalid ).

    SELECT SINGLE * FROM zaog_repo_state INTO ls_check
      WHERE repo_key = c_repo2 AND branch_name = lv_branch.
    cl_abap_unit_assert=>assert_equals( act = ls_check-snap_state
                                         exp = zcl_abapgit_ortec_mat_state=>cs_snap_state-complete ).
  ENDMETHOD.

  METHOD clean_attempts_clears_stale.
    DATA ls_hist TYPE zaog_commit_hist.
    DATA lt_hist TYPE STANDARD TABLE OF zaog_commit_hist WITH DEFAULT KEY.
    DATA lv_old_ts TYPE timestampl.

    GET TIME STAMP FIELD lv_old_ts.
    lv_old_ts = cl_abap_tstmp=>subtractsecs( tstmp = lv_old_ts secs = 100 * 3600 ).

    ls_hist-repo_key    = c_repo1.
    ls_hist-commit_sha1 = c_sha1.
    ls_hist-branch_name = c_branch.
    ls_hist-hist_level  = zcl_abapgit_ortec_mat_state=>cs_hist_level-unknown.
    ls_hist-snap_state  = zcl_abapgit_ortec_mat_state=>cs_snap_state-pending.
    ls_hist-attempt_id  = 'STALE_ATTEMPT_TO_BE_CLEARED'.
    ls_hist-updated_at  = lv_old_ts.
    APPEND ls_hist TO lt_hist.
    gi_environment->insert_test_data( lt_hist ).

    DATA(lv_cleaned) = zcl_abapgit_ortec_mat_state=>clean_incomplete_attempts(
      iv_repo_key      = c_repo1
      iv_max_age_hours = 24 ).

    cl_abap_unit_assert=>assert_equals( act = lv_cleaned exp = 1 ).

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).
    cl_abap_unit_assert=>assert_initial( ls_state-attempt_id ).
    " hist_level/snap_state are left untouched by cleanup.
    cl_abap_unit_assert=>assert_equals( act = ls_state-hist_level
                                         exp = zcl_abapgit_ortec_mat_state=>cs_hist_level-unknown ).
  ENDMETHOD.

  METHOD clean_attempts_keeps_fresh.
    DATA(lv_attempt) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).

    DATA(lv_cleaned) = zcl_abapgit_ortec_mat_state=>clean_incomplete_attempts(
      iv_repo_key      = c_repo1
      iv_max_age_hours = 24 ).

    cl_abap_unit_assert=>assert_equals( act = lv_cleaned exp = 0 ).

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state(
      iv_repo_key = c_repo1
      iv_commit   = c_sha1 ).
    cl_abap_unit_assert=>assert_equals( act = ls_state-attempt_id exp = lv_attempt ).
  ENDMETHOD.

ENDCLASS.
