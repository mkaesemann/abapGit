CLASS ltcl_have_policy DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  PRIVATE SECTION.
    CONSTANTS: c_repo1  TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key VALUE 'ZAOG_HVP_01',
               c_repo2  TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key VALUE 'ZAOG_HVP_02',
               c_branch TYPE string VALUE 'refs/heads/main'.

    METHODS setup.
    METHODS teardown.

    METHODS cleanup_repo
      IMPORTING iv_repo TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.

    METHODS certify_commit
      IMPORTING iv_repo       TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                iv_commit     TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_full       TYPE abap_bool DEFAULT abap_true
                iv_updated_at TYPE timestampl OPTIONAL
      RAISING   cx_static_check.

    METHODS build_complete_commit
      IMPORTING iv_repo          TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                iv_store_blob    TYPE abap_bool DEFAULT abap_true
      RETURNING VALUE(rv_commit) TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING   cx_static_check.

    METHODS full_certified_have_eligible FOR TESTING RAISING cx_static_check.
    METHODS graph_only_have_ineligible FOR TESTING RAISING cx_static_check.
    METHODS uncertified_have_ineligible FOR TESTING RAISING cx_static_check.
    METHODS other_repo_excluded FOR TESTING RAISING cx_static_check.
    METHODS want_excluded_from_haves FOR TESTING RAISING cx_static_check.
    METHODS haves_ordered_deterministic FOR TESTING RAISING cx_static_check.
    METHODS haves_capped_at_max FOR TESTING RAISING cx_static_check.
    METHODS haves_empty_when_none FOR TESTING RAISING cx_static_check.

    METHODS classify_warm_unchanged FOR TESTING RAISING cx_static_check.
    METHODS classify_incremental_update FOR TESTING RAISING cx_static_check.
    METHODS classify_cold_branch FOR TESTING RAISING cx_static_check.
    METHODS classify_no_side_effects FOR TESTING RAISING cx_static_check.

    METHODS backfill_skips_never_seen FOR TESTING RAISING cx_static_check.
    METHODS backfill_completes_locally FOR TESTING RAISING cx_static_check.
    METHODS backfill_incomplete_no_publish FOR TESTING RAISING cx_static_check.
    METHODS backfill_repeat_idempotent FOR TESTING RAISING cx_static_check.

ENDCLASS.


CLASS ltcl_have_policy IMPLEMENTATION.

  METHOD setup.
    cleanup_repo( c_repo1 ).
    cleanup_repo( c_repo2 ).
  ENDMETHOD.

  METHOD teardown.
    cleanup_repo( c_repo1 ).
    cleanup_repo( c_repo2 ).
  ENDMETHOD.

  METHOD cleanup_repo.
    " try_backfill_target issues its own COMMIT WORK, so a bare ROLLBACK
    " WORK cannot be trusted to undo prior test data - explicitly delete
    " and commit the deletion itself (see repo memory: DELETE + ROLLBACK
    " WORK only cleans up correctly when the SUT never commits).
    ROLLBACK WORK.
    DELETE FROM zaog_commit_hist WHERE repo_key = iv_repo.
    DELETE FROM zaog_obj_store WHERE repo_key = iv_repo.
    DELETE FROM zaog_repo_state WHERE repo_key = iv_repo.
    COMMIT WORK.
  ENDMETHOD.

  METHOD certify_commit.
    DATA lv_attempt TYPE zcl_abapgit_ortec_mat_state=>ty_attempt_id.

    lv_attempt = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = iv_repo
      iv_commit   = iv_commit ).
    zcl_abapgit_ortec_mat_state=>mark_graph_complete(
      iv_repo_key   = iv_repo
      iv_commit     = iv_commit
      iv_attempt_id = lv_attempt ).

    IF iv_full = abap_true.
      zcl_abapgit_ortec_mat_state=>mark_full_complete(
        iv_repo_key   = iv_repo
        iv_commit     = iv_commit
        iv_attempt_id = lv_attempt ).
    ENDIF.

    IF iv_updated_at IS NOT INITIAL.
      UPDATE zaog_commit_hist SET updated_at = iv_updated_at
        WHERE repo_key = iv_repo AND commit_sha1 = iv_commit.
    ENDIF.
  ENDMETHOD.

  METHOD build_complete_commit.
    DATA lt_nodes      TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node       LIKE LINE OF lt_nodes.
    DATA ls_commit     TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lv_blob_data  TYPE xstring.
    DATA lv_blob_sha   TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_tree_data  TYPE xstring.
    DATA lv_tree_sha   TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_commit_data TYPE xstring.

    lv_blob_data = zcl_abapgit_convert=>string_to_xstring_utf8( 'hello' ).
    lv_blob_sha  = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).

    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_node-name  = 'hello.txt'.
    ls_node-sha1  = lv_blob_sha.
    APPEND ls_node TO lt_nodes.

    lv_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    lv_tree_sha  = zcl_abapgit_hash=>sha1_tree( lv_tree_data ).

    ls_commit-tree      = lv_tree_sha.
    ls_commit-author    = 'Test <test@example.com> 0 +0000'.
    ls_commit-committer = 'Test <test@example.com> 0 +0000'.
    ls_commit-body      = 'have policy test'.
    lv_commit_data = zcl_abapgit_git_pack=>encode_commit( ls_commit ).
    rv_commit      = zcl_abapgit_hash=>sha1_commit( lv_commit_data ).

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = iv_repo
      iv_sha1     = rv_commit
      iv_type     = zif_abapgit_git_definitions=>c_type-commit
      iv_data     = lv_commit_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = iv_repo
      iv_sha1     = lv_tree_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-tree
      iv_data     = lv_tree_data ).

    IF iv_store_blob = abap_true.
      zcl_abapgit_ortec_obj_store=>store_object(
        iv_repo_key = iv_repo
        iv_sha1     = lv_blob_sha
        iv_type     = zif_abapgit_git_definitions=>c_type-blob
        iv_data     = lv_blob_data ).
    ENDIF.
  ENDMETHOD.

  METHOD full_certified_have_eligible.
    certify_commit( iv_repo = c_repo1 iv_commit = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' ).

    DATA(lt_haves) = zcl_abapgit_ortec_have_policy=>get_certified_haves( iv_repo_key = c_repo1 ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_haves ) exp = 1 ).
    cl_abap_unit_assert=>assert_equals(
      act = line_exists( lt_haves[ table_line = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' ] )
      exp = abap_true ).
  ENDMETHOD.

  METHOD graph_only_have_ineligible.
    certify_commit( iv_repo = c_repo1 iv_commit = 'BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB' iv_full = abap_false ).

    DATA(lt_haves) = zcl_abapgit_ortec_have_policy=>get_certified_haves( iv_repo_key = c_repo1 ).

    cl_abap_unit_assert=>assert_initial( lt_haves ).
  ENDMETHOD.

  METHOD uncertified_have_ineligible.
    " Mimics today's live defect (design §0): persist_pull_result's raw
    " INSERT leaves HIST_LEVEL space.
    DATA ls_row TYPE zaog_commit_hist.

    ls_row-repo_key    = c_repo1.
    ls_row-commit_sha1 = 'CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC'.
    ls_row-branch_name = c_branch.
    GET TIME STAMP FIELD ls_row-fetched_at.
    MODIFY zaog_commit_hist FROM ls_row.
    COMMIT WORK.

    DATA(lt_haves) = zcl_abapgit_ortec_have_policy=>get_certified_haves( iv_repo_key = c_repo1 ).

    cl_abap_unit_assert=>assert_initial( lt_haves ).
  ENDMETHOD.

  METHOD other_repo_excluded.
    certify_commit( iv_repo = c_repo2 iv_commit = 'DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD' ).

    DATA(lt_haves) = zcl_abapgit_ortec_have_policy=>get_certified_haves( iv_repo_key = c_repo1 ).

    cl_abap_unit_assert=>assert_initial( lt_haves ).
  ENDMETHOD.

  METHOD want_excluded_from_haves.
    certify_commit( iv_repo = c_repo1 iv_commit = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' ).
    certify_commit( iv_repo = c_repo1 iv_commit = 'EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE' ).

    DATA(lt_haves) = zcl_abapgit_ortec_have_policy=>get_certified_haves(
      iv_repo_key    = c_repo1
      it_want_hashes = VALUE #( ( 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' ) ) ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_haves ) exp = 1 ).
    cl_abap_unit_assert=>assert_equals(
      act = line_exists( lt_haves[ table_line = 'EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE' ] )
      exp = abap_true ).
  ENDMETHOD.

  METHOD haves_ordered_deterministic.
    DATA lv_t1 TYPE timestampl.
    DATA lv_t2 TYPE timestampl.

    GET TIME STAMP FIELD lv_t1.
    lv_t2 = lv_t1 + 3600. " one hour later

    " Two commits share the same (older) UPDATED_AT - tie-break must be
    " COMMIT_SHA1 ascending. A third, newer commit must sort first.
    certify_commit( iv_repo = c_repo1 iv_commit = 'BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB' iv_updated_at = lv_t1 ).
    certify_commit( iv_repo = c_repo1 iv_commit = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' iv_updated_at = lv_t1 ).
    certify_commit( iv_repo = c_repo1 iv_commit = 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF' iv_updated_at = lv_t2 ).

    DATA(lt_haves) = zcl_abapgit_ortec_have_policy=>get_certified_haves( iv_repo_key = c_repo1 ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_haves ) exp = 3 ).
    cl_abap_unit_assert=>assert_equals( act = lt_haves[ 1 ] exp = 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF' ).
    cl_abap_unit_assert=>assert_equals( act = lt_haves[ 2 ] exp = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' ).
    cl_abap_unit_assert=>assert_equals( act = lt_haves[ 3 ] exp = 'BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB' ).
  ENDMETHOD.

  METHOD haves_capped_at_max.
    certify_commit( iv_repo = c_repo1 iv_commit = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' ).
    certify_commit( iv_repo = c_repo1 iv_commit = 'BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB' ).
    certify_commit( iv_repo = c_repo1 iv_commit = 'CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC' ).

    DATA(lt_haves) = zcl_abapgit_ortec_have_policy=>get_certified_haves(
      iv_repo_key  = c_repo1
      iv_max_haves = 2 ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_haves ) exp = 2 ).
  ENDMETHOD.

  METHOD haves_empty_when_none.
    DATA(lt_haves) = zcl_abapgit_ortec_have_policy=>get_certified_haves( iv_repo_key = c_repo1 ).

    cl_abap_unit_assert=>assert_initial( lt_haves ).
  ENDMETHOD.

  METHOD classify_warm_unchanged.
    DATA lv_attempt TYPE zcl_abapgit_ortec_mat_state=>ty_attempt_id.
    DATA lv_commit  TYPE zif_abapgit_git_definitions=>ty_sha1 VALUE 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'.

    lv_attempt = zcl_abapgit_ortec_mat_state=>begin_attempt( iv_repo_key = c_repo1 iv_commit = lv_commit ).
    zcl_abapgit_ortec_mat_state=>mark_graph_complete(
      iv_repo_key = c_repo1 iv_commit = lv_commit iv_attempt_id = lv_attempt ).
    zcl_abapgit_ortec_mat_state=>mark_full_complete(
      iv_repo_key = c_repo1 iv_commit = lv_commit iv_attempt_id = lv_attempt ).
    zcl_abapgit_ortec_mat_state=>publish_snapshot_complete(
      iv_repo_key = c_repo1 iv_branch_name = c_branch iv_commit = lv_commit iv_attempt_id = lv_attempt ).

    DATA(lv_class) = zcl_abapgit_ortec_have_policy=>classify_operation(
      iv_repo_key = c_repo1 iv_target_commit = lv_commit ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_class exp = zcl_abapgit_ortec_have_policy=>cs_op_class-warm_unchanged ).
  ENDMETHOD.

  METHOD classify_incremental_update.
    certify_commit( iv_repo = c_repo1 iv_commit = 'BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB' ).

    DATA(lv_class) = zcl_abapgit_ortec_have_policy=>classify_operation(
      iv_repo_key       = c_repo1
      iv_target_commit  = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_class exp = zcl_abapgit_ortec_have_policy=>cs_op_class-incremental_update ).
  ENDMETHOD.

  METHOD classify_cold_branch.
    DATA(lv_class) = zcl_abapgit_ortec_have_policy=>classify_operation(
      iv_repo_key       = c_repo1
      iv_target_commit  = 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA' ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_class exp = zcl_abapgit_ortec_have_policy=>cs_op_class-cold_branch ).
  ENDMETHOD.

  METHOD classify_no_side_effects.
    DATA lv_commit TYPE zif_abapgit_git_definitions=>ty_sha1 VALUE 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'.
    DATA ls_row    TYPE zaog_commit_hist.

    zcl_abapgit_ortec_have_policy=>classify_operation( iv_repo_key = c_repo1 iv_target_commit = lv_commit ).
    zcl_abapgit_ortec_have_policy=>classify_operation( iv_repo_key = c_repo1 iv_target_commit = lv_commit ).

    SELECT SINGLE * FROM zaog_commit_hist INTO ls_row
      WHERE repo_key = c_repo1 AND commit_sha1 = lv_commit.

    cl_abap_unit_assert=>assert_equals( act = sy-subrc exp = 4 ).
  ENDMETHOD.

  METHOD backfill_skips_never_seen.
    DATA lv_commit TYPE zif_abapgit_git_definitions=>ty_sha1 VALUE 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'.

    DATA(lv_certified) = zcl_abapgit_ortec_have_policy=>try_backfill_target(
      iv_repo_key = c_repo1 iv_target_commit = lv_commit ).

    cl_abap_unit_assert=>assert_equals( act = lv_certified exp = abap_false ).

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state( iv_repo_key = c_repo1 iv_commit = lv_commit ).
    cl_abap_unit_assert=>assert_initial( ls_state-hist_level ).
  ENDMETHOD.

  METHOD backfill_completes_locally.
    DATA(lv_commit) = build_complete_commit( iv_repo = c_repo1 iv_store_blob = abap_true ).

    DATA(lv_certified) = zcl_abapgit_ortec_have_policy=>try_backfill_target(
      iv_repo_key      = c_repo1
      iv_target_commit = lv_commit
      iv_branch_name   = c_branch ).

    cl_abap_unit_assert=>assert_equals( act = lv_certified exp = abap_true ).

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state( iv_repo_key = c_repo1 iv_commit = lv_commit ).
    cl_abap_unit_assert=>assert_equals(
      act = ls_state-hist_level exp = zcl_abapgit_ortec_mat_state=>cs_hist_level-full_complete ).
    cl_abap_unit_assert=>assert_equals(
      act = ls_state-snap_state exp = zcl_abapgit_ortec_mat_state=>cs_snap_state-complete ).

    DATA(lv_class) = zcl_abapgit_ortec_have_policy=>classify_operation(
      iv_repo_key = c_repo1 iv_target_commit = lv_commit ).
    cl_abap_unit_assert=>assert_equals(
      act = lv_class exp = zcl_abapgit_ortec_have_policy=>cs_op_class-warm_unchanged ).
  ENDMETHOD.

  METHOD backfill_incomplete_no_publish.
    " Tree references a blob leaf that is deliberately never stored.
    DATA(lv_commit) = build_complete_commit( iv_repo = c_repo1 iv_store_blob = abap_false ).

    DATA(lv_certified) = zcl_abapgit_ortec_have_policy=>try_backfill_target(
      iv_repo_key      = c_repo1
      iv_target_commit = lv_commit
      iv_branch_name   = c_branch ).

    cl_abap_unit_assert=>assert_equals( act = lv_certified exp = abap_false ).

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state( iv_repo_key = c_repo1 iv_commit = lv_commit ).
    cl_abap_unit_assert=>assert_equals(
      act = ls_state-hist_level exp = zcl_abapgit_ortec_mat_state=>cs_hist_level-graph_complete ).
    cl_abap_unit_assert=>assert_differs(
      act = ls_state-snap_state exp = zcl_abapgit_ortec_mat_state=>cs_snap_state-complete ).
  ENDMETHOD.

  METHOD backfill_repeat_idempotent.
    DATA(lv_commit) = build_complete_commit( iv_repo = c_repo1 iv_store_blob = abap_true ).

    DATA(lv_certified_1) = zcl_abapgit_ortec_have_policy=>try_backfill_target(
      iv_repo_key      = c_repo1
      iv_target_commit = lv_commit
      iv_branch_name   = c_branch ).
    DATA(lv_certified_2) = zcl_abapgit_ortec_have_policy=>try_backfill_target(
      iv_repo_key      = c_repo1
      iv_target_commit = lv_commit
      iv_branch_name   = c_branch ).

    cl_abap_unit_assert=>assert_equals( act = lv_certified_1 exp = abap_true ).
    cl_abap_unit_assert=>assert_equals( act = lv_certified_2 exp = abap_true ).

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state( iv_repo_key = c_repo1 iv_commit = lv_commit ).
    cl_abap_unit_assert=>assert_equals(
      act = ls_state-hist_level exp = zcl_abapgit_ortec_mat_state=>cs_hist_level-full_complete ).
    cl_abap_unit_assert=>assert_equals(
      act = ls_state-snap_state exp = zcl_abapgit_ortec_mat_state=>cs_snap_state-complete ).
  ENDMETHOD.

ENDCLASS.
