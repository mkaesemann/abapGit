CLASS zcl_abapgit_ortec_fastpath DEFINITION LOCAL FRIENDS ltcl_fastpath.

CLASS ltcl_fastpath DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  PRIVATE SECTION.
    CONSTANTS: c_repo1  TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key VALUE 'ZAOG_FPR_01',
               c_branch TYPE string VALUE 'refs/heads/main'.

    METHODS setup.
    METHODS teardown.

    METHODS cleanup_repo
      IMPORTING iv_repo TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.

    "! Builds and stores a commit -> tree -> blob object graph (design §0
    "! fixture pattern, matches zcl_abapgit_ortec_have_policy's tests).
    METHODS build_commit
      IMPORTING iv_repo          TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                iv_store_tree    TYPE abap_bool DEFAULT abap_true
                iv_store_blob    TYPE abap_bool DEFAULT abap_true
      RETURNING VALUE(rv_commit) TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING   cx_static_check.

    METHODS certify_closure_incomplete FOR TESTING RAISING cx_static_check.
    METHODS certify_missing_blob       FOR TESTING RAISING cx_static_check.
    METHODS certify_full_publishes     FOR TESTING RAISING cx_static_check.
    METHODS certify_idempotent_repeat  FOR TESTING RAISING cx_static_check.

    METHODS persist_stores_new_objects FOR TESTING RAISING cx_static_check.
    METHODS persist_skips_existing_obj FOR TESTING RAISING cx_static_check.
    METHODS persist_dup_sha_once       FOR TESTING RAISING cx_static_check.
    METHODS persist_ignores_other      FOR TESTING RAISING cx_static_check.

ENDCLASS.


CLASS ltcl_fastpath IMPLEMENTATION.

  METHOD setup.
    cleanup_repo( c_repo1 ).
  ENDMETHOD.

  METHOD teardown.
    cleanup_repo( c_repo1 ).
  ENDMETHOD.

  METHOD cleanup_repo.
    " certify_fetched_commit's callers (mat_state) issue no COMMIT WORK of
    " their own, but this test class calls zcl_abapgit_ortec_obj_store=>
    " store_object, which does - so DELETE + bare ROLLBACK WORK cannot be
    " trusted (see repo memory: DELETE + ROLLBACK WORK only cleans up
    " correctly when the SUT never commits).
    ROLLBACK WORK.
    DELETE FROM zaog_commit_hist WHERE repo_key = iv_repo.
    DELETE FROM zaog_obj_store WHERE repo_key = iv_repo.
    DELETE FROM zaog_repo_state WHERE repo_key = iv_repo.
    COMMIT WORK.
  ENDMETHOD.

  METHOD build_commit.
    DATA lt_nodes       TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node        LIKE LINE OF lt_nodes.
    DATA ls_commit      TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lv_blob_data   TYPE xstring.
    DATA lv_blob_sha    TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_tree_data   TYPE xstring.
    DATA lv_tree_sha    TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_commit_data TYPE xstring.

    lv_blob_data = zcl_abapgit_convert=>string_to_xstring_utf8( 'fastpath test' ).
    lv_blob_sha  = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).

    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_node-name  = 'fastpath.txt'.
    ls_node-sha1  = lv_blob_sha.
    APPEND ls_node TO lt_nodes.

    lv_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    lv_tree_sha  = zcl_abapgit_hash=>sha1_tree( lv_tree_data ).

    ls_commit-tree      = lv_tree_sha.
    ls_commit-author    = 'Test <test@example.com> 0 +0000'.
    ls_commit-committer = 'Test <test@example.com> 0 +0000'.
    ls_commit-body      = 'fastpath persist test'.
    lv_commit_data = zcl_abapgit_git_pack=>encode_commit( ls_commit ).
    rv_commit      = zcl_abapgit_hash=>sha1_commit( lv_commit_data ).

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = iv_repo
      iv_sha1     = rv_commit
      iv_type     = zif_abapgit_git_definitions=>c_type-commit
      iv_data     = lv_commit_data ).

    IF iv_store_tree = abap_true.
      zcl_abapgit_ortec_obj_store=>store_object(
        iv_repo_key = iv_repo
        iv_sha1     = lv_tree_sha
        iv_type     = zif_abapgit_git_definitions=>c_type-tree
        iv_data     = lv_tree_data ).
    ENDIF.

    IF iv_store_blob = abap_true.
      zcl_abapgit_ortec_obj_store=>store_object(
        iv_repo_key = iv_repo
        iv_sha1     = lv_blob_sha
        iv_type     = zif_abapgit_git_definitions=>c_type-blob
        iv_data     = lv_blob_data ).
    ENDIF.
  ENDMETHOD.

  METHOD certify_closure_incomplete.
    " Commit stored, tree/blob missing - verify_tree_closure must raise
    " inside certify_fetched_commit, so no certificate is published (test
    " list item 8: "closure failure prevents graph/full/snapshot
    " publication").
    DATA(lv_commit) = build_commit(
      iv_repo       = c_repo1
      iv_store_tree = abap_false
      iv_store_blob = abap_false ).

    zcl_abapgit_ortec_fastpath=>certify_fetched_commit(
      iv_repo_key    = c_repo1
      iv_commit      = lv_commit
      iv_branch_name = c_branch ).

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).

    cl_abap_unit_assert=>assert_differs(
      act = ls_state-hist_level
      exp = zcl_abapgit_ortec_mat_state=>cs_hist_level-graph_complete ).
    cl_abap_unit_assert=>assert_differs(
      act = ls_state-snap_state
      exp = zcl_abapgit_ortec_mat_state=>cs_snap_state-complete ).
  ENDMETHOD.

  METHOD certify_missing_blob.
    " Commit + tree stored (closure OK), blob missing - graph completes but
    " full/snapshot completeness must not be published (test list item 9).
    DATA(lv_commit) = build_commit(
      iv_repo       = c_repo1
      iv_store_tree = abap_true
      iv_store_blob = abap_false ).

    zcl_abapgit_ortec_fastpath=>certify_fetched_commit(
      iv_repo_key    = c_repo1
      iv_commit      = lv_commit
      iv_branch_name = c_branch ).

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_state-hist_level
      exp = zcl_abapgit_ortec_mat_state=>cs_hist_level-graph_complete ).
    cl_abap_unit_assert=>assert_differs(
      act = ls_state-snap_state
      exp = zcl_abapgit_ortec_mat_state=>cs_snap_state-complete ).
  ENDMETHOD.

  METHOD certify_full_publishes.
    " Commit + tree + blob all stored - full certification and snapshot
    " completeness must be published, and the commit must then be usable
    " as a C1 certified have (test list item 10, plus an end-to-end proof
    " that C2's certification actually feeds C1's have policy).
    DATA(lv_commit) = build_commit( iv_repo = c_repo1 ).

    zcl_abapgit_ortec_fastpath=>certify_fetched_commit(
      iv_repo_key    = c_repo1
      iv_commit      = lv_commit
      iv_branch_name = c_branch ).

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_state-hist_level
      exp = zcl_abapgit_ortec_mat_state=>cs_hist_level-full_complete ).
    cl_abap_unit_assert=>assert_equals(
      act = ls_state-snap_state
      exp = zcl_abapgit_ortec_mat_state=>cs_snap_state-complete ).

    DATA(lt_haves) = zcl_abapgit_ortec_have_policy=>get_certified_haves( iv_repo_key = c_repo1 ).

    DATA(lv_have) = abap_false.
    IF line_exists( lt_haves[ table_line = lv_commit ] ).
      lv_have = abap_true.
    ENDIF.

    cl_abap_unit_assert=>assert_equals(
      act = lv_have
      exp = abap_true ).
  ENDMETHOD.

  METHOD certify_idempotent_repeat.
    " Calling certify_fetched_commit again for an already FULL_COMPLETE
    " commit must not raise and must not downgrade the certificate.
    DATA(lv_commit) = build_commit( iv_repo = c_repo1 ).

    zcl_abapgit_ortec_fastpath=>certify_fetched_commit(
      iv_repo_key    = c_repo1
      iv_commit      = lv_commit
      iv_branch_name = c_branch ).
    zcl_abapgit_ortec_fastpath=>certify_fetched_commit(
      iv_repo_key    = c_repo1
      iv_commit      = lv_commit
      iv_branch_name = c_branch ).

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_state-hist_level
      exp = zcl_abapgit_ortec_mat_state=>cs_hist_level-full_complete ).
    cl_abap_unit_assert=>assert_equals(
      act = ls_state-snap_state
      exp = zcl_abapgit_ortec_mat_state=>cs_snap_state-complete ).
  ENDMETHOD.

  METHOD persist_stores_new_objects.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object  LIKE LINE OF lt_objects.

    ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-sha1 = zcl_abapgit_hash=>sha1_blob( zcl_abapgit_convert=>string_to_xstring_utf8( 'x' ) ).
    ls_object-data = zcl_abapgit_convert=>string_to_xstring_utf8( 'x' ).
    APPEND ls_object TO lt_objects.

    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = c_repo1 iv_sha1 = ls_object-sha1 )
      exp = abap_false ).

    zcl_abapgit_ortec_fastpath=>persist_missing_objects(
      iv_repo_key = c_repo1
      it_objects  = lt_objects ).

    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = c_repo1 iv_sha1 = ls_object-sha1 )
      exp = abap_true ).
  ENDMETHOD.

  METHOD persist_skips_existing_obj.
    DATA ls_object  TYPE zif_abapgit_definitions=>ty_object.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_count   TYPE i.

    ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-sha1 = zcl_abapgit_hash=>sha1_blob( zcl_abapgit_convert=>string_to_xstring_utf8( 'existing' ) ).
    ls_object-data = zcl_abapgit_convert=>string_to_xstring_utf8( 'existing' ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = c_repo1
      iv_sha1     = ls_object-sha1
      iv_type     = ls_object-type
      iv_data     = ls_object-data ).
    APPEND ls_object TO lt_objects.

    SELECT COUNT( * ) FROM zaog_obj_store
      INTO @lv_count
      WHERE repo_key = @c_repo1
        AND obj_sha1 = @ls_object-sha1.

    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 1 ).

    zcl_abapgit_ortec_fastpath=>persist_missing_objects(
      iv_repo_key = c_repo1
      it_objects  = lt_objects ).

    SELECT COUNT( * ) FROM zaog_obj_store
      INTO @lv_count
      WHERE repo_key = @c_repo1
        AND obj_sha1 = @ls_object-sha1.

    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 1 ).
  ENDMETHOD.

  METHOD persist_dup_sha_once.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object  LIKE LINE OF lt_objects.
    DATA ls_object2 LIKE LINE OF lt_objects.
    DATA lv_count   TYPE i.

    ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-sha1 = zcl_abapgit_hash=>sha1_blob( zcl_abapgit_convert=>string_to_xstring_utf8( 'dup' ) ).
    ls_object-data = zcl_abapgit_convert=>string_to_xstring_utf8( 'dup' ).
    APPEND ls_object TO lt_objects.

    ls_object2-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object2-sha1 = ls_object-sha1.
    ls_object2-data = zcl_abapgit_convert=>string_to_xstring_utf8( 'dup' ).
    APPEND ls_object2 TO lt_objects.

    zcl_abapgit_ortec_fastpath=>persist_missing_objects(
      iv_repo_key = c_repo1
      it_objects  = lt_objects ).

    SELECT COUNT( * ) FROM zaog_obj_store
      INTO @lv_count
      WHERE repo_key = @c_repo1
        AND obj_sha1 = @ls_object-sha1.

    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 1 ).
  ENDMETHOD.

  METHOD persist_ignores_other.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_new     LIKE LINE OF lt_objects.
    DATA ls_existing1 TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_existing2 TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_before  TYPE i.
    DATA lv_after   TYPE i.

    ls_existing1-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_existing1-sha1 = zcl_abapgit_hash=>sha1_blob( zcl_abapgit_convert=>string_to_xstring_utf8( 'one' ) ).
    ls_existing1-data = zcl_abapgit_convert=>string_to_xstring_utf8( 'one' ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = c_repo1
      iv_sha1     = ls_existing1-sha1
      iv_type     = ls_existing1-type
      iv_data     = ls_existing1-data ).

    ls_existing2-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_existing2-sha1 = zcl_abapgit_hash=>sha1_blob( zcl_abapgit_convert=>string_to_xstring_utf8( 'two' ) ).
    ls_existing2-data = zcl_abapgit_convert=>string_to_xstring_utf8( 'two' ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = c_repo1
      iv_sha1     = ls_existing2-sha1
      iv_type     = ls_existing2-type
      iv_data     = ls_existing2-data ).

    ls_new-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_new-sha1 = zcl_abapgit_hash=>sha1_blob( zcl_abapgit_convert=>string_to_xstring_utf8( 'new' ) ).
    ls_new-data = zcl_abapgit_convert=>string_to_xstring_utf8( 'new' ).
    APPEND ls_new TO lt_objects.

    SELECT COUNT( * ) FROM zaog_obj_store
      INTO @lv_before
      WHERE repo_key = @c_repo1.

    zcl_abapgit_ortec_fastpath=>persist_missing_objects(
      iv_repo_key = c_repo1
      it_objects  = lt_objects ).

    SELECT COUNT( * ) FROM zaog_obj_store
      INTO @lv_after
      WHERE repo_key = @c_repo1.

    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = c_repo1 iv_sha1 = ls_new-sha1 )
      exp = abap_true ).
    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = c_repo1 iv_sha1 = ls_existing1-sha1 )
      exp = abap_true ).
    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = c_repo1 iv_sha1 = ls_existing2-sha1 )
      exp = abap_true ).
    cl_abap_unit_assert=>assert_equals( act = lv_after - lv_before exp = 1 ).
  ENDMETHOD.

ENDCLASS.
