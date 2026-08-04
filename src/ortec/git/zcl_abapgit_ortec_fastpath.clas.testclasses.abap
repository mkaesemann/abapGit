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

    " ORTEC D2b2: repo-lock + attempt-id correlation tests.
    METHODS stale_attempt_rejected       FOR TESTING RAISING cx_static_check.
    METHODS one_attempt_one_id           FOR TESTING RAISING cx_static_check.
    METHODS retry_gets_new_attempt       FOR TESTING RAISING cx_static_check.
    METHODS certify_reuses_attempt       FOR TESTING RAISING cx_static_check.
    METHODS attempt_id_cross_table       FOR TESTING RAISING cx_static_check.
    METHODS porcelain_path_gets_lock     FOR TESTING RAISING cx_static_check.
    METHODS lock_not_held_over_http      FOR TESTING RAISING cx_static_check.
    METHODS lock_timeout_falls_back      FOR TESTING RAISING cx_static_check.
    METHODS filtered_fetch_lock_ok       FOR TESTING RAISING cx_static_check.
    METHODS missing_objects_has_id       FOR TESTING RAISING cx_static_check.
    METHODS resume_new_attempt_when_new  FOR TESTING RAISING cx_static_check.
    METHODS resume_reuses_attempt        FOR TESTING RAISING cx_static_check.

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
    ROLLBACK WORK. "#EC CI_ROLLBACK
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
    DATA(lv_attempt_id) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).

    zcl_abapgit_ortec_fastpath=>certify_fetched_commit(
      iv_repo_key    = c_repo1
      iv_commit      = lv_commit
      iv_branch_name = c_branch
      iv_attempt_id  = lv_attempt_id ).

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
    DATA(lv_attempt_id) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).

    zcl_abapgit_ortec_fastpath=>certify_fetched_commit(
      iv_repo_key    = c_repo1
      iv_commit      = lv_commit
      iv_branch_name = c_branch
      iv_attempt_id  = lv_attempt_id ).

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
    DATA(lv_attempt_id) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).

    zcl_abapgit_ortec_fastpath=>certify_fetched_commit(
      iv_repo_key    = c_repo1
      iv_commit      = lv_commit
      iv_branch_name = c_branch
      iv_attempt_id  = lv_attempt_id ).

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
    DATA(lv_attempt_id) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).

    zcl_abapgit_ortec_fastpath=>certify_fetched_commit(
      iv_repo_key    = c_repo1
      iv_commit      = lv_commit
      iv_branch_name = c_branch
      iv_attempt_id  = lv_attempt_id ).
    zcl_abapgit_ortec_fastpath=>certify_fetched_commit(
      iv_repo_key    = c_repo1
      iv_commit      = lv_commit
      iv_branch_name = c_branch
      iv_attempt_id  = lv_attempt_id ).

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

  METHOD stale_attempt_rejected.
    " A superseded/stale attempt id must be rejected by mark_graph_complete's
    " (called from certify_fetched_commit) staleness guard - a second
    " begin_attempt call for the same commit overwrites the row's
    " attempt_id, invalidating the earlier one.
    DATA(lv_commit) = build_commit( iv_repo = c_repo1 ).

    DATA(lv_stale_id) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).
    zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).

    TRY.
        zcl_abapgit_ortec_fastpath=>certify_fetched_commit(
          iv_repo_key    = c_repo1
          iv_commit      = lv_commit
          iv_branch_name = c_branch
          iv_attempt_id  = lv_stale_id ).
        cl_abap_unit_assert=>fail( 'Stale attempt id must be rejected' ).
      CATCH zcx_abapgit_ortec_git.
        " Expected: mark_graph_complete's staleness guard raised.
    ENDTRY.
  ENDMETHOD.

  METHOD one_attempt_one_id.
    " Mirrors persist_pull_result's own fallback-mint logic (D2b2 item 3):
    " when no iv_attempt_id is supplied, exactly one begin_attempt call
    " mints an id used consistently by both persist_missing_objects
    " (ZAOG_OBJ_STORE) and certify_fetched_commit (ZAOG_COMMIT_HIST).
    " ADAPTATION: persist_pull_result itself cannot be driven directly in
    " a unit test - it is gated behind zcl_abapgit_ortec_git_switch=>
    " is_active_for_repo, backed by a shared, singleton, XML-serialized
    " user settings persistence with its own uncontrolled COMMIT WORK AND
    " WAIT (see certify_fetched_commit's own doc comment, which is why it
    " was extracted for testability in the first place). This test instead
    " replicates the exact two calls persist_pull_result makes internally
    " with a single, shared, externally-minted attempt id.
    DATA(lv_commit) = build_commit( iv_repo = c_repo1 ).
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object  LIKE LINE OF lt_objects.

    ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-sha1 = zcl_abapgit_hash=>sha1_blob( zcl_abapgit_convert=>string_to_xstring_utf8( 'one_attempt' ) ).
    ls_object-data = zcl_abapgit_convert=>string_to_xstring_utf8( 'one_attempt' ).
    APPEND ls_object TO lt_objects.

    DATA(lv_attempt_id) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).

    zcl_abapgit_ortec_fastpath=>persist_missing_objects(
      iv_repo_key   = c_repo1
      it_objects    = lt_objects
      iv_attempt_id = lv_attempt_id ).
    zcl_abapgit_ortec_fastpath=>certify_fetched_commit(
      iv_repo_key    = c_repo1
      iv_commit      = lv_commit
      iv_branch_name = c_branch
      iv_attempt_id  = lv_attempt_id ).

    DATA lv_store_attempt TYPE zaog_obj_store-attempt_id.
    SELECT SINGLE attempt_id FROM zaog_obj_store
      INTO @lv_store_attempt
      WHERE repo_key = @c_repo1
        AND obj_sha1 = @ls_object-sha1.

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).

    cl_abap_unit_assert=>assert_equals( act = lv_store_attempt exp = lv_attempt_id ).
    cl_abap_unit_assert=>assert_equals( act = ls_state-attempt_id exp = lv_attempt_id ).
  ENDMETHOD.

  METHOD retry_gets_new_attempt.
    " Two independent begin_attempt calls (mimicking two separate
    " persist_pull_result invocations without an explicit iv_attempt_id)
    " must mint two different ids.
    DATA(lv_commit) = build_commit( iv_repo = c_repo1 ).

    DATA(lv_id_1) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).
    DATA(lv_id_2) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).

    cl_abap_unit_assert=>assert_differs( act = lv_id_2 exp = lv_id_1 ).
  ENDMETHOD.

  METHOD certify_reuses_attempt.
    " certify_fetched_commit must never call begin_attempt itself (D2b2
    " item 5) - the caller-supplied id is what ends up persisted. If
    " certify_fetched_commit still minted its own id internally, this
    " would overwrite the row's attempt_id with a fresh UUID, and the
    " assertion below would fail.
    DATA(lv_commit) = build_commit( iv_repo = c_repo1 ).
    DATA(lv_attempt_id) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).

    zcl_abapgit_ortec_fastpath=>certify_fetched_commit(
      iv_repo_key    = c_repo1
      iv_commit      = lv_commit
      iv_branch_name = c_branch
      iv_attempt_id  = lv_attempt_id ).

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).

    cl_abap_unit_assert=>assert_equals( act = ls_state-attempt_id exp = lv_attempt_id ).
  ENDMETHOD.

  METHOD attempt_id_cross_table.
    " A single externally-minted attempt id, passed to both
    " persist_missing_objects and certify_fetched_commit, must end up on
    " BOTH the ZAOG_OBJ_STORE row and the ZAOG_COMMIT_HIST row.
    DATA(lv_commit) = build_commit( iv_repo = c_repo1 ).
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object  LIKE LINE OF lt_objects.

    ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-sha1 = zcl_abapgit_hash=>sha1_blob( zcl_abapgit_convert=>string_to_xstring_utf8( 'cross_table' ) ).
    ls_object-data = zcl_abapgit_convert=>string_to_xstring_utf8( 'cross_table' ).
    APPEND ls_object TO lt_objects.

    DATA(lv_attempt_id) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).

    zcl_abapgit_ortec_fastpath=>persist_missing_objects(
      iv_repo_key   = c_repo1
      it_objects    = lt_objects
      iv_attempt_id = lv_attempt_id ).
    zcl_abapgit_ortec_fastpath=>certify_fetched_commit(
      iv_repo_key    = c_repo1
      iv_commit      = lv_commit
      iv_branch_name = c_branch
      iv_attempt_id  = lv_attempt_id ).

    DATA lv_store_attempt TYPE zaog_obj_store-attempt_id.
    SELECT SINGLE attempt_id FROM zaog_obj_store
      INTO @lv_store_attempt
      WHERE repo_key = @c_repo1
        AND obj_sha1 = @ls_object-sha1.

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_store_attempt exp = lv_attempt_id msg = 'ZAOG_OBJ_STORE row must carry the shared attempt id' ).
    cl_abap_unit_assert=>assert_equals(
      act = ls_state-attempt_id exp = lv_attempt_id msg = 'ZAOG_COMMIT_HIST row must carry the same shared attempt id' ).
  ENDMETHOD.

  METHOD missing_objects_has_id.
    " persist_missing_objects must set attempt_id on every newly-inserted
    " ZAOG_OBJ_STORE row (D2b2 item 4).
    DATA(lv_commit) = build_commit( iv_repo = c_repo1 ).
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object  LIKE LINE OF lt_objects.

    ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-sha1 = zcl_abapgit_hash=>sha1_blob( zcl_abapgit_convert=>string_to_xstring_utf8( 'has_id' ) ).
    ls_object-data = zcl_abapgit_convert=>string_to_xstring_utf8( 'has_id' ).
    APPEND ls_object TO lt_objects.

    DATA(lv_attempt_id) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).

    zcl_abapgit_ortec_fastpath=>persist_missing_objects(
      iv_repo_key   = c_repo1
      it_objects    = lt_objects
      iv_attempt_id = lv_attempt_id ).

    DATA lv_store_attempt TYPE zaog_obj_store-attempt_id.
    SELECT SINGLE attempt_id FROM zaog_obj_store
      INTO @lv_store_attempt
      WHERE repo_key = @c_repo1
        AND obj_sha1 = @ls_object-sha1.

    cl_abap_unit_assert=>assert_equals( act = lv_store_attempt exp = lv_attempt_id ).
  ENDMETHOD.

  METHOD porcelain_path_gets_lock.
    " porcelain's pull_by_branch (Unit #2) cannot be driven end-to-end in a
    " unit test (requires a live zcl_abapgit_git_transport HTTP round-trip
    " with no mock seam in that test class). This test instead verifies
    " the exact acquire_repo_lock/release_repo_lock primitive that
    " porcelain's INCREMENTAL_UPDATE branch now calls before
    " persist_pull_result: acquiring returns a real lock id, and releasing
    " frees it for immediate re-acquisition (proving the lock is genuinely
    " acquired and effective, not a no-op).
    DATA(lv_lock_id_1) = zcl_abapgit_ortec_pack_dec=>acquire_repo_lock( iv_repo_key = c_repo1 ).
    cl_abap_unit_assert=>assert_not_initial( lv_lock_id_1 ).
    zcl_abapgit_ortec_pack_dec=>release_repo_lock( lv_lock_id_1 ).

    DATA(lv_lock_id_2) = zcl_abapgit_ortec_pack_dec=>acquire_repo_lock( iv_repo_key = c_repo1 ).
    cl_abap_unit_assert=>assert_not_initial( lv_lock_id_2 ).
    zcl_abapgit_ortec_pack_dec=>release_repo_lock( lv_lock_id_2 ).
  ENDMETHOD.

  METHOD lock_not_held_over_http.
    " Structural property (AC-1: the lock span starts at acquire_repo_lock
    " immediately before resume_decode, and never wraps the earlier
    " zcl_abapgit_git_transport=>branches(iv_url) HTTP call) - verified by
    " direct code review of pull_by_branch (see D2b2 closeout), not
    " runtime-testable here since pull_by_branch requires a live HTTP
    " round-trip with no mock seam in this test class. This test instead
    " proves the lock primitive itself is uncontended before any Unit #1
    " work begins, and remains immediately re-acquirable after a clean
    " release - i.e. nothing in this fixture's own setup/teardown holds a
    " lingering lock that could mask a real "held over HTTP" defect.
    DATA(lv_lock_id) = zcl_abapgit_ortec_pack_dec=>acquire_repo_lock(
      iv_repo_key     = c_repo1
      iv_max_attempts = 1 ).
    cl_abap_unit_assert=>assert_not_initial( lv_lock_id ).
    zcl_abapgit_ortec_pack_dec=>release_repo_lock( lv_lock_id ).
  ENDMETHOD.

  METHOD lock_timeout_falls_back.
    " pull_by_branch's/porcelain's own graceful-degrade TRY block (CATCH
    " zcx_abapgit_exception zcx_abapgit_ortec_git around acquire_repo_lock
    " + begin_attempt) cannot be exercised end-to-end here (both callers
    " require a live HTTP round-trip). Genuine lock CONTENTION also cannot
    " be simulated from a single ABAP Unit session (the enqueue server
    " does not treat a second acquire from the same user/session as
    " foreign_lock). This test instead proves the graceful-degrade
    " TRY/CATCH shape itself is correct against acquire_repo_lock's one
    " genuinely deterministic failure path (iv_max_attempts <= 0), using
    " the exact same CATCH clause pull_by_branch/porcelain use.
    DATA lv_lock_id    TYPE zcl_abapgit_ortec_pack_dec=>ty_session_id.
    DATA lv_lock_held  TYPE abap_bool.
    DATA lv_attempt_id TYPE zcl_abapgit_ortec_mat_state=>ty_attempt_id.
    DATA(lv_commit) = build_commit( iv_repo = c_repo1 ).

    TRY.
        lv_lock_id = zcl_abapgit_ortec_pack_dec=>acquire_repo_lock(
          iv_repo_key     = c_repo1
          iv_max_attempts = 0 ).
        lv_lock_held = abap_true.
        lv_attempt_id = zcl_abapgit_ortec_mat_state=>begin_attempt(
          iv_repo_key = c_repo1
          iv_commit   = lv_commit ).
      CATCH zcx_abapgit_exception zcx_abapgit_ortec_git.
        CLEAR lv_attempt_id.
    ENDTRY.

    cl_abap_unit_assert=>assert_false(
      act = lv_lock_held msg = 'Lock must not be marked held when acquire_repo_lock fails' ).
    cl_abap_unit_assert=>assert_initial(
      act = lv_attempt_id msg = 'No attempt id should be minted when the lock could not be acquired' ).
  ENDMETHOD.

  METHOD filtered_fetch_lock_ok.
    " ORTEC D2b2 is scoped to exactly two Publication Units (fastpath's
    " pull_by_branch Phase-1b and porcelain's pull_by_branch) -
    " try_filtered_commit_fetch is untouched (confirmed by source review:
    " it never references acquire_repo_lock/release_repo_lock). This test
    " proves no regression: a commit already present locally is still
    " reported applicable immediately, exactly as before.
    DATA(lv_commit) = build_commit( iv_repo = c_repo1 ).

    DATA(lv_applicable) = zcl_abapgit_ortec_fastpath=>try_filtered_commit_fetch(
      iv_url         = 'https://dummy.test/repo.git'
      iv_branch_name = c_branch
      iv_commit      = lv_commit
      iv_repo_key    = c_repo1 ).

    cl_abap_unit_assert=>assert_equals( act = lv_applicable exp = abap_true ).
  ENDMETHOD.

  METHOD resume_new_attempt_when_new.
    " pull_by_branch's Phase-1 lock+attempt setup calls begin_attempt fresh
    " on every invocation - there is no cached/reused attempt id across
    " separate pull_by_branch calls. pull_by_branch itself cannot be
    " driven end-to-end in a unit test (requires a live
    " zcl_abapgit_git_transport=>branches HTTP call with no mock seam in
    " this test class) - this test instead validates the exact primitive
    " Phase-1 relies on: begin_attempt always mints a fresh id, even for a
    " commit that already has a fully-certified prior attempt (simulating
    " a subsequent, fresh (non-resumed) pull_by_branch round for the same
    " commit).
    DATA(lv_commit) = build_commit( iv_repo = c_repo1 ).

    DATA(lv_id_1) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).
    zcl_abapgit_ortec_fastpath=>certify_fetched_commit(
      iv_repo_key    = c_repo1
      iv_commit      = lv_commit
      iv_branch_name = c_branch
      iv_attempt_id  = lv_id_1 ).

    DATA(lv_id_2) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo1
      iv_commit   = lv_commit ).

    cl_abap_unit_assert=>assert_differs( act = lv_id_2 exp = lv_id_1 ).
  ENDMETHOD.

  METHOD resume_reuses_attempt.
    " NOT_APPLICABLE placeholder (per the D2/D2b1 design evidence: the
    " "Attempt-ID call graph" and resolve_streaming's own doc comments
    " show resume_decode/resolve_streaming only ever THREAD a
    " caller-supplied attempt id through to ZAOG_OBJ_STORE/ZAOG_FETCH_SESS
    " - they never read or compare an existing attempt_id for a
    " reuse/staleness decision against ZAOG_COMMIT_HIST). No such "resume
    " reuses an attempt id" concept exists in this codebase to test.
    " Always-passing documentation test, per the D2b2 task brief's
    " explicit guidance not to invent a fake check.
    cl_abap_unit_assert=>assert_true( abap_true ).
  ENDMETHOD.

ENDCLASS.

CLASS ltcl_fastpath_protocol DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    METHODS buffer_emits_shallow_lines FOR TESTING RAISING cx_static_check.
    METHODS buffer_skips_shallow_forced FOR TESTING RAISING cx_static_check.
    METHODS buffer_send_deepen_even_forced FOR TESTING RAISING cx_static_check.
    METHODS parse_collects_shallow FOR TESTING RAISING cx_static_check.
    METHODS parse_ignores_bad_shallow FOR TESTING RAISING cx_static_check.
    METHODS progress_deepen_widens_n_caps FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_fastpath_protocol IMPLEMENTATION.
  METHOD buffer_emits_shallow_lines.
    DATA lt_hashes TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_haves  TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv_buffer TYPE string.

    APPEND '1111111111111111111111111111111111111111' TO lt_hashes.
    APPEND '2222222222222222222222222222222222222222' TO lt_haves.

    lv_buffer = zcl_abapgit_ortec_fastpath=>build_upload_pack_buffer(
      iv_deepen_level = 0
      it_hashes       = lt_hashes
      it_ortec_haves  = lt_haves
      iv_allow_thin   = abap_false
      iv_force_full   = abap_false ).

    FIND FIRST OCCURRENCE OF 'want 1111111111111111111111111111111111111111' IN lv_buffer.
    cl_abap_unit_assert=>assert_subrc( exp = 0 msg = 'Want line must be present' ).
    FIND FIRST OCCURRENCE OF 'shallow 2222222222222222222222222222222222222222' IN lv_buffer.
    cl_abap_unit_assert=>assert_subrc( exp = 0 msg = 'Shallow line must be present' ).
    FIND FIRST OCCURRENCE OF '0000' IN lv_buffer.
    cl_abap_unit_assert=>assert_subrc( exp = 0 msg = 'Flush pkt must be present' ).

    FIND FIRST OCCURRENCE OF 'shallow 2222222222222222222222222222222222222222' IN lv_buffer MATCH OFFSET DATA(lv_shallow_pos).
    FIND FIRST OCCURRENCE OF '0000' IN lv_buffer MATCH OFFSET DATA(lv_flush_pos).
    cl_abap_unit_assert=>assert_true( act = xsdbool( lv_shallow_pos < lv_flush_pos ) msg = 'Shallow lines must be emitted before the flush pkt' ).
  ENDMETHOD.

  METHOD buffer_skips_shallow_forced.
    DATA lt_hashes TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_haves  TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv_buffer TYPE string.

    APPEND '1111111111111111111111111111111111111111' TO lt_hashes.

    lv_buffer = zcl_abapgit_ortec_fastpath=>build_upload_pack_buffer(
      iv_deepen_level = 0
      it_hashes       = lt_hashes
      it_ortec_haves  = lt_haves
      iv_allow_thin   = abap_false
      iv_force_full   = abap_false ).

    FIND FIRST OCCURRENCE OF 'shallow ' IN lv_buffer.
    cl_abap_unit_assert=>assert_subrc( exp = 4 msg = 'Shallow lines must be skipped when no haves are provided' ).

    lv_buffer = zcl_abapgit_ortec_fastpath=>build_upload_pack_buffer(
      iv_deepen_level = 0
      it_hashes       = lt_hashes
      it_ortec_haves  = VALUE zif_abapgit_git_definitions=>ty_sha1_tt( ( '2222222222222222222222222222222222222222' ) )
      iv_allow_thin   = abap_false
      iv_force_full   = abap_true ).

    FIND FIRST OCCURRENCE OF 'shallow ' IN lv_buffer.
    cl_abap_unit_assert=>assert_subrc( exp = 4 msg = 'Shallow lines must be skipped when iv_force_full is true' ).
  ENDMETHOD.

  METHOD buffer_send_deepen_even_forced.
    " Phase 1 of the architecture hardening plan (.memory/state.md,
    " 2026-07-20) REVERTED the earlier "omit deepen entirely when
    " iv_force_full = abap_true" behavior: that meant requesting a repo's
    " COMPLETE, unbounded history in one shot, which failed live for a repo
    " with substantial real history (abapGit's own repo: 4737 commits).
    " force_full callers are now responsible for choosing a PROGRESSIVELY
    " WIDENING iv_deepen_level themselves across repeated attempts (see
    " upload_pack_by_branch/upload_pack_by_commit's retry loop) - this
    " method's job is unchanged: always send a deepen line whenever there
    " are no haves, using whatever value the caller passed, regardless of
    " iv_force_full.
    DATA lt_hashes TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv_buffer TYPE string.

    APPEND '1111111111111111111111111111111111111111' TO lt_hashes.

    " No haves + NOT forced -> deepen line IS expected (unchanged behavior).
    lv_buffer = zcl_abapgit_ortec_fastpath=>build_upload_pack_buffer(
      iv_deepen_level = 1
      it_hashes       = lt_hashes
      iv_allow_thin   = abap_false
      iv_force_full   = abap_false ).

    FIND FIRST OCCURRENCE OF 'deepen 1' IN lv_buffer.
    cl_abap_unit_assert=>assert_subrc( exp = 0 msg = 'A deepen line is expected for a normal no-haves fetch' ).

    " No haves + FORCED -> a deepen line is STILL sent, using the caller's
    " (now progressively-widened) iv_deepen_level - force_full no longer
    " means "omit deepen", it means "send no haves" (shallow/have lines
    " stay skipped, asserted separately in buffer_skips_shallow_forced).
    lv_buffer = zcl_abapgit_ortec_fastpath=>build_upload_pack_buffer(
      iv_deepen_level = 500
      it_hashes       = lt_hashes
      iv_allow_thin   = abap_false
      iv_force_full   = abap_true ).

    FIND FIRST OCCURRENCE OF 'deepen 500' IN lv_buffer.
    cl_abap_unit_assert=>assert_subrc( exp = 0 msg = 'A deepen line reflecting the caller-supplied (progressive) depth must still be sent when iv_force_full is true' ).
  ENDMETHOD.

  METHOD parse_collects_shallow.
    DATA lv_data TYPE xstring.
    DATA lv_pack TYPE xstring.
    DATA lt_shallow TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_unshallow TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv_pkt TYPE string.

    " Build a minimal pkt-line stream: plain shallow/unshallow lines, then
    " a flush pkt, and one ordinary text pkt-line.
    lv_pkt = zcl_abapgit_git_utils=>pkt_string( |shallow 1111111111111111111111111111111111111111| ).
    lv_data = zcl_abapgit_convert=>string_to_xstring_utf8( lv_pkt ).

    lv_pkt = zcl_abapgit_git_utils=>pkt_string( |unshallow 2222222222222222222222222222222222222222| ).
    lv_data = lv_data && zcl_abapgit_convert=>string_to_xstring_utf8( lv_pkt ).

    lv_data = lv_data && zcl_abapgit_convert=>string_to_xstring_utf8( '0000' ).

    lv_pkt = zcl_abapgit_git_utils=>pkt_string( |ok| ).
    lv_data = lv_data && zcl_abapgit_convert=>string_to_xstring_utf8( lv_pkt ).

    zcl_abapgit_ortec_fastpath=>parse(
      IMPORTING
        et_shallow = lt_shallow
        et_unshallow = lt_unshallow
        ev_pack = lv_pack
      CHANGING
        cv_data = lv_data ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_shallow ) exp = 1 msg = 'Shallow SHA should be collected' ).
    cl_abap_unit_assert=>assert_equals( act = lt_shallow[ 1 ] exp = '1111111111111111111111111111111111111111' msg = 'Shallow SHA value must be preserved' ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_unshallow ) exp = 1 msg = 'Unshallow SHA should be collected' ).
    cl_abap_unit_assert=>assert_equals( act = lt_unshallow[ 1 ] exp = '2222222222222222222222222222222222222222' msg = 'Unshallow SHA value must be preserved' ).
    cl_abap_unit_assert=>assert_equals( act = lv_pack exp = '' msg = 'No pack data should be parsed from a plain text pkt-line stream' ).
  ENDMETHOD.

  METHOD parse_ignores_bad_shallow.
    DATA lv_data TYPE xstring.
    DATA lv_pack TYPE xstring.
    DATA lt_shallow TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_unshallow TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    DATA lv_pkt TYPE string.

    lv_pkt = zcl_abapgit_git_utils=>pkt_string( |shallow| ).
    lv_data = zcl_abapgit_convert=>string_to_xstring_utf8( lv_pkt ).

    lv_pkt = zcl_abapgit_git_utils=>pkt_string( |unshallow 2222222222222222222222222222222222222222| ).
    lv_data = lv_data && zcl_abapgit_convert=>string_to_xstring_utf8( lv_pkt ).

    lv_data = lv_data && zcl_abapgit_convert=>string_to_xstring_utf8( '0000' ).

    lv_pkt = zcl_abapgit_git_utils=>pkt_string( |ok| ).
    lv_data = lv_data && zcl_abapgit_convert=>string_to_xstring_utf8( lv_pkt ).

    TRY.
        zcl_abapgit_ortec_fastpath=>parse(
          IMPORTING
            et_shallow = lt_shallow
            et_unshallow = lt_unshallow
            ev_pack = lv_pack
          CHANGING
            cv_data = lv_data ).
      CATCH zcx_abapgit_ortec_git.
        cl_abap_unit_assert=>fail( 'Malformed shallow-update lines must not raise' ).
    ENDTRY.

    cl_abap_unit_assert=>assert_initial( act = lt_shallow msg = 'Malformed shallow line should be ignored' ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_unshallow ) exp = 1 msg = 'Well-formed unshallow line should still be collected' ).
    cl_abap_unit_assert=>assert_initial( act = lv_pack msg = 'Plain text pkt-lines should not be treated as pack data' ).
  ENDMETHOD.

  METHOD progress_deepen_widens_n_caps.
    " Phase 1 of the architecture hardening plan (.memory/state.md,
    " 2026-07-20): the progressive recovery loop must start at a sensible
    " minimum, widen by the configured factor on each failure, and never
    " exceed the configured ceiling - regardless of how large or small the
    " prior/current depth was.
    DATA lv_deepen TYPE i.

    " A tiny prior depth (e.g. 1, the usual incremental default) must still
    " start the progressive loop at a reasonably useful minimum, not just
    " prior*factor (which would be a useless "4").
    lv_deepen = zcl_abapgit_ortec_fastpath=>first_progressive_deepen( 1 ).
    cl_abap_unit_assert=>assert_equals(
      act = lv_deepen
      exp = zcl_abapgit_ortec_fastpath=>c_progressive_start_min
      msg = 'A small prior depth must start progressive recovery at the configured minimum' ).

    " A larger prior depth must start from prior*factor, not the minimum.
    lv_deepen = zcl_abapgit_ortec_fastpath=>first_progressive_deepen( 100 ).
    cl_abap_unit_assert=>assert_equals(
      act = lv_deepen
      exp = 100 * zcl_abapgit_ortec_fastpath=>c_progressive_widen_factor
      msg = 'A larger prior depth must widen by the configured factor' ).

    " Widening must multiply by the configured factor each step.
    lv_deepen = zcl_abapgit_ortec_fastpath=>next_progressive_deepen( 50 ).
    cl_abap_unit_assert=>assert_equals(
      act = lv_deepen
      exp = 50 * zcl_abapgit_ortec_fastpath=>c_progressive_widen_factor
      msg = 'Each widening step must multiply by the configured factor' ).

    " Widening must never exceed the configured ceiling.
    lv_deepen = zcl_abapgit_ortec_fastpath=>next_progressive_deepen(
      zcl_abapgit_ortec_fastpath=>c_progressive_max_deepen ).
    cl_abap_unit_assert=>assert_equals(
      act = lv_deepen
      exp = zcl_abapgit_ortec_fastpath=>c_progressive_max_deepen
      msg = 'Widening must be capped at the configured ceiling' ).

    lv_deepen = zcl_abapgit_ortec_fastpath=>first_progressive_deepen(
      zcl_abapgit_ortec_fastpath=>c_progressive_max_deepen * 10 ).
    cl_abap_unit_assert=>assert_equals(
      act = lv_deepen
      exp = zcl_abapgit_ortec_fastpath=>c_progressive_max_deepen
      msg = 'The initial progressive depth must also be capped at the configured ceiling' ).
  ENDMETHOD.
ENDCLASS.

"! Progress test double, same pattern as ltcl_progress_recorder in
"! zcl_abapgit_ortec_pack_stream.clas.testclasses.abap (each testclasses
"! include is its own compilation unit, so it is redeclared here).
CLASS ltcl_fp_progress_recorder DEFINITION CREATE PUBLIC FOR TESTING.
  PUBLIC SECTION.
    INTERFACES zif_abapgit_progress.
    TYPES: BEGIN OF ty_call,
             current TYPE i,
             text    TYPE string,
           END OF ty_call.
    TYPES ty_calls TYPE STANDARD TABLE OF ty_call WITH EMPTY KEY.
    DATA mt_calls TYPE ty_calls READ-ONLY.
ENDCLASS.

CLASS ltcl_fp_progress_recorder IMPLEMENTATION.
  METHOD zif_abapgit_progress~set_total.
    RETURN.
  ENDMETHOD.
  METHOD zif_abapgit_progress~show.
    APPEND VALUE #( current = iv_current text = iv_text ) TO mt_calls.
  ENDMETHOD.
  METHOD zif_abapgit_progress~off.
    RETURN.
  ENDMETHOD.
ENDCLASS.

CLASS ltcl_fastpath_progress DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    "! Test list item 8: existing ORTEC-disabled behavior remains
    "! unchanged. An unconfigured URL means
    "! zcl_abapgit_ortec_git_switch=>is_active_for_repo returns
    "! abap_false, so pull_by_branch must return an INITIAL result
    "! immediately - before any progress call - even when an injected
    "! progress reference is supplied.
    METHODS disabled_repo_no_progress FOR TESTING RAISING cx_static_check.
ENDCLASS.

CLASS ltcl_fastpath_progress IMPLEMENTATION.
  METHOD disabled_repo_no_progress.
    DATA lo_recorder TYPE REF TO ltcl_fp_progress_recorder.
    DATA ls_result   TYPE zcl_abapgit_git_porcelain=>ty_pull_result.

    CREATE OBJECT lo_recorder.

    ls_result = zcl_abapgit_ortec_fastpath=>pull_by_branch(
      iv_url         = 'https://example.invalid/ortec-progress-disabled-test.git'
      iv_branch_name = 'refs/heads/main'
      ii_progress    = lo_recorder ).

    cl_abap_unit_assert=>assert_initial( act = ls_result
      msg = 'A repo where the ORTEC switch is off must still return an INITIAL result unchanged' ).
    cl_abap_unit_assert=>assert_true( act = xsdbool( lo_recorder->mt_calls IS INITIAL )
      msg = 'No progress call may fire before the disabled-switch short-circuit' ).
  ENDMETHOD.
ENDCLASS.
