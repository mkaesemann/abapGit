CLASS zcl_abapgit_ortec_cold_init DEFINITION LOCAL FRIENDS ltcl_cold_init.

CLASS ltcl_cold_init DEFINITION FINAL
  FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.

  PRIVATE SECTION.
    CONSTANTS c_tip     TYPE zif_abapgit_git_definitions=>ty_sha1     VALUE 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'.
    CONSTANTS mc_repo   TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key VALUE 'ZAOG_CI_TEST'.
    CONSTANTS mc_branch TYPE string                                   VALUE 'refs/heads/main'.

    CLASS-DATA gi_environment TYPE REF TO if_osql_test_environment.

    CLASS-METHODS class_setup.
    CLASS-METHODS class_teardown.

    METHODS setup.
    METHODS teardown.

    METHODS empty_repo_key_raises        FOR TESTING RAISING cx_static_check.
    METHODS empty_tip_commit_raises      FOR TESTING RAISING cx_static_check.
    METHODS graph_response_ceiling_value FOR TESTING RAISING cx_static_check.

    " B3 mandatory tests (12, 25): end-to-end, no HTTP double available -
    " both rely on the all-present shortcut making HTTP genuinely
    " unreachable code for these fixtures (a real HTTP attempt against the
    " bogus URL used here would raise and fail the test).
    METHODS all_present_needs_no_http    FOR TESTING RAISING cx_static_check.
    METHODS snapshot_idempotent          FOR TESTING RAISING cx_static_check.

    " B3 mandatory tests (13, 14): pure, HTTP-free chunking.
    METHODS chunk_missing_dedups         FOR TESTING RAISING cx_static_check.
    METHODS chunk_missing_batch_limit    FOR TESTING RAISING cx_static_check.

    " B3 mandatory tests (15, 16): pure, HTTP-free oversize decision.
    METHODS oversize_action_byte_limit   FOR TESTING RAISING cx_static_check.
    METHODS oversize_action_repeatable   FOR TESTING RAISING cx_static_check.

    " B3 mandatory tests (20, 21, 22, 23): bounded, HTTP-free per-batch
    " verification against DB fixtures simulating decode_streaming output.
    METHODS verify_batch_missing_raises  FOR TESTING RAISING cx_static_check.
    METHODS verify_batch_extra_ignored   FOR TESTING RAISING cx_static_check.
    METHODS verify_batch_wrong_type      FOR TESTING RAISING cx_static_check.
    METHODS verify_batch_absent_content  FOR TESTING RAISING cx_static_check.

    " B3 mandatory test (24): pure publication gate.
    METHODS may_publish_false_missing    FOR TESTING RAISING cx_static_check.

    METHODS build_tip_fixture
      EXPORTING ev_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1
                ev_blob_sha   TYPE zif_abapgit_git_definitions=>ty_sha1.

ENDCLASS.


CLASS ltcl_cold_init IMPLEMENTATION.
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
    " zaog_obj_store is real DB (not doubled, matching ltcl_obj_store's own
    " pattern) - materialize_tip_snapshot issues its own COMMIT WORK, so a
    " bare DELETE + ROLLBACK WORK would NOT actually clean up a previously
    " committed row (see user memory: "DELETE + ROLLBACK WORK only cleans
    " up correctly when the SUT never issues its own COMMIT WORK") -
    " ROLLBACK first (discard anything uncommitted), then DELETE, then
    " COMMIT the cleanup itself.
    ROLLBACK WORK.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    COMMIT WORK.
    zcl_abapgit_ortec_obj_store=>invalidate_cache( ).
  ENDMETHOD.

  METHOD teardown.
    ROLLBACK WORK.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    COMMIT WORK.
    zcl_abapgit_ortec_obj_store=>invalidate_cache( ).
  ENDMETHOD.

  METHOD build_tip_fixture.
    DATA lt_nodes       TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA lv_blob_data   TYPE xstring.
    DATA ls_node        LIKE LINE OF lt_nodes.
    DATA lv_tree_data   TYPE xstring.
    DATA lv_tree_sha    TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA ls_commit      TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lv_commit_data TYPE xstring.

    TRY.
        lv_blob_data = '48656C6C6F'.
        ev_blob_sha = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).

        ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
        ls_node-name  = 'hello.txt'.
        ls_node-sha1  = ev_blob_sha.
        APPEND ls_node TO lt_nodes.

        lv_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
        lv_tree_sha = zcl_abapgit_hash=>sha1_tree( lv_tree_data ).

        ls_commit-tree      = lv_tree_sha.
        ls_commit-author    = 'Test <test@example.com> 0 +0000'.
        ls_commit-committer = 'Test <test@example.com> 0 +0000'.
        ls_commit-body      = 'materialize fixture'.
        lv_commit_data = zcl_abapgit_git_pack=>encode_commit( ls_commit ).
        ev_commit_sha = zcl_abapgit_hash=>sha1_commit( lv_commit_data ).

        zcl_abapgit_ortec_obj_store=>store_object(
            iv_repo_key = mc_repo
            iv_sha1     = ev_commit_sha
            iv_type     = zif_abapgit_git_definitions=>c_type-commit
            iv_data     = lv_commit_data ).
        zcl_abapgit_ortec_obj_store=>store_object(
            iv_repo_key = mc_repo
            iv_sha1     = lv_tree_sha
            iv_type     = zif_abapgit_git_definitions=>c_type-tree
            iv_data     = lv_tree_data ).
        zcl_abapgit_ortec_obj_store=>store_object(
            iv_repo_key = mc_repo
            iv_sha1     = ev_blob_sha
            iv_type     = zif_abapgit_git_definitions=>c_type-blob
            iv_data     = lv_blob_data ).

      CATCH zcx_abapgit_exception INTO DATA(lox_git_error).
        cl_abap_unit_assert=>fail( |Fixture build failed: { lox_git_error->get_text( ) }| ).
      CATCH zcx_abapgit_ortec_git INTO DATA(lox_ortec_error).
        cl_abap_unit_assert=>fail( |Fixture build failed: { lox_ortec_error->get_text( ) }| ).
    ENDTRY.
  ENDMETHOD.

  METHOD empty_repo_key_raises.
    " Both guard clauses run BEFORE any HTTP call is made - this codebase
    " has no HTTP client injection point (zcl_abapgit_ortec_fastpath's
    " own upload_pack_by_commit/upload_pack_by_branch are, for the same
    " reason, likewise only unit tested up to build_request/parse in
    " isolation, never end to end) - so only the pre-HTTP guard clauses of
    " acquire_blobless_graph are directly unit-testable here.
    TRY.
        zcl_abapgit_ortec_cold_init=>acquire_blobless_graph(
            iv_url        = 'https://example.com/x.git'
            iv_repo_key   = ''
            iv_tip_commit = c_tip ).
        cl_abap_unit_assert=>fail( 'Empty repo key must raise' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.

  METHOD empty_tip_commit_raises.
    TRY.
        zcl_abapgit_ortec_cold_init=>acquire_blobless_graph(
            iv_url        = 'https://example.com/x.git'
            iv_repo_key   = 'ZAOG_TEST_01'
            iv_tip_commit = '' ).
        cl_abap_unit_assert=>fail( 'Empty tip commit must raise' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.

  METHOD graph_response_ceiling_value.
    " Pins INV-B-12 (.memory/logs/variant_b_package_b_design.md §10): the
    " memory-risk gate ceiling for a single blobless-graph HTTP response.
    cl_abap_unit_assert=>assert_equals(
        exp = 209715200
        act = zcl_abapgit_ortec_cold_init=>c_max_graph_response_bytes
        msg = '200 MiB memory-risk ceiling must not silently drift' ).
  ENDMETHOD.

  METHOD all_present_needs_no_http.
    DATA lv_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    " TODO: variable is assigned but never used (ABAP cleaner)
    DATA lv_blob_sha   TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_attempt_id TYPE zcl_abapgit_ortec_mat_state=>ty_attempt_id.
    DATA ls_state      TYPE zcl_abapgit_ortec_mat_state=>ty_state.

    build_tip_fixture(
      IMPORTING
        ev_commit_sha = lv_commit_sha
        ev_blob_sha   = lv_blob_sha ).

    lv_attempt_id = zcl_abapgit_ortec_mat_state=>begin_attempt(
                        iv_repo_key = mc_repo
                        iv_commit   = lv_commit_sha ).
    zcl_abapgit_ortec_mat_state=>mark_graph_complete(
        iv_repo_key   = mc_repo
        iv_commit     = lv_commit_sha
        iv_attempt_id = lv_attempt_id ).

    " A garbage, unresolvable URL - if materialize_tip_snapshot attempted
    " any HTTP call at all, this would raise and fail the test.
    zcl_abapgit_ortec_cold_init=>materialize_tip_snapshot(
        iv_url         = 'https://example.invalid/x.git'
        iv_repo_key    = mc_repo
        iv_branch_name = mc_branch
        iv_tip_commit  = lv_commit_sha ).

    ls_state = zcl_abapgit_ortec_mat_state=>get_state(
                   iv_repo_key = mc_repo
                   iv_commit   = lv_commit_sha ).
    cl_abap_unit_assert=>assert_equals(
        exp = zcl_abapgit_ortec_mat_state=>cs_snap_state-complete
        act = ls_state-snap_state
        msg = 'All-present tip publishes SNAPSHOT_COMPLETE with zero HTTP calls' ).
  ENDMETHOD.

  METHOD snapshot_idempotent.
    DATA lv_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    " TODO: variable is assigned but never used (ABAP cleaner)
    DATA lv_blob_sha   TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_attempt_id TYPE zcl_abapgit_ortec_mat_state=>ty_attempt_id.
    DATA ls_state      TYPE zcl_abapgit_ortec_mat_state=>ty_state.

    build_tip_fixture(
      IMPORTING
        ev_commit_sha = lv_commit_sha
        ev_blob_sha   = lv_blob_sha ).

    lv_attempt_id = zcl_abapgit_ortec_mat_state=>begin_attempt(
                        iv_repo_key = mc_repo
                        iv_commit   = lv_commit_sha ).
    zcl_abapgit_ortec_mat_state=>mark_graph_complete(
        iv_repo_key   = mc_repo
        iv_commit     = lv_commit_sha
        iv_attempt_id = lv_attempt_id ).

    " Two consecutive calls, both against an all-present tip - the second
    " call generates its own fresh BEGIN_ATTEMPT internally and must
    " succeed identically, never erroring on "stale attempt" against
    " itself.
    zcl_abapgit_ortec_cold_init=>materialize_tip_snapshot(
        iv_url         = 'https://example.invalid/x.git'
        iv_repo_key    = mc_repo
        iv_branch_name = mc_branch
        iv_tip_commit  = lv_commit_sha ).
    zcl_abapgit_ortec_cold_init=>materialize_tip_snapshot(
        iv_url         = 'https://example.invalid/x.git'
        iv_repo_key    = mc_repo
        iv_branch_name = mc_branch
        iv_tip_commit  = lv_commit_sha ).

    ls_state = zcl_abapgit_ortec_mat_state=>get_state(
                   iv_repo_key = mc_repo
                   iv_commit   = lv_commit_sha ).
    cl_abap_unit_assert=>assert_equals(
        exp = zcl_abapgit_ortec_mat_state=>cs_snap_state-complete
        act = ls_state-snap_state
        msg = 'A second materialize call against an already-complete snapshot is idempotent' ).
  ENDMETHOD.

  METHOD chunk_missing_dedups.
    DATA lt_missing TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_batches TYPE zcl_abapgit_ortec_cold_init=>ty_sha1_batch_tt.

    APPEND 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' TO lt_missing.
    APPEND 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' TO lt_missing.
    APPEND 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' TO lt_missing.

    lt_batches = zcl_abapgit_ortec_cold_init=>chunk_missing_sha1s( lt_missing ).

    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( lt_batches ) ).
    cl_abap_unit_assert=>assert_equals(
        exp = 2
        act = lines( lt_batches[ 1 ] )
        msg = 'A duplicated SHA1 is chunked once' ).
  ENDMETHOD.

  METHOD chunk_missing_batch_limit.
    DATA lv_index   TYPE i.
    DATA lv_sha1    TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lt_missing TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_batches TYPE zcl_abapgit_ortec_cold_init=>ty_sha1_batch_tt.

    DO 101 TIMES.
      lv_index = sy-index.
      lv_sha1 = |{ lv_index WIDTH = 40 ALIGN = RIGHT PAD = '0' }|.
      APPEND lv_sha1 TO lt_missing.
    ENDDO.

    lt_batches = zcl_abapgit_ortec_cold_init=>chunk_missing_sha1s( lt_missing ).

    cl_abap_unit_assert=>assert_equals(
        exp = 2
        act = lines( lt_batches )
        msg = '101 unique SHA1s split into a full 100-batch plus a 1-entry remainder' ).
    cl_abap_unit_assert=>assert_equals(
        exp = zcl_abapgit_ortec_fetch_req=>c_materialize_batch_max
        act = lines( lt_batches[ 1 ] ) ).
    cl_abap_unit_assert=>assert_equals(
        exp = 1
        act = lines( lt_batches[ 2 ] ) ).
  ENDMETHOD.

  METHOD oversize_action_byte_limit.
    DATA lv_action TYPE zcl_abapgit_ortec_cold_init=>ty_oversize_action.

    lv_action = zcl_abapgit_ortec_cold_init=>decide_oversize_action(
                    iv_response_bytes = zcl_abapgit_ortec_cold_init=>c_max_batch_response_bytes
                    iv_batch_size     = 2
                    iv_splits_used    = 0 ).
    cl_abap_unit_assert=>assert_equals(
        exp = zcl_abapgit_ortec_cold_init=>cs_oversize_action-none
        act = lv_action
        msg = 'A response exactly at the ceiling is accepted' ).

    lv_action = zcl_abapgit_ortec_cold_init=>decide_oversize_action(
                    iv_response_bytes = zcl_abapgit_ortec_cold_init=>c_max_batch_response_bytes + 1
                    iv_batch_size     = 2
                    iv_splits_used    = 0 ).
    cl_abap_unit_assert=>assert_equals(
        exp = zcl_abapgit_ortec_cold_init=>cs_oversize_action-split
        act = lv_action
        msg = 'An oversized response with a splittable batch is split' ).

    lv_action = zcl_abapgit_ortec_cold_init=>decide_oversize_action(
                    iv_response_bytes = zcl_abapgit_ortec_cold_init=>c_max_batch_response_bytes + 1
                    iv_batch_size     = 1
                    iv_splits_used    = 0 ).
    cl_abap_unit_assert=>assert_equals(
        exp = zcl_abapgit_ortec_cold_init=>cs_oversize_action-raise
        act = lv_action
        msg = 'An oversized single-SHA1 batch cannot be split further' ).

    lv_action = zcl_abapgit_ortec_cold_init=>decide_oversize_action(
                    iv_response_bytes = zcl_abapgit_ortec_cold_init=>c_max_batch_response_bytes + 1
                    iv_batch_size     = 2
                    iv_splits_used    = zcl_abapgit_ortec_cold_init=>c_max_oversize_splits ).
    cl_abap_unit_assert=>assert_equals(
        exp = zcl_abapgit_ortec_cold_init=>cs_oversize_action-raise
        act = lv_action
        msg = 'An exhausted split budget cannot be split further' ).
  ENDMETHOD.

  METHOD oversize_action_repeatable.
    DATA lv_first  TYPE zcl_abapgit_ortec_cold_init=>ty_oversize_action.
    DATA lv_second TYPE zcl_abapgit_ortec_cold_init=>ty_oversize_action.

    lv_first = zcl_abapgit_ortec_cold_init=>decide_oversize_action(
                   iv_response_bytes = zcl_abapgit_ortec_cold_init=>c_max_batch_response_bytes + 1
                   iv_batch_size     = 4
                   iv_splits_used    = 1 ).
    lv_second = zcl_abapgit_ortec_cold_init=>decide_oversize_action(
                    iv_response_bytes = zcl_abapgit_ortec_cold_init=>c_max_batch_response_bytes + 1
                    iv_batch_size     = 4
                    iv_splits_used    = 1 ).

    cl_abap_unit_assert=>assert_equals(
        exp = lv_second
        act = lv_first
        msg = 'Identical inputs deterministically produce the identical decision' ).
  ENDMETHOD.

  METHOD verify_batch_missing_raises.
    DATA lt_batch TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    APPEND 'cccccccccccccccccccccccccccccccccccccccc' TO lt_batch.

    TRY.
        zcl_abapgit_ortec_cold_init=>verify_batch_objects(
            iv_repo_key = mc_repo
            it_batch    = lt_batch ).
        cl_abap_unit_assert=>fail( 'A requested SHA1 never persisted must raise' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.

  METHOD verify_batch_extra_ignored.
    DATA lv_blob_data  TYPE xstring.
    DATA lv_blob_sha   TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_extra_data TYPE xstring.
    DATA lv_extra_sha  TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lt_batch      TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    lv_blob_data = '48656C6C6F'.
    lv_blob_sha = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).
    lv_extra_data = '576F726C64'.
    lv_extra_sha = zcl_abapgit_hash=>sha1_blob( lv_extra_data ).

    zcl_abapgit_ortec_obj_store=>store_object(
        iv_repo_key = mc_repo
        iv_sha1     = lv_blob_sha
        iv_type     = zif_abapgit_git_definitions=>c_type-blob
        iv_data     = lv_blob_data ).
    " Extra, unrequested object - must not affect verification of the
    " actually-requested batch (design §5 policy: harmless/ignored).
    zcl_abapgit_ortec_obj_store=>store_object(
        iv_repo_key = mc_repo
        iv_sha1     = lv_extra_sha
        iv_type     = zif_abapgit_git_definitions=>c_type-blob
        iv_data     = lv_extra_data ).

    APPEND lv_blob_sha TO lt_batch.

    zcl_abapgit_ortec_cold_init=>verify_batch_objects(
        iv_repo_key = mc_repo
        it_batch    = lt_batch ).
  ENDMETHOD.

  METHOD verify_batch_wrong_type.
    DATA lv_data  TYPE xstring.
    DATA lv_sha1  TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lt_batch TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    lv_data = '48656C6C6F'.
    lv_sha1 = zcl_abapgit_hash=>sha1_blob( lv_data ).

    " Stored under the correct content-derived SHA1 but as the wrong type -
    " a defensive DB-integrity guard, since a genuine type/content mismatch
    " under the SAME SHA1 would otherwise require a real hash collision.
    zcl_abapgit_ortec_obj_store=>store_object(
        iv_repo_key = mc_repo
        iv_sha1     = lv_sha1
        iv_type     = zif_abapgit_git_definitions=>c_type-tree
        iv_data     = lv_data ).

    APPEND lv_sha1 TO lt_batch.

    TRY.
        zcl_abapgit_ortec_cold_init=>verify_batch_objects(
            iv_repo_key = mc_repo
            it_batch    = lt_batch ).
        cl_abap_unit_assert=>fail( 'A wrong-type stored object must raise' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.

  METHOD verify_batch_absent_content.
    " "SHA-invalid returned content" and "missing requested object"
    " collapse to the identical mechanism here: Git's content-addressed
    " SHA1 means content that does not match the requested SHA1 simply
    " never appears under that key after decode - both are proven by the
    " same GET_OBJECTS-not-found raise as verify_batch_missing_raises.

    DATA lt_batch TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    APPEND 'dddddddddddddddddddddddddddddddddddddddd' TO lt_batch.

    TRY.
        zcl_abapgit_ortec_cold_init=>verify_batch_objects(
            iv_repo_key = mc_repo
            it_batch    = lt_batch ).
        cl_abap_unit_assert=>fail( 'Content that never produces the wanted SHA1 must raise' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.

  METHOD may_publish_false_missing.
    DATA lt_still_missing TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    APPEND 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' TO lt_still_missing.

    DATA(lv_yes) = zcl_abapgit_ortec_cold_init=>may_publish_snapshot( lt_still_missing ).

    cl_abap_unit_assert=>assert_false(
        act = lv_yes
        msg = 'A non-empty still-missing set must never gate publication' ).
  ENDMETHOD.
ENDCLASS.
