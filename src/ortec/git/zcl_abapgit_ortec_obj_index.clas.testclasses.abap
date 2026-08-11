CLASS zcl_abapgit_ortec_obj_index DEFINITION LOCAL FRIENDS ltcl_obj_index.
CLASS ltcl_obj_index DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_OBJIDX'.
    METHODS setup. METHODS teardown.
    METHODS marker_required_for_ready FOR TESTING RAISING cx_static_check.

    " Package E E1-TEST (design doc §1, INV-E1-T-1/2): CONFIRMED_CURRENT
    " regression coverage - no defect found, these pin the existing correct
    " behavior of the commit-scoped index and its readiness marker.
    METHODS build_commit
      IMPORTING
        iv_filename          TYPE string
        iv_content            TYPE xstring
      RETURNING VALUE(rv_commit_sha) TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING   zcx_abapgit_exception
                zcx_abapgit_ortec_git.

    METHODS index_no_cross_commit_leak FOR TESTING RAISING cx_static_check.
    METHODS ready_rejects_other_commit FOR TESTING RAISING cx_static_check.
    METHODS ready_accepts_exact_commit FOR TESTING RAISING cx_static_check.
    METHODS index_chunk_boundary_ok    FOR TESTING RAISING cx_static_check.

    " E1-PERF-A (design doc §2, run-brief test matrix; revised to the
    " 30000-row batch size). build_bulk_commit is a dedicated fixture
    " helper (kept separate from build_commit/index_chunk_boundary_ok to
    " avoid touching already-validated checkpoint-1 test code).
    "
    " KNOWN, DOCUMENTED LOCAL COVERAGE LIMITATION: the active chunk
    " boundary is now 30000 rows. Building 30000+ complete Git objects in
    " an ABAP Unit DURATION SHORT test is not appropriate (excessive
    " runtime cost for no correctness benefit over the proven chunking
    " ALGORITHM). The named constant is PRIVATE with no LOCAL FRIENDS
    " declared, so it is also not legally pinnable from these tests
    " without a scope-exceeding production change. Local tests below
    " therefore prove bulk multi-row correctness on the single
    " final-flush path (below the active boundary) plus the unchanged
    " zero-row path; the actual in-loop multi-chunk flush behavior AT
    " 30000 rows is verified only via the owner's live IT8 SAT/SQL
    " measurement (ACTUAL_MODIFY_PACKAGE_COUNT), not locally.
    METHODS build_bulk_commit
      IMPORTING
        iv_file_count TYPE i
      EXPORTING
        ev_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1
        et_filter     TYPE zif_abapgit_definitions=>ty_tadir_tt
      RAISING
        zcx_abapgit_exception
        zcx_abapgit_ortec_git.

    METHODS index_bulk_rows_preserved FOR TESTING RAISING cx_static_check.
    METHODS index_empty_no_match      FOR TESTING RAISING cx_static_check.

    " AR-1-01/AR-2-01 (design doc §3.0/§3.0b/§9 Slice 1b/1d): positive-row
    " and readiness-marker context isolation regression coverage.
    METHODS ready_rejects_different_context   FOR TESTING RAISING cx_static_check.
    METHODS select_rows_excludes_other_context FOR TESTING RAISING cx_static_check.
    METHODS blank_legacy_context_is_never_ready FOR TESTING RAISING cx_static_check.
    METHODS partial_rows_context_disjoint     FOR TESTING RAISING cx_static_check.
    METHODS select_partial_rows_chunk_boundary FOR TESTING RAISING cx_static_check.

    " OBJ-PERF-IMPL-B (design doc §11 step 3 sub-steps 1-2, Slice 2):
    " ensure_filtered_coverage warm-fast-path/fallback regression coverage.
    METHODS warm_coverage_skips_rewalk FOR TESTING RAISING cx_static_check.
    METHODS incomplete_coverage_falls_through_to_rebuild FOR TESTING RAISING cx_static_check.

    " OBJ-PERF-IMPL-C (design doc §11 step 4, §4.1, §5, §13 W4/W5/W6/W8,
    " Slice 3): walk_filtered/invalidate_commit_index/backoff/current-remote
    " regression coverage. LOCAL FRIENDS above grants access to these two
    " PRIVATE class methods for direct testing.
    METHODS filtered_walk_writes_only_requested_objects FOR TESTING RAISING cx_static_check.
    METHODS filtered_walk_never_sets_ready_marker FOR TESTING RAISING cx_static_check.
    METHODS filtered_walk_idempotent_on_overlap FOR TESTING RAISING cx_static_check.
    METHODS filtered_walk_writes_context_hash_as_key FOR TESTING RAISING cx_static_check.
    METHODS filtered_walk_no_cross_context_overwrite FOR TESTING RAISING cx_static_check.
    METHODS retry_purge_removes_all_three_tables FOR TESTING RAISING cx_static_check.
    METHODS missing_tree_writes_m_row_then_reraises FOR TESTING RAISING cx_static_check.
    METHODS repeat_request_within_backoff_skips_walk FOR TESTING RAISING cx_static_check.
    METHODS repeat_request_after_backoff_retries_walk FOR TESTING RAISING cx_static_check.
    METHODS not_present_remote_requires_current_remote_commit FOR TESTING RAISING cx_static_check.
    METHODS not_present_remote_requires_current_remote_supplied FOR TESTING RAISING cx_static_check.
    METHODS select_rows_chunk_boundary FOR TESTING RAISING cx_static_check.

    METHODS build_commit_two_objects
      RETURNING VALUE(rv_commit_sha) TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING   zcx_abapgit_exception
                zcx_abapgit_ortec_git.
ENDCLASS.
CLASS ltcl_obj_index IMPLEMENTATION.
  METHOD setup.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    DELETE FROM zaog_obj_index WHERE repo_key = mc_repo.
    DELETE FROM zaog_obj_pidx WHERE repo_key = mc_repo.
    DELETE FROM zaog_obj_cover WHERE repo_key = mc_repo.
    DELETE FROM zaog_commit_hist WHERE repo_key = mc_repo.
  ENDMETHOD.
  METHOD teardown.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    DELETE FROM zaog_obj_index WHERE repo_key = mc_repo.
    DELETE FROM zaog_obj_pidx WHERE repo_key = mc_repo.
    DELETE FROM zaog_obj_cover WHERE repo_key = mc_repo.
    DELETE FROM zaog_commit_hist WHERE repo_key = mc_repo.
    ROLLBACK WORK.
  ENDMETHOD.
  METHOD marker_required_for_ready.
    DATA lt_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node LIKE LINE OF lt_nodes.
    DATA ls_commit TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lv_blob_data TYPE xstring.
    DATA lv_blob_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_src_tree_data TYPE xstring.
    DATA lv_src_tree_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_root_tree_data TYPE xstring.
    DATA lv_root_tree_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_commit_data TYPE xstring.
    DATA lv_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lo_dot TYPE REF TO zcl_abapgit_dot_abapgit.
    DATA lo_filter TYPE REF TO zcl_abapgit_object_filter_obj.
    DATA lt_files TYPE zif_abapgit_git_definitions=>ty_files_tt.
    DATA ls_file LIKE LINE OF lt_files.

    " Build a minimal commit -> /src/ tree -> zprogram.prog.abap blob graph,
    " matching the exact filename/path convention already proven by
    " zcl_abapgit_filename_logic's own unit tests (PROG/ZPROGRAM at /src/).
    lv_blob_data = '48656C6C6F'.
    lv_blob_sha = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).

    CLEAR ls_node.
    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_node-name  = 'zprogram.prog.abap'.
    ls_node-sha1  = lv_blob_sha.
    CLEAR lt_nodes.
    APPEND ls_node TO lt_nodes.
    lv_src_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    lv_src_tree_sha = zcl_abapgit_hash=>sha1_tree( lv_src_tree_data ).

    CLEAR ls_node.
    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-dir.
    ls_node-name  = 'src'.
    ls_node-sha1  = lv_src_tree_sha.
    CLEAR lt_nodes.
    APPEND ls_node TO lt_nodes.
    lv_root_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    lv_root_tree_sha = zcl_abapgit_hash=>sha1_tree( lv_root_tree_data ).

    ls_commit-tree = lv_root_tree_sha.
    ls_commit-author = 'Test <test@example.com> 0 +0000'.
    ls_commit-committer = 'Test <test@example.com> 0 +0000'.
    ls_commit-body = 'test'.
    lv_commit_data = zcl_abapgit_git_pack=>encode_commit( ls_commit ).
    lv_commit_sha = zcl_abapgit_hash=>sha1_commit( lv_commit_data ).

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_commit_sha
      iv_type = zif_abapgit_git_definitions=>c_type-commit iv_data = lv_commit_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_root_tree_sha
      iv_type = zif_abapgit_git_definitions=>c_type-tree iv_data = lv_root_tree_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_src_tree_sha
      iv_type = zif_abapgit_git_definitions=>c_type-tree iv_data = lv_src_tree_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_blob_sha
      iv_type = zif_abapgit_git_definitions=>c_type-blob iv_data = lv_blob_data ).

    lo_dot = zcl_abapgit_dot_abapgit=>build_default( ).
    lo_filter = NEW #( it_filter = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) ) ).

    " First build: proves the index is built correctly and (per the fix) the
    " completion marker is written even though rows were found - previously
    " the marker was only written when the walk found ZERO relevant rows.
    lt_files = zcl_abapgit_ortec_obj_index=>get_files_for_filter(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_sha
      ii_obj_filter = lo_filter
      io_dot        = lo_dot
      iv_devclass   = '$PACK' ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_files ) exp = 1
      msg = 'The filtered PROG/ZPROGRAM file must be resolved' ).
    READ TABLE lt_files INTO ls_file INDEX 1.
    cl_abap_unit_assert=>assert_equals( act = ls_file-path exp = '/src/'
      msg = 'File must be resolved from the freshly-built index' ).

    SELECT SINGLE path_hash FROM zaog_obj_index INTO @DATA(lv_marker_hash)
      WHERE repo_key = @mc_repo AND commit_sha1 = @lv_commit_sha
        AND obj_type = '$IDX' AND obj_name = '__READY__' AND idx_status = 'R'.
    cl_abap_unit_assert=>assert_true(
      act = xsdbool( lv_marker_hash IS NOT INITIAL )
      msg = 'The completion marker must be written even when rows were found' ).

    " Simulate an index left behind by a rebuild interrupted AFTER this row
    " was written but BEFORE the completion marker: drop the marker and
    " corrupt the row's path so a stale reuse becomes observable.
    UPDATE zaog_obj_index SET file_path = '/WRONG/'
      WHERE repo_key = mc_repo AND commit_sha1 = lv_commit_sha
        AND obj_type = 'PROG' AND obj_name = 'ZPROGRAM'.
    DELETE FROM zaog_obj_index
      WHERE repo_key = mc_repo AND commit_sha1 = lv_commit_sha
        AND obj_type = '$IDX' AND obj_name = '__READY__'.

    CLEAR lt_files.
    lt_files = zcl_abapgit_ortec_obj_index=>get_files_for_filter(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_sha
      ii_obj_filter = lo_filter
      io_dot        = lo_dot
      iv_devclass   = '$PACK' ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_files ) exp = 1
      msg = 'The filtered file must still be resolved after self-heal' ).
    READ TABLE lt_files INTO ls_file INDEX 1.
    cl_abap_unit_assert=>assert_equals( act = ls_file-path exp = '/src/'
      msg = 'STRICT mode must detect the missing marker and rebuild from ' &&
            'the real stored objects instead of trusting the stale row' ).
  ENDMETHOD.

  METHOD build_commit.
    " Shared fixture builder for E1-TEST: commit -> /src/ tree -> single
    " zprogram.prog.abap blob (PROG/ZPROGRAM naming convention), matching
    " marker_required_for_ready's own proven graph shape.
    DATA lt_nodes           TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node            LIKE LINE OF lt_nodes.
    DATA ls_commit          TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lv_blob_sha        TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_src_tree_data   TYPE xstring.
    DATA lv_src_tree_sha    TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_root_tree_data  TYPE xstring.
    DATA lv_root_tree_sha   TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_commit_data     TYPE xstring.

    lv_blob_sha = zcl_abapgit_hash=>sha1_blob( iv_content ).

    CLEAR ls_node.
    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_node-name  = iv_filename.
    ls_node-sha1  = lv_blob_sha.
    CLEAR lt_nodes.
    APPEND ls_node TO lt_nodes.
    lv_src_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    lv_src_tree_sha = zcl_abapgit_hash=>sha1_tree( lv_src_tree_data ).

    CLEAR ls_node.
    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-dir.
    ls_node-name  = 'src'.
    ls_node-sha1  = lv_src_tree_sha.
    CLEAR lt_nodes.
    APPEND ls_node TO lt_nodes.
    lv_root_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    lv_root_tree_sha = zcl_abapgit_hash=>sha1_tree( lv_root_tree_data ).

    ls_commit-tree = lv_root_tree_sha.
    ls_commit-author = 'Test <test@example.com> 0 +0000'.
    ls_commit-committer = 'Test <test@example.com> 0 +0000'.
    " Vary the commit body by content sha1 so two commits with the same
    " tree shape but different blob content never collide on commit sha1.
    ls_commit-body = |test { lv_blob_sha }|.
    lv_commit_data = zcl_abapgit_git_pack=>encode_commit( ls_commit ).
    rv_commit_sha = zcl_abapgit_hash=>sha1_commit( lv_commit_data ).

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = rv_commit_sha
      iv_type = zif_abapgit_git_definitions=>c_type-commit iv_data = lv_commit_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_root_tree_sha
      iv_type = zif_abapgit_git_definitions=>c_type-tree iv_data = lv_root_tree_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_src_tree_sha
      iv_type = zif_abapgit_git_definitions=>c_type-tree iv_data = lv_src_tree_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_blob_sha
      iv_type = zif_abapgit_git_definitions=>c_type-blob iv_data = iv_content ).
  ENDMETHOD.

  METHOD index_no_cross_commit_leak.
    " E1-T-01 (design §1, INV-E1-T-1): rebuild_index's composite key already
    " includes commit_sha1 by design - this proves the RUNTIME behavior
    " actually respects it: building the index for a SECOND, different
    " commit under the SAME repo key must not corrupt or leak into the
    " FIRST commit's already-built index.
    DATA(lv_commit_1) = build_commit( iv_filename = 'zprogram.prog.abap' iv_content = '48656C6C6F' ).
    DATA(lv_commit_2) = build_commit( iv_filename = 'zprogram.prog.abap' iv_content = '576F726C64' ).

    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lo_filter) = NEW zcl_abapgit_object_filter_obj( it_filter = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) ) ).

    DATA(lt_files_1) = zcl_abapgit_ortec_obj_index=>get_files_for_filter(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_1
      ii_obj_filter = lo_filter
      io_dot        = lo_dot
      iv_devclass   = '$PACK' ).
    DATA(lt_files_2) = zcl_abapgit_ortec_obj_index=>get_files_for_filter(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_2
      ii_obj_filter = lo_filter
      io_dot        = lo_dot
      iv_devclass   = '$PACK' ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_files_1 ) exp = 1
      msg = 'Commit 1 must resolve its own file' ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_files_2 ) exp = 1
      msg = 'Commit 2 must resolve its own file' ).
    cl_abap_unit_assert=>assert_differs(
      act = lt_files_2[ 1 ]-sha1
      exp = lt_files_1[ 1 ]-sha1
      msg = 'The second commit''s index must resolve its OWN blob sha1, not the first commit''s' ).

    " Re-querying the FIRST commit again after the second commit's index
    " was built must still return the first commit's own (unchanged)
    " content - no cross-commit corruption of the persistent index rows.
    DATA(lt_files_1_again) = zcl_abapgit_ortec_obj_index=>get_files_for_filter(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_1
      ii_obj_filter = lo_filter
      io_dot        = lo_dot
      iv_devclass   = '$PACK' ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_files_1_again ) exp = 1 ).
    cl_abap_unit_assert=>assert_equals(
      act = lt_files_1_again[ 1 ]-sha1
      exp = lt_files_1[ 1 ]-sha1
      msg = 'Commit 1''s content must be unaffected by commit 2''s later index build' ).

    SELECT COUNT(*) FROM zaog_obj_index INTO @DATA(lv_count)
      WHERE repo_key = @mc_repo AND commit_sha1 = @lv_commit_1
        AND obj_type = 'PROG' AND obj_name = 'ZPROGRAM'.
    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 1
      msg = 'Building the index for a second commit must not duplicate or ' &&
            'corrupt the first commit''s own index rows' ).
  ENDMETHOD.

  METHOD ready_rejects_other_commit.
    " E1-T-02 (design §1, INV-E1-T-2): is_index_ready must not report ready
    " for a commit that was never indexed, even when its repo_key's OTHER
    " commit is fully built and ready.
    DATA(lv_commit_1) = build_commit( iv_filename = 'zprogram.prog.abap' iv_content = '48656C6C6F' ).
    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lo_filter) = NEW zcl_abapgit_object_filter_obj( it_filter = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) ) ).

    zcl_abapgit_ortec_obj_index=>get_files_for_filter(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_1
      ii_obj_filter = lo_filter
      io_dot        = lo_dot
      iv_devclass   = '$PACK' ).

    DATA(lv_ctx_ready_1) = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot ).

    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_index=>is_index_ready(
        iv_repo_key = mc_repo iv_commit = lv_commit_1 iv_context_hash = lv_ctx_ready_1 )
      msg = 'Sanity: the built commit must be ready' ).

    cl_abap_unit_assert=>assert_false(
      act = zcl_abapgit_ortec_obj_index=>is_index_ready(
        iv_repo_key = mc_repo
        iv_commit   = '9999999999999999999999999999999999999999'
        iv_context_hash = lv_ctx_ready_1 )
      msg = 'is_index_ready must not report ready for a commit that was ' &&
            'never indexed, even though its repo_key''s OTHER commit is ' &&
            'fully built' ).
  ENDMETHOD.

  METHOD ready_accepts_exact_commit.
    " E1-T-03 (design §1, INV-E1-T-2): after building TWO different commits'
    " indices under the same repo key, is_index_ready for EACH exact commit
    " must independently return true (proving exactness, not merely "the
    " only one built is ready").
    DATA(lv_commit_1) = build_commit( iv_filename = 'zprogram.prog.abap' iv_content = '48656C6C6F' ).
    DATA(lv_commit_2) = build_commit( iv_filename = 'zprogram.prog.abap' iv_content = '576F726C64' ).
    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lo_filter) = NEW zcl_abapgit_object_filter_obj( it_filter = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) ) ).

    zcl_abapgit_ortec_obj_index=>get_files_for_filter(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_1
      ii_obj_filter = lo_filter
      io_dot        = lo_dot
      iv_devclass   = '$PACK' ).
    zcl_abapgit_ortec_obj_index=>get_files_for_filter(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_2
      ii_obj_filter = lo_filter
      io_dot        = lo_dot
      iv_devclass   = '$PACK' ).

    DATA(lv_ctx_ready_2) = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot ).

    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_index=>is_index_ready(
        iv_repo_key = mc_repo iv_commit = lv_commit_1 iv_context_hash = lv_ctx_ready_2 )
      msg = 'Commit 1''s own exact index must be reported ready' ).
    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_index=>is_index_ready(
        iv_repo_key = mc_repo iv_commit = lv_commit_2 iv_context_hash = lv_ctx_ready_2 )
      msg = 'Commit 2''s own exact index must ALSO be reported ready, independently of commit 1' ).
  ENDMETHOD.

  METHOD index_chunk_boundary_ok.
    " E1-T-04 (design §1, §10 outcome-preservation row "E1 correctness"):
    " rebuild_index's bulk MODIFY chunks at a named constant
    " (c_index_write_chunk_size, currently 30000 after the E1-PERF-A
    " revision). This fixture deliberately does NOT hardcode that literal
    " anywhere, so it stays valid across future E1-PERF batch-size
    " changes. NOTE: at the current 30000-row batch size this fixture's
    " 1200 rows no longer cross the active in-loop chunk boundary (it now
    " only exercises the single final-flush path) - it is retained as
    " generic multi-row/marker regression coverage, not as boundary
    " coverage; see index_bulk_rows_preserved's comment for why the
    " active boundary is not locally crossable.
    CONSTANTS lc_file_count TYPE i VALUE 1200.

    DATA lt_nodes          TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node           LIKE LINE OF lt_nodes.
    DATA lt_objects        TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object         LIKE LINE OF lt_objects.
    DATA lt_filter         TYPE zif_abapgit_definitions=>ty_tadir_tt.
    DATA ls_filter         LIKE LINE OF lt_filter.
    DATA ls_commit         TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lv_src_tree_data  TYPE xstring.
    DATA lv_src_tree_sha   TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_root_tree_data TYPE xstring.
    DATA lv_root_tree_sha  TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_commit_data    TYPE xstring.
    DATA lv_commit_sha     TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_name           TYPE string.
    DATA lv_obj_name       TYPE string.
    DATA lv_blob_data      TYPE xstring.
    DATA lv_blob_sha       TYPE zif_abapgit_git_definitions=>ty_sha1.

    DO lc_file_count TIMES.
      lv_name     = |zprogram{ sy-index WIDTH = 4 ALIGN = RIGHT PAD = '0' }|.
      lv_obj_name = |ZPROGRAM{ sy-index WIDTH = 4 ALIGN = RIGHT PAD = '0' }|.
      lv_blob_data = zcl_abapgit_convert=>string_to_xstring_utf8( |content { sy-index }| ).
      lv_blob_sha = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).

      CLEAR ls_object.
      ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
      ls_object-sha1 = lv_blob_sha.
      ls_object-data = lv_blob_data.
      APPEND ls_object TO lt_objects.

      CLEAR ls_node.
      ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
      ls_node-name  = |{ lv_name }.prog.abap|.
      ls_node-sha1  = lv_blob_sha.
      APPEND ls_node TO lt_nodes.

      CLEAR ls_filter.
      ls_filter-object   = 'PROG'.
      ls_filter-obj_name = lv_obj_name.
      APPEND ls_filter TO lt_filter.
    ENDDO.

    lv_src_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    lv_src_tree_sha = zcl_abapgit_hash=>sha1_tree( lv_src_tree_data ).

    CLEAR lt_nodes.
    CLEAR ls_node.
    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-dir.
    ls_node-name  = 'src'.
    ls_node-sha1  = lv_src_tree_sha.
    APPEND ls_node TO lt_nodes.
    lv_root_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    lv_root_tree_sha = zcl_abapgit_hash=>sha1_tree( lv_root_tree_data ).

    ls_commit-tree = lv_root_tree_sha.
    ls_commit-author = 'Test <test@example.com> 0 +0000'.
    ls_commit-committer = 'Test <test@example.com> 0 +0000'.
    ls_commit-body = 'chunk boundary test'.
    lv_commit_data = zcl_abapgit_git_pack=>encode_commit( ls_commit ).
    lv_commit_sha = zcl_abapgit_hash=>sha1_commit( lv_commit_data ).

    CLEAR ls_object.
    ls_object-type = zif_abapgit_git_definitions=>c_type-commit.
    ls_object-sha1 = lv_commit_sha.
    ls_object-data = lv_commit_data.
    APPEND ls_object TO lt_objects.

    ls_object-type = zif_abapgit_git_definitions=>c_type-tree.
    ls_object-sha1 = lv_root_tree_sha.
    ls_object-data = lv_root_tree_data.
    APPEND ls_object TO lt_objects.

    ls_object-sha1 = lv_src_tree_sha.
    ls_object-data = lv_src_tree_data.
    APPEND ls_object TO lt_objects.

    " Bulk-store all 1200 blobs + tree/commit objects in one call, per the
    " project's own documented per-row-DB-loop performance lesson (never
    " loop store_object for a large fixture).
    zcl_abapgit_ortec_obj_store=>store_objects( iv_repo_key = mc_repo it_objects = lt_objects ).

    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lo_filter) = NEW zcl_abapgit_object_filter_obj( it_filter = lt_filter ).

    DATA(lt_files) = zcl_abapgit_ortec_obj_index=>get_files_for_filter(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_sha
      ii_obj_filter = lo_filter
      io_dot        = lo_dot
      iv_devclass   = '$PACK' ).

    cl_abap_unit_assert=>assert_equals(
      act = lines( lt_files )
      exp = lc_file_count
      msg = 'All files must be indexed across the chunk boundary, not just the first chunk' ).

    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_index=>is_index_ready(
        iv_repo_key = mc_repo iv_commit = lv_commit_sha
        iv_context_hash = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot ) )
      msg = 'Completion marker must be written after a multi-chunk rebuild' ).
  ENDMETHOD.

  METHOD build_bulk_commit.
    " Shared fixture for E1-PERF-A boundary tests: iv_file_count files under
    " /src/, each independently PROG/ZBULKnnnnnn-mapped, bulk-stored in one
    " store_objects call (never a per-row DB loop), per the project's own
    " documented per-row-DB-loop performance lesson.
    DATA lt_nodes          TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node           LIKE LINE OF lt_nodes.
    DATA lt_objects        TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object         LIKE LINE OF lt_objects.
    DATA ls_filter         LIKE LINE OF et_filter.
    DATA ls_commit         TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lv_src_tree_data  TYPE xstring.
    DATA lv_src_tree_sha   TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_root_tree_data TYPE xstring.
    DATA lv_root_tree_sha  TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_commit_data    TYPE xstring.
    DATA lv_name           TYPE string.
    DATA lv_obj_name       TYPE string.
    DATA lv_blob_data      TYPE xstring.
    DATA lv_blob_sha       TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_index          TYPE i.

    CLEAR et_filter.
    CLEAR lt_nodes.
    CLEAR lt_objects.

    DO iv_file_count TIMES.
      lv_index    = sy-index.
      lv_name     = |zbulk{ lv_index WIDTH = 6 ALIGN = RIGHT PAD = '0' }|.
      lv_obj_name = |ZBULK{ lv_index WIDTH = 6 ALIGN = RIGHT PAD = '0' }|.
      lv_blob_data = zcl_abapgit_convert=>string_to_xstring_utf8( |content { lv_index }| ).
      lv_blob_sha = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).

      CLEAR ls_object.
      ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
      ls_object-sha1 = lv_blob_sha.
      ls_object-data = lv_blob_data.
      APPEND ls_object TO lt_objects.

      CLEAR ls_node.
      ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
      ls_node-name  = |{ lv_name }.prog.abap|.
      ls_node-sha1  = lv_blob_sha.
      APPEND ls_node TO lt_nodes.

      CLEAR ls_filter.
      ls_filter-object   = 'PROG'.
      ls_filter-obj_name = lv_obj_name.
      APPEND ls_filter TO et_filter.
    ENDDO.

    lv_src_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    lv_src_tree_sha = zcl_abapgit_hash=>sha1_tree( lv_src_tree_data ).

    CLEAR lt_nodes.
    CLEAR ls_node.
    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-dir.
    ls_node-name  = 'src'.
    ls_node-sha1  = lv_src_tree_sha.
    APPEND ls_node TO lt_nodes.
    lv_root_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    lv_root_tree_sha = zcl_abapgit_hash=>sha1_tree( lv_root_tree_data ).

    ls_commit-tree = lv_root_tree_sha.
    ls_commit-author = 'Test <test@example.com> 0 +0000'.
    ls_commit-committer = 'Test <test@example.com> 0 +0000'.
    ls_commit-body = |bulk { iv_file_count }|.
    lv_commit_data = zcl_abapgit_git_pack=>encode_commit( ls_commit ).
    ev_commit_sha = zcl_abapgit_hash=>sha1_commit( lv_commit_data ).

    CLEAR ls_object.
    ls_object-type = zif_abapgit_git_definitions=>c_type-commit.
    ls_object-sha1 = ev_commit_sha.
    ls_object-data = lv_commit_data.
    APPEND ls_object TO lt_objects.

    ls_object-type = zif_abapgit_git_definitions=>c_type-tree.
    ls_object-sha1 = lv_root_tree_sha.
    ls_object-data = lv_root_tree_data.
    APPEND ls_object TO lt_objects.

    ls_object-sha1 = lv_src_tree_sha.
    ls_object-data = lv_src_tree_data.
    APPEND ls_object TO lt_objects.

    zcl_abapgit_ortec_obj_store=>store_objects( iv_repo_key = mc_repo it_objects = lt_objects ).
  ENDMETHOD.

  METHOD index_bulk_rows_preserved.
    " E1-PERF-A (revised to 30000): proves bulk multi-row correctness on
    " the single final-flush path (5000 rows, well below the active
    " 30000-row in-loop chunk boundary - see the class-local coverage
    " limitation comment above build_bulk_commit for why the boundary
    " itself is not locally crossable). All rows must survive, the
    " completion marker must be written, and no row may be duplicated or
    " dropped.
    DATA lv_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lt_filter TYPE zif_abapgit_definitions=>ty_tadir_tt.

    build_bulk_commit(
      EXPORTING iv_file_count = 5000
      IMPORTING ev_commit_sha = lv_commit_sha et_filter = lt_filter ).

    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lo_filter) = NEW zcl_abapgit_object_filter_obj( it_filter = lt_filter ).

    DATA(lt_files) = zcl_abapgit_ortec_obj_index=>get_files_for_filter(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_sha
      ii_obj_filter = lo_filter
      io_dot        = lo_dot
      iv_devclass   = '$PACK' ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_files ) exp = 5000
      msg = 'All bulk rows below the active chunk boundary must survive via the final flush' ).
    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_index=>is_index_ready(
        iv_repo_key = mc_repo iv_commit = lv_commit_sha
        iv_context_hash = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot ) )
      msg = 'Completion marker must be written when the walk never crosses the in-loop chunk check' ).

    SELECT COUNT(*) FROM zaog_obj_index INTO @DATA(lv_count)
      WHERE repo_key = @mc_repo AND commit_sha1 = @lv_commit_sha AND obj_type = 'PROG'.
    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 5000
      msg = 'The final flush must not duplicate or drop any bulk row' ).
  ENDMETHOD.

  METHOD index_empty_no_match.
    " E1-PERF-A: the zero-relevant-rows path must remain correct at the
    " active chunk size - lt_rows never reaches the in-loop chunk check nor
    " the final "IF lt_rows IS NOT INITIAL" flush, yet the completion marker
    " must still be written unconditionally (see rebuild_index's own
    " comment on this exact invariant). A filename with no "." segment
    " (e.g. "readme") maps to an empty obj_type via file_to_object, which
    " rebuild_index skips via its own "obj_type IS INITIAL ... CONTINUE"
    " guard - a real, reachable zero-relevant-rows case (rebuild_index has
    " no filter parameter of its own; ALL resolvable objects in the tree
    " are indexed, so this is not merely a filter mismatch).
    DATA lt_nodes          TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node           LIKE LINE OF lt_nodes.
    DATA lt_objects        TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object         LIKE LINE OF lt_objects.
    DATA ls_commit         TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lv_root_tree_data TYPE xstring.
    DATA lv_root_tree_sha  TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_commit_data    TYPE xstring.
    DATA lv_commit_sha     TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_blob_data      TYPE xstring.
    DATA lv_blob_sha       TYPE zif_abapgit_git_definitions=>ty_sha1.

    lv_blob_data = zcl_abapgit_convert=>string_to_xstring_utf8( 'not abap' ).
    lv_blob_sha = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).

    CLEAR ls_object.
    ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-sha1 = lv_blob_sha.
    ls_object-data = lv_blob_data.
    APPEND ls_object TO lt_objects.

    CLEAR ls_node.
    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_node-name  = 'readme'.
    ls_node-sha1  = lv_blob_sha.
    APPEND ls_node TO lt_nodes.
    lv_root_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    lv_root_tree_sha = zcl_abapgit_hash=>sha1_tree( lv_root_tree_data ).

    ls_commit-tree = lv_root_tree_sha.
    ls_commit-author = 'Test <test@example.com> 0 +0000'.
    ls_commit-committer = 'Test <test@example.com> 0 +0000'.
    ls_commit-body = 'empty no match'.
    lv_commit_data = zcl_abapgit_git_pack=>encode_commit( ls_commit ).
    lv_commit_sha = zcl_abapgit_hash=>sha1_commit( lv_commit_data ).

    CLEAR ls_object.
    ls_object-type = zif_abapgit_git_definitions=>c_type-commit.
    ls_object-sha1 = lv_commit_sha.
    ls_object-data = lv_commit_data.
    APPEND ls_object TO lt_objects.

    ls_object-type = zif_abapgit_git_definitions=>c_type-tree.
    ls_object-sha1 = lv_root_tree_sha.
    ls_object-data = lv_root_tree_data.
    APPEND ls_object TO lt_objects.

    zcl_abapgit_ortec_obj_store=>store_objects( iv_repo_key = mc_repo it_objects = lt_objects ).

    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lo_filter) = NEW zcl_abapgit_object_filter_obj(
      it_filter = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) ) ).

    DATA(lt_files) = zcl_abapgit_ortec_obj_index=>get_files_for_filter(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_sha
      ii_obj_filter = lo_filter
      io_dot        = lo_dot
      iv_devclass   = '$PACK' ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_files ) exp = 0
      msg = 'A tree with no ABAP-resolvable objects must index zero rows' ).
    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_index=>is_index_ready(
        iv_repo_key = mc_repo iv_commit = lv_commit_sha
        iv_context_hash = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot ) )
      msg = 'The completion marker must be written even when zero rows were found - ' &&
            'the final flush check must not gate the unconditional marker write' ).
  ENDMETHOD.

  METHOD ready_rejects_different_context.
    " AR-1-01/AR-2-01: a commit indexed under context A must not be
    " reported ready under a different context B - is_index_ready's own
    " marker predicate must bind iv_context_hash, never trust "any row
    " exists" independent of context.
    DATA(lv_commit_sha) = build_commit( iv_filename = 'zprogram.prog.abap' iv_content = '48656C6C6F' ).
    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lo_filter) = NEW zcl_abapgit_object_filter_obj( it_filter = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) ) ).

    zcl_abapgit_ortec_obj_index=>get_files_for_filter(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_sha
      ii_obj_filter = lo_filter
      io_dot        = lo_dot
      iv_devclass   = '$PACK' ).

    DATA(lv_context_a) = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot ).
    DATA(lv_context_b) = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = 'ZOTHERPACK' io_dot = lo_dot ).

    cl_abap_unit_assert=>assert_differs( act = lv_context_b exp = lv_context_a
      msg = 'Sanity: a different devclass must produce a different context hash' ).
    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_index=>is_index_ready(
        iv_repo_key = mc_repo iv_commit = lv_commit_sha iv_context_hash = lv_context_a )
      msg = 'Sanity: ready under the context it was actually built with' ).
    cl_abap_unit_assert=>assert_false(
      act = zcl_abapgit_ortec_obj_index=>is_index_ready(
        iv_repo_key = mc_repo iv_commit = lv_commit_sha iv_context_hash = lv_context_b )
      msg = 'is_index_ready must not report ready for the same commit under a ' &&
            'different resolution context - the marker predicate must bind context_hash' ).
  ENDMETHOD.

  METHOD select_rows_excludes_other_context.
    " AR-1-01/AR-2-01: ZAOG_OBJ_INDEX carries CONTEXT_HASH as a non-key
    " column safely ONLY because rebuild_index always purges the whole
    " commit before rewriting under a new context (design doc §3.0
    " closure). This proves that runtime guarantee end-to-end: once a
    " context change forces a rebuild, the OLD context can never again be
    " reported ready (its rows/marker no longer exist), so a caller can
    " never read a stale cross-context row via get_files_for_filter.
    DATA(lv_commit_sha) = build_commit( iv_filename = 'zprogram.prog.abap' iv_content = '48656C6C6F' ).
    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lo_filter) = NEW zcl_abapgit_object_filter_obj( it_filter = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) ) ).

    DATA(lt_files_a) = zcl_abapgit_ortec_obj_index=>get_files_for_filter(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_sha
      ii_obj_filter = lo_filter
      io_dot        = lo_dot
      iv_devclass   = '$PACK' ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_files_a ) exp = 1
      msg = 'Sanity: context A must resolve the file' ).

    DATA(lv_context_a) = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot ).

    " Force a rebuild under a DIFFERENT context (different devclass) for
    " the SAME commit - is_index_ready(context B) is false, so
    " get_files_for_filter must purge and re-walk under context B.
    DATA(lt_files_b) = zcl_abapgit_ortec_obj_index=>get_files_for_filter(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_sha
      ii_obj_filter = lo_filter
      io_dot        = lo_dot
      iv_devclass   = 'ZOTHERPACK' ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_files_b ) exp = 1
      msg = 'Context B must independently resolve the same file after its own rebuild' ).

    cl_abap_unit_assert=>assert_false(
      act = zcl_abapgit_ortec_obj_index=>is_index_ready(
        iv_repo_key = mc_repo iv_commit = lv_commit_sha iv_context_hash = lv_context_a )
      msg = 'After a context-B rebuild purged the whole commit, context A''s own ' &&
            'marker/rows must be gone - a later context-A request must re-walk, ' &&
            'never silently reuse a row left behind by a different context' ).
  ENDMETHOD.

  METHOD blank_legacy_context_is_never_ready.
    " AR-1-01: a pre-migration row written before CONTEXT_HASH existed
    " (simulated here as a blank/initial context_hash) must never satisfy
    " is_index_ready for a real, non-blank context - it must be treated as
    " "legacy, always rebuild", never as an accidental context match.
    DATA(lv_commit_sha) = build_commit( iv_filename = 'zprogram.prog.abap' iv_content = '48656C6C6F' ).
    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).

    " Directly seed a legacy-shaped completion marker row (blank context)
    " without going through rebuild_index, simulating data written before
    " this column existed.
    DATA ls_marker TYPE zaog_obj_index.
    CLEAR ls_marker.
    ls_marker-repo_key    = mc_repo.
    ls_marker-commit_sha1 = lv_commit_sha.
    ls_marker-obj_type    = '$IDX'.
    ls_marker-obj_name    = '__READY__'.
    ls_marker-idx_status  = 'R'.
    " ls_marker-context_hash left blank/initial on purpose.
    MODIFY zaog_obj_index FROM ls_marker.

    DATA(lv_real_context) = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot ).

    cl_abap_unit_assert=>assert_false(
      act = zcl_abapgit_ortec_obj_index=>is_index_ready(
        iv_repo_key = mc_repo iv_commit = lv_commit_sha iv_context_hash = lv_real_context )
      msg = 'A blank/legacy context_hash marker row must never be treated as ready ' &&
            'for a real, non-blank context - it must force a rebuild instead' ).
  ENDMETHOD.

  METHOD partial_rows_context_disjoint.
    " AR-2-01 direct retest: two ZAOG_OBJ_PIDX rows for the identical
    " (repo, commit, obj_type, obj_name, path_hash) but DIFFERENT
    " context_hash must physically coexist as distinct rows (context_hash
    " is a real KEY field on this table) - neither can overwrite the
    " other, and select_partial_rows_for_filter must return exactly the
    " row matching the caller's own context, never the other one.
    CONSTANTS lv_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1 VALUE '1111111111111111111111111111111111111111'.
    CONSTANTS lv_context_a TYPE zif_abapgit_git_definitions=>ty_sha1 VALUE 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'.
    CONSTANTS lv_context_b TYPE zif_abapgit_git_definitions=>ty_sha1 VALUE 'BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB'.
    CONSTANTS lv_path_hash TYPE zif_abapgit_git_definitions=>ty_sha1 VALUE 'CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCC'.

    DATA lt_pidx TYPE STANDARD TABLE OF zaog_obj_pidx WITH DEFAULT KEY.
    DATA ls_pidx TYPE zaog_obj_pidx.

    CLEAR ls_pidx.
    ls_pidx-repo_key     = mc_repo.
    ls_pidx-commit_sha1  = lv_commit_sha.
    ls_pidx-obj_type     = 'PROG'.
    ls_pidx-obj_name     = 'ZPROGRAM'.
    ls_pidx-context_hash = lv_context_a.
    ls_pidx-path_hash    = lv_path_hash.
    ls_pidx-file_path    = '/src/'.
    ls_pidx-file_name    = 'zprogram_a.prog.abap'.
    ls_pidx-idx_status   = 'R'.
    APPEND ls_pidx TO lt_pidx.

    ls_pidx-context_hash = lv_context_b.
    ls_pidx-file_name    = 'zprogram_b.prog.abap'.
    APPEND ls_pidx TO lt_pidx.

    MODIFY zaog_obj_pidx FROM TABLE lt_pidx.

    DATA(lt_filter) = VALUE zif_abapgit_definitions=>ty_tadir_tt( ( object = 'PROG' obj_name = 'ZPROGRAM' ) ).

    DATA(lt_rows_a) = zcl_abapgit_ortec_obj_index=>select_partial_rows_for_filter(
      iv_repo_key = mc_repo iv_commit = lv_commit_sha
      iv_context_hash = lv_context_a it_filter = lt_filter ).
    DATA(lt_rows_b) = zcl_abapgit_ortec_obj_index=>select_partial_rows_for_filter(
      iv_repo_key = mc_repo iv_commit = lv_commit_sha
      iv_context_hash = lv_context_b it_filter = lt_filter ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_rows_a ) exp = 1
      msg = 'Context A must see exactly its own row' ).
    cl_abap_unit_assert=>assert_equals( act = lt_rows_a[ 1 ]-file_name exp = 'zprogram_a.prog.abap'
      msg = 'Context A must never see context B''s file' ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_rows_b ) exp = 1
      msg = 'Context B must see exactly its own row' ).
    cl_abap_unit_assert=>assert_equals( act = lt_rows_b[ 1 ]-file_name exp = 'zprogram_b.prog.abap'
      msg = 'Context B must never see context A''s file' ).

    DELETE FROM zaog_obj_pidx WHERE repo_key = mc_repo AND commit_sha1 = lv_commit_sha.
  ENDMETHOD.

  METHOD select_partial_rows_chunk_boundary.
    " AR-1-04: select_partial_rows_for_filter must chunk it_filter at
    " zcl_abapgit_ortec_obj_cover=>c_filter_chunk_size (5000) - proves no
    " row is lost/duplicated when the caller-supplied filter set crosses
    " that boundary.
    CONSTANTS lc_file_count TYPE i VALUE 5100.
    CONSTANTS lv_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1 VALUE '2222222222222222222222222222222222222222'.
    CONSTANTS lv_context TYPE zif_abapgit_git_definitions=>ty_sha1 VALUE 'DDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD'.

    DATA lt_pidx TYPE STANDARD TABLE OF zaog_obj_pidx WITH DEFAULT KEY.
    DATA lt_filter TYPE zif_abapgit_definitions=>ty_tadir_tt.
    DATA ls_pidx TYPE zaog_obj_pidx.
    DATA ls_filter LIKE LINE OF lt_filter.
    DATA lv_index TYPE i.
    DATA lv_obj_name TYPE string.

    DO lc_file_count TIMES.
      lv_index = sy-index.
      lv_obj_name = |ZBULK{ lv_index WIDTH = 6 ALIGN = RIGHT PAD = '0' }|.

      CLEAR ls_pidx.
      ls_pidx-repo_key     = mc_repo.
      ls_pidx-commit_sha1  = lv_commit_sha.
      ls_pidx-obj_type     = 'PROG'.
      ls_pidx-obj_name     = lv_obj_name.
      ls_pidx-context_hash = lv_context.
      ls_pidx-path_hash    = zcl_abapgit_hash=>sha1_string( lv_obj_name ).
      ls_pidx-file_path    = '/src/'.
      ls_pidx-file_name    = |{ lv_obj_name }.prog.abap|.
      ls_pidx-idx_status   = 'R'.
      APPEND ls_pidx TO lt_pidx.

      CLEAR ls_filter.
      ls_filter-object    = 'PROG'.
      ls_filter-obj_name  = lv_obj_name.
      APPEND ls_filter TO lt_filter.
    ENDDO.

    MODIFY zaog_obj_pidx FROM TABLE lt_pidx.

    DATA(lt_rows) = zcl_abapgit_ortec_obj_index=>select_partial_rows_for_filter(
      iv_repo_key = mc_repo iv_commit = lv_commit_sha
      iv_context_hash = lv_context it_filter = lt_filter ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_rows ) exp = lc_file_count
      msg = 'All rows must be returned across the c_filter_chunk_size boundary, ' &&
            'not just the first chunk' ).

    DELETE FROM zaog_obj_pidx WHERE repo_key = mc_repo AND commit_sha1 = lv_commit_sha.
  ENDMETHOD.

  METHOD warm_coverage_skips_rewalk.
    " Slice 2 (design §11 step 3.4): once coverage is FOUND for every
    " requested object, get_files_for_filter must serve the file straight
    " from ZAOG_OBJ_PIDX without ever touching ZAOG_OBJ_STORE/rebuild_index
    " - proven here by seeding ONLY the coverage/partial-index rows (no
    " commit/tree/blob object is stored at all) and confirming the file is
    " still returned without any exception, which a real tree walk against
    " a nonexistent commit would otherwise raise.
    CONSTANTS lv_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1
      VALUE '3333333333333333333333333333333333333333'.

    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lv_context) = zcl_abapgit_ortec_obj_cover=>compute_context_hash(
      iv_devclass = '$PACK' io_dot = lo_dot ).

    DATA ls_cover TYPE zaog_obj_cover.
    CLEAR ls_cover.
    ls_cover-repo_key          = mc_repo.
    ls_cover-commit_sha1       = lv_commit_sha.
    ls_cover-obj_type          = 'PROG'.
    ls_cover-obj_name          = 'ZPROGRAM'.
    ls_cover-context_hash      = lv_context.
    ls_cover-resolution_status = zcl_abapgit_ortec_obj_cover=>cs_resolution-found.
    MODIFY zaog_obj_cover FROM ls_cover.

    DATA ls_pidx TYPE zaog_obj_pidx.
    CLEAR ls_pidx.
    ls_pidx-repo_key     = mc_repo.
    ls_pidx-commit_sha1  = lv_commit_sha.
    ls_pidx-obj_type     = 'PROG'.
    ls_pidx-obj_name     = 'ZPROGRAM'.
    ls_pidx-context_hash = lv_context.
    ls_pidx-path_hash    = zcl_abapgit_hash=>sha1_string( '/src/zprogram.prog.abap' ).
    ls_pidx-file_path    = '/src/'.
    ls_pidx-file_name    = 'zprogram.prog.abap'.
    ls_pidx-blob_sha1    = zcl_abapgit_hash=>sha1_blob( '48656C6C6F' ).
    ls_pidx-idx_status   = 'R'.
    MODIFY zaog_obj_pidx FROM ls_pidx.

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = ls_pidx-blob_sha1
      iv_type = zif_abapgit_git_definitions=>c_type-blob iv_data = '48656C6C6F' ).

    DATA(lo_filter) = NEW zcl_abapgit_object_filter_obj( it_filter = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) ) ).

    DATA(lt_files) = zcl_abapgit_ortec_obj_index=>get_files_for_filter(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_sha
      ii_obj_filter = lo_filter
      io_dot        = lo_dot
      iv_devclass   = '$PACK' ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_files ) exp = 1
      msg = 'A FOUND coverage row must serve the file from ZAOG_OBJ_PIDX without a real walk - ' &&
            'no commit/tree object exists in ZAOG_OBJ_STORE, so any attempted rebuild would raise' ).
    cl_abap_unit_assert=>assert_equals( act = lt_files[ 1 ]-filename exp = 'zprogram.prog.abap' ).
    cl_abap_unit_assert=>assert_false(
      act = zcl_abapgit_ortec_obj_index=>is_index_ready(
        iv_repo_key = mc_repo iv_commit = lv_commit_sha iv_context_hash = lv_context )
      msg = 'The warm-via-coverage path must not write a COMPLETE-mode $IDX/__READY__ marker - ' &&
            'FILTERED-mode coverage never becomes COMPLETE-mode readiness' ).

    DELETE FROM zaog_obj_cover WHERE repo_key = mc_repo AND commit_sha1 = lv_commit_sha.
    DELETE FROM zaog_obj_pidx WHERE repo_key = mc_repo AND commit_sha1 = lv_commit_sha.
  ENDMETHOD.

  METHOD incomplete_coverage_falls_through_to_rebuild.
    " Slice 2: when coverage is NOT complete for every requested object
    " (walk_filtered does not exist until Slice 3), ensure_filtered_coverage
    " must fall through to the existing, unchanged COMPLETE-mode
    " ensure_index/rebuild_index path - proving Slice 2 adds a fast path
    " only and does not yet change cold-walk behavior.
    DATA(lv_commit_sha) = build_commit( iv_filename = 'zprogram.prog.abap' iv_content = '48656C6C6F' ).
    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lo_filter) = NEW zcl_abapgit_object_filter_obj( it_filter = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) ) ).

    " No ZAOG_OBJ_COVER row exists for this (repo, commit, object, context) -
    " coverage is incomplete, so the file must still be resolved via a real
    " COMPLETE-mode rebuild against the real stored commit/tree/blob.
    DATA(lt_files) = zcl_abapgit_ortec_obj_index=>get_files_for_filter(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_sha
      ii_obj_filter = lo_filter
      io_dot        = lo_dot
      iv_devclass   = '$PACK' ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_files ) exp = 1
      msg = 'Incomplete coverage must fall through to the existing COMPLETE-mode rebuild path' ).

    DATA(lv_context) = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot ).
    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_index=>is_index_ready(
        iv_repo_key = mc_repo iv_commit = lv_commit_sha iv_context_hash = lv_context )
      msg = 'The fallback COMPLETE-mode path must still write the $IDX/__READY__ marker as today' ).
  ENDMETHOD.

  METHOD build_commit_two_objects.
    " Shared fixture for Slice 3 tests needing TWO distinct filter-relevant
    " objects in the same tree (zprogram.prog.abap -> PROG/ZPROGRAM,
    " zother.prog.abap -> PROG/ZOTHER), both under /src/.
    DATA lt_nodes           TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node            LIKE LINE OF lt_nodes.
    DATA ls_commit          TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lv_blob_sha_1      TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_blob_sha_2      TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_src_tree_data   TYPE xstring.
    DATA lv_src_tree_sha    TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_root_tree_data  TYPE xstring.
    DATA lv_root_tree_sha   TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_commit_data     TYPE xstring.

    lv_blob_sha_1 = zcl_abapgit_hash=>sha1_blob( '48656C6C6F' ).
    lv_blob_sha_2 = zcl_abapgit_hash=>sha1_blob( '576F726C64' ).

    CLEAR ls_node.
    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_node-name  = 'zprogram.prog.abap'.
    ls_node-sha1  = lv_blob_sha_1.
    APPEND ls_node TO lt_nodes.

    CLEAR ls_node.
    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_node-name  = 'zother.prog.abap'.
    ls_node-sha1  = lv_blob_sha_2.
    APPEND ls_node TO lt_nodes.

    lv_src_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    lv_src_tree_sha = zcl_abapgit_hash=>sha1_tree( lv_src_tree_data ).

    CLEAR lt_nodes.
    CLEAR ls_node.
    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-dir.
    ls_node-name  = 'src'.
    ls_node-sha1  = lv_src_tree_sha.
    APPEND ls_node TO lt_nodes.
    lv_root_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    lv_root_tree_sha = zcl_abapgit_hash=>sha1_tree( lv_root_tree_data ).

    ls_commit-tree = lv_root_tree_sha.
    ls_commit-author = 'Test <test@example.com> 0 +0000'.
    ls_commit-committer = 'Test <test@example.com> 0 +0000'.
    ls_commit-body = 'two objects'.
    lv_commit_data = zcl_abapgit_git_pack=>encode_commit( ls_commit ).
    rv_commit_sha = zcl_abapgit_hash=>sha1_commit( lv_commit_data ).

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = rv_commit_sha
      iv_type = zif_abapgit_git_definitions=>c_type-commit iv_data = lv_commit_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_root_tree_sha
      iv_type = zif_abapgit_git_definitions=>c_type-tree iv_data = lv_root_tree_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_src_tree_sha
      iv_type = zif_abapgit_git_definitions=>c_type-tree iv_data = lv_src_tree_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_blob_sha_1
      iv_type = zif_abapgit_git_definitions=>c_type-blob iv_data = '48656C6C6F' ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_blob_sha_2
      iv_type = zif_abapgit_git_definitions=>c_type-blob iv_data = '576F726C64' ).
  ENDMETHOD.

  METHOD filtered_walk_writes_only_requested_objects.
    " design §11 step 4: a tree leaf's row is appended ONLY IF present in
    " it_filter - ZOTHER must never get a ZAOG_OBJ_PIDX row even though it
    " exists in the same tree, when only ZPROGRAM was requested.
    DATA(lv_commit_sha) = build_commit_two_objects( ).
    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lv_context) = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot ).

    zcl_abapgit_ortec_obj_index=>walk_filtered(
      iv_repo_key     = mc_repo
      iv_commit       = lv_commit_sha
      io_dot          = lo_dot
      iv_devclass     = '$PACK'
      it_filter       = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) )
      iv_context_hash = lv_context ).

    SELECT COUNT(*) FROM zaog_obj_pidx INTO @DATA(lv_prog_count)
      WHERE repo_key = @mc_repo AND commit_sha1 = @lv_commit_sha AND obj_name = 'ZPROGRAM'.
    SELECT COUNT(*) FROM zaog_obj_pidx INTO @DATA(lv_other_count)
      WHERE repo_key = @mc_repo AND commit_sha1 = @lv_commit_sha AND obj_name = 'ZOTHER'.

    cl_abap_unit_assert=>assert_equals( act = lv_prog_count exp = 1
      msg = 'The requested object must get exactly one ZAOG_OBJ_PIDX row' ).
    cl_abap_unit_assert=>assert_equals( act = lv_other_count exp = 0
      msg = 'A non-requested object present in the same tree must get zero rows - ' &&
            'walk_filtered must bound writes to it_filter, never the full tree' ).
  ENDMETHOD.

  METHOD filtered_walk_never_sets_ready_marker.
    " design §12: FILTERED-mode walk_filtered never writes ZAOG_OBJ_INDEX
    " or its $IDX/__READY__ marker - is_index_ready must stay false.
    DATA(lv_commit_sha) = build_commit( iv_filename = 'zprogram.prog.abap' iv_content = '48656C6C6F' ).
    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lv_context) = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot ).

    zcl_abapgit_ortec_obj_index=>walk_filtered(
      iv_repo_key     = mc_repo
      iv_commit       = lv_commit_sha
      io_dot          = lo_dot
      iv_devclass     = '$PACK'
      it_filter       = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) )
      iv_context_hash = lv_context ).

    cl_abap_unit_assert=>assert_false(
      act = zcl_abapgit_ortec_obj_index=>is_index_ready(
        iv_repo_key = mc_repo iv_commit = lv_commit_sha iv_context_hash = lv_context )
      msg = 'walk_filtered must never write the COMPLETE-mode $IDX/__READY__ marker' ).

    SELECT SINGLE @abap_true FROM zaog_obj_index INTO @DATA(lv_exists)
      WHERE repo_key = @mc_repo AND commit_sha1 = @lv_commit_sha.
    cl_abap_unit_assert=>assert_initial( act = lv_exists
      msg = 'walk_filtered must never write any ZAOG_OBJ_INDEX row at all' ).
  ENDMETHOD.

  METHOD filtered_walk_idempotent_on_overlap.
    " design §5/§11 step 4: two overlapping-but-different-filter walks
    " under the same context must be idempotent - ZPROGRAM's row must not
    " be duplicated when a second, wider walk includes it again alongside
    " a new object.
    DATA(lv_commit_sha) = build_commit_two_objects( ).
    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lv_context) = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot ).

    zcl_abapgit_ortec_obj_index=>walk_filtered(
      iv_repo_key     = mc_repo
      iv_commit       = lv_commit_sha
      io_dot          = lo_dot
      iv_devclass     = '$PACK'
      it_filter       = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) )
      iv_context_hash = lv_context ).

    zcl_abapgit_ortec_obj_index=>walk_filtered(
      iv_repo_key     = mc_repo
      iv_commit       = lv_commit_sha
      io_dot          = lo_dot
      iv_devclass     = '$PACK'
      it_filter       = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) ( object = 'PROG' obj_name = 'ZOTHER' ) )
      iv_context_hash = lv_context ).

    SELECT COUNT(*) FROM zaog_obj_pidx INTO @DATA(lv_prog_count)
      WHERE repo_key = @mc_repo AND commit_sha1 = @lv_commit_sha AND obj_name = 'ZPROGRAM'.
    SELECT COUNT(*) FROM zaog_obj_pidx INTO @DATA(lv_other_count)
      WHERE repo_key = @mc_repo AND commit_sha1 = @lv_commit_sha AND obj_name = 'ZOTHER'.

    cl_abap_unit_assert=>assert_equals( act = lv_prog_count exp = 1
      msg = 'A second overlapping walk must not duplicate the first walk''s row (MODIFY upsert)' ).
    cl_abap_unit_assert=>assert_equals( act = lv_other_count exp = 1
      msg = 'The second walk''s new object must still be resolved' ).
  ENDMETHOD.

  METHOD filtered_walk_writes_context_hash_as_key.
    " AR-2-01 direct write-side retest: two walk_filtered calls under
    " DIFFERENT contexts for the SAME object/path must produce two
    " physically distinct ZAOG_OBJ_PIDX rows, never one overwriting the
    " other (context_hash is a real KEY field on this table).
    DATA(lv_commit_sha) = build_commit( iv_filename = 'zprogram.prog.abap' iv_content = '48656C6C6F' ).
    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lv_context_a) = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot ).
    DATA(lv_context_b) = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = 'ZOTHERPACK' io_dot = lo_dot ).

    zcl_abapgit_ortec_obj_index=>walk_filtered(
      iv_repo_key     = mc_repo iv_commit = lv_commit_sha io_dot = lo_dot iv_devclass = '$PACK'
      it_filter       = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) )
      iv_context_hash = lv_context_a ).
    zcl_abapgit_ortec_obj_index=>walk_filtered(
      iv_repo_key     = mc_repo iv_commit = lv_commit_sha io_dot = lo_dot iv_devclass = 'ZOTHERPACK'
      it_filter       = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) )
      iv_context_hash = lv_context_b ).

    SELECT COUNT(*) FROM zaog_obj_pidx INTO @DATA(lv_count)
      WHERE repo_key = @mc_repo AND commit_sha1 = @lv_commit_sha AND obj_name = 'ZPROGRAM'.

    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 2
      msg = 'Two different contexts must produce two physically distinct ZAOG_OBJ_PIDX rows' ).
  ENDMETHOD.

  METHOD filtered_walk_no_cross_context_overwrite.
    " AR-2-01: after a context-B walk, context A's own earlier row must
    " remain fully intact and readable via select_partial_rows_for_filter.
    DATA(lv_commit_sha) = build_commit( iv_filename = 'zprogram.prog.abap' iv_content = '48656C6C6F' ).
    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lv_context_a) = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot ).
    DATA(lv_context_b) = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = 'ZOTHERPACK' io_dot = lo_dot ).

    zcl_abapgit_ortec_obj_index=>walk_filtered(
      iv_repo_key     = mc_repo iv_commit = lv_commit_sha io_dot = lo_dot iv_devclass = '$PACK'
      it_filter       = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) )
      iv_context_hash = lv_context_a ).
    zcl_abapgit_ortec_obj_index=>walk_filtered(
      iv_repo_key     = mc_repo iv_commit = lv_commit_sha io_dot = lo_dot iv_devclass = 'ZOTHERPACK'
      it_filter       = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) )
      iv_context_hash = lv_context_b ).

    DATA(lt_rows_a) = zcl_abapgit_ortec_obj_index=>select_partial_rows_for_filter(
      iv_repo_key = mc_repo iv_commit = lv_commit_sha iv_context_hash = lv_context_a
      it_filter = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) ) ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_rows_a ) exp = 1
      msg = 'Context A''s row must survive a later context-B walk for the same object' ).
    cl_abap_unit_assert=>assert_equals( act = lt_rows_a[ 1 ]-context_hash exp = lv_context_a ).
  ENDMETHOD.

  METHOD retry_purge_removes_all_three_tables.
    " AR-1-02/AR-2-01 direct retest: invalidate_commit_index must purge
    " ZAOG_OBJ_INDEX, ZAOG_OBJ_COVER, and ZAOG_OBJ_PIDX together, in one
    " call, for a commit - never leaving orphaned coverage/partial rows
    " behind after an index-side purge.
    CONSTANTS lv_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1
      VALUE '4444444444444444444444444444444444444444'.

    DATA ls_index TYPE zaog_obj_index.
    DATA ls_cover TYPE zaog_obj_cover.
    DATA ls_pidx  TYPE zaog_obj_pidx.

    CLEAR ls_index.
    ls_index-repo_key = mc_repo. ls_index-commit_sha1 = lv_commit_sha.
    ls_index-obj_type = 'PROG'. ls_index-obj_name = 'ZPROGRAM'.
    ls_index-path_hash = 'A'. ls_index-idx_status = 'R'.
    MODIFY zaog_obj_index FROM ls_index.

    CLEAR ls_cover.
    ls_cover-repo_key = mc_repo. ls_cover-commit_sha1 = lv_commit_sha.
    ls_cover-obj_type = 'PROG'. ls_cover-obj_name = 'ZPROGRAM'.
    ls_cover-context_hash = 'A'. ls_cover-resolution_status = 'F'.
    MODIFY zaog_obj_cover FROM ls_cover.

    CLEAR ls_pidx.
    ls_pidx-repo_key = mc_repo. ls_pidx-commit_sha1 = lv_commit_sha.
    ls_pidx-obj_type = 'PROG'. ls_pidx-obj_name = 'ZPROGRAM'.
    ls_pidx-context_hash = 'A'. ls_pidx-path_hash = 'A'. ls_pidx-idx_status = 'R'.
    MODIFY zaog_obj_pidx FROM ls_pidx.

    zcl_abapgit_ortec_obj_index=>invalidate_commit_index(
      iv_repo_key = mc_repo iv_commit = lv_commit_sha ).

    SELECT COUNT(*) FROM zaog_obj_index INTO @DATA(lv_idx_count)
      WHERE repo_key = @mc_repo AND commit_sha1 = @lv_commit_sha.
    SELECT COUNT(*) FROM zaog_obj_cover INTO @DATA(lv_cov_count)
      WHERE repo_key = @mc_repo AND commit_sha1 = @lv_commit_sha.
    SELECT COUNT(*) FROM zaog_obj_pidx INTO @DATA(lv_pidx_count)
      WHERE repo_key = @mc_repo AND commit_sha1 = @lv_commit_sha.

    cl_abap_unit_assert=>assert_equals( act = lv_idx_count exp = 0
      msg = 'invalidate_commit_index must purge ZAOG_OBJ_INDEX' ).
    cl_abap_unit_assert=>assert_equals( act = lv_cov_count exp = 0
      msg = 'invalidate_commit_index must purge ZAOG_OBJ_COVER' ).
    cl_abap_unit_assert=>assert_equals( act = lv_pidx_count exp = 0
      msg = 'invalidate_commit_index must purge ZAOG_OBJ_PIDX' ).
  ENDMETHOD.

  METHOD missing_tree_writes_m_row_then_reraises.
    " design §4.1/AR-1-07: a missing commit must cause walk_filtered to
    " raise, and best-effort write one 'M' (unresolved_missing_local_data)
    " coverage row per it_filter entry BEFORE re-raising the original
    " exception unchanged.
    CONSTANTS lv_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1
      VALUE '5555555555555555555555555555555555555555'.

    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lv_context) = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot ).
    DATA lv_raised TYPE abap_bool.

    TRY.
        zcl_abapgit_ortec_obj_index=>walk_filtered(
          iv_repo_key     = mc_repo
          iv_commit       = lv_commit_sha
          io_dot          = lo_dot
          iv_devclass     = '$PACK'
          it_filter       = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) )
          iv_context_hash = lv_context ).
      CATCH zcx_abapgit_exception.
        lv_raised = abap_true.
    ENDTRY.

    cl_abap_unit_assert=>assert_true( act = lv_raised
      msg = 'walk_filtered must raise when the commit is missing from ZAOG_OBJ_STORE' ).

    SELECT SINGLE resolution_status FROM zaog_obj_cover INTO @DATA(lv_status)
      WHERE repo_key = @mc_repo AND commit_sha1 = @lv_commit_sha
        AND obj_type = 'PROG' AND obj_name = 'ZPROGRAM' AND context_hash = @lv_context.

    cl_abap_unit_assert=>assert_equals(
      act = lv_status exp = zcl_abapgit_ortec_obj_cover=>cs_resolution-unresolved_missing_local_data
      msg = 'A best-effort M row must be written for every requested object before the re-raise' ).
  ENDMETHOD.

  METHOD repeat_request_within_backoff_skips_walk.
    " design §4.1/§11 step 3a: a fresh 'M' row must short-circuit
    " ensure_filtered_coverage to an immediate raise, WITHOUT attempting
    " walk_filtered again - proven here by seeding the backoff row against
    " a commit that IS otherwise fully resolvable (so any actual walk
    " attempt would succeed and write a row, which must NOT happen).
    DATA(lv_commit_sha) = build_commit( iv_filename = 'zprogram.prog.abap' iv_content = '48656C6C6F' ).
    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lv_context) = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot ).
    DATA lv_raised TYPE abap_bool.
    DATA lv_now TYPE timestampl.

    GET TIME STAMP FIELD lv_now.

    DATA ls_cover TYPE zaog_obj_cover.
    CLEAR ls_cover.
    ls_cover-repo_key = mc_repo. ls_cover-commit_sha1 = lv_commit_sha.
    ls_cover-obj_type = 'PROG'. ls_cover-obj_name = 'ZPROGRAM'.
    ls_cover-context_hash = lv_context.
    ls_cover-resolution_status = zcl_abapgit_ortec_obj_cover=>cs_resolution-unresolved_missing_local_data.
    ls_cover-resolved_at = lv_now.
    MODIFY zaog_obj_cover FROM ls_cover.

    TRY.
        zcl_abapgit_ortec_obj_index=>ensure_filtered_coverage(
          iv_repo_key     = mc_repo
          iv_commit       = lv_commit_sha
          io_dot          = lo_dot
          iv_devclass     = '$PACK'
          it_filter       = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) )
          iv_context_hash = lv_context ).
      CATCH zcx_abapgit_exception.
        lv_raised = abap_true.
    ENDTRY.

    cl_abap_unit_assert=>assert_true( act = lv_raised
      msg = 'A fresh backoff M row must short-circuit to a raise without re-walking' ).

    SELECT SINGLE @abap_true FROM zaog_obj_pidx INTO @DATA(lv_exists)
      WHERE repo_key = @mc_repo AND commit_sha1 = @lv_commit_sha AND obj_name = 'ZPROGRAM'.
    cl_abap_unit_assert=>assert_initial( act = lv_exists
      msg = 'No ZAOG_OBJ_PIDX row may exist - the walk that would have succeeded must never run ' &&
            'while the backoff is live' ).
  ENDMETHOD.

  METHOD repeat_request_after_backoff_retries_walk.
    " design §4.1: once the backoff window has elapsed, the exact same
    " request must retry a real walk_filtered and succeed.
    DATA(lv_commit_sha) = build_commit( iv_filename = 'zprogram.prog.abap' iv_content = '48656C6C6F' ).
    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lv_context) = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot ).
    DATA lv_now TYPE timestampl.
    DATA lv_expired TYPE timestampl.

    GET TIME STAMP FIELD lv_now.
    lv_expired = lv_now - ( zcl_abapgit_ortec_obj_cover=>c_missing_data_backoff_seconds + 60 ).

    DATA ls_cover TYPE zaog_obj_cover.
    CLEAR ls_cover.
    ls_cover-repo_key = mc_repo. ls_cover-commit_sha1 = lv_commit_sha.
    ls_cover-obj_type = 'PROG'. ls_cover-obj_name = 'ZPROGRAM'.
    ls_cover-context_hash = lv_context.
    ls_cover-resolution_status = zcl_abapgit_ortec_obj_cover=>cs_resolution-unresolved_missing_local_data.
    ls_cover-resolved_at = lv_expired.
    MODIFY zaog_obj_cover FROM ls_cover.

    DATA(lt_rows) = zcl_abapgit_ortec_obj_index=>ensure_filtered_coverage(
      iv_repo_key     = mc_repo
      iv_commit       = lv_commit_sha
      io_dot          = lo_dot
      iv_devclass     = '$PACK'
      it_filter       = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) )
      iv_context_hash = lv_context ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_rows ) exp = 1
      msg = 'An expired backoff must allow a real retry walk that resolves the file' ).
  ENDMETHOD.

  METHOD not_present_remote_requires_current_remote_commit.
    " AR-2-02/§13 W8: a zero-match result may only claim the strong
    " RESOLVED_NOT_PRESENT_REMOTE fact when the graph is have-eligible
    " AND iv_current_remote actually equals iv_commit. A mismatched
    " iv_current_remote must cap the result at RESOLVED_NO_FILES even
    " though the graph is otherwise have-eligible.
    DATA(lv_commit_sha) = build_commit( iv_filename = 'zprogram.prog.abap' iv_content = '48656C6C6F' ).
    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lv_context) = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot ).

    MODIFY zaog_commit_hist FROM VALUE #(
      repo_key = mc_repo commit_sha1 = lv_commit_sha
      hist_level = zcl_abapgit_ortec_mat_state=>cs_hist_level-graph_complete ).

    " Positive control: matching current-remote DOES yield the strong state.
    zcl_abapgit_ortec_obj_index=>walk_filtered(
      iv_repo_key       = mc_repo iv_commit = lv_commit_sha io_dot = lo_dot iv_devclass = '$PACK'
      it_filter         = VALUE #( ( object = 'PROG' obj_name = 'ZOTHER' ) )
      iv_context_hash   = lv_context
      iv_current_remote = lv_commit_sha ).

    SELECT SINGLE resolution_status FROM zaog_obj_cover INTO @DATA(lv_status_match)
      WHERE repo_key = @mc_repo AND commit_sha1 = @lv_commit_sha
        AND obj_type = 'PROG' AND obj_name = 'ZOTHER' AND context_hash = @lv_context.
    cl_abap_unit_assert=>assert_equals(
      act = lv_status_match exp = zcl_abapgit_ortec_obj_cover=>cs_resolution-resolved_not_present_remote
      msg = 'Graph-complete AND iv_current_remote = iv_commit must yield the strong negative state' ).

    " Negative control: a mismatched current-remote must cap at the weaker state.
    zcl_abapgit_ortec_obj_index=>walk_filtered(
      iv_repo_key       = mc_repo iv_commit = lv_commit_sha io_dot = lo_dot iv_devclass = '$PACK'
      it_filter         = VALUE #( ( object = 'PROG' obj_name = 'ZOTHER' ) )
      iv_context_hash   = lv_context
      iv_current_remote = '6666666666666666666666666666666666666666' ).

    SELECT SINGLE resolution_status FROM zaog_obj_cover INTO @DATA(lv_status_mismatch)
      WHERE repo_key = @mc_repo AND commit_sha1 = @lv_commit_sha
        AND obj_type = 'PROG' AND obj_name = 'ZOTHER' AND context_hash = @lv_context.
    cl_abap_unit_assert=>assert_equals(
      act = lv_status_mismatch exp = zcl_abapgit_ortec_obj_cover=>cs_resolution-resolved_no_files
      msg = 'A mismatched iv_current_remote must cap the result at RESOLVED_NO_FILES, ' &&
            'even though the graph is have-eligible' ).
  ENDMETHOD.

  METHOD not_present_remote_requires_current_remote_supplied.
    " AR-2-02: an omitted/initial iv_current_remote must also yield
    " RESOLVED_NO_FILES, even when the commit is otherwise graph-have-eligible.
    DATA(lv_commit_sha) = build_commit( iv_filename = 'zprogram.prog.abap' iv_content = '48656C6C6F' ).
    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lv_context) = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot ).

    MODIFY zaog_commit_hist FROM VALUE #(
      repo_key = mc_repo commit_sha1 = lv_commit_sha
      hist_level = zcl_abapgit_ortec_mat_state=>cs_hist_level-graph_complete ).

    zcl_abapgit_ortec_obj_index=>walk_filtered(
      iv_repo_key     = mc_repo iv_commit = lv_commit_sha io_dot = lo_dot iv_devclass = '$PACK'
      it_filter       = VALUE #( ( object = 'PROG' obj_name = 'ZOTHER' ) )
      iv_context_hash = lv_context ).
      " iv_current_remote intentionally omitted - always initial, as on
      " the real pull_filtered call path (AR-2-02).

    SELECT SINGLE resolution_status FROM zaog_obj_cover INTO @DATA(lv_status)
      WHERE repo_key = @mc_repo AND commit_sha1 = @lv_commit_sha
        AND obj_type = 'PROG' AND obj_name = 'ZOTHER' AND context_hash = @lv_context.
    cl_abap_unit_assert=>assert_equals(
      act = lv_status exp = zcl_abapgit_ortec_obj_cover=>cs_resolution-resolved_no_files
      msg = 'An omitted iv_current_remote must cap the result at RESOLVED_NO_FILES regardless ' &&
            'of graph completeness' ).
  ENDMETHOD.

  METHOD select_rows_chunk_boundary.
    " PA-001: select_rows_for_filter must chunk it_filter at
    " zcl_abapgit_ortec_obj_cover=>c_filter_chunk_size (5000), not issue one
    " unbounded FOR ALL ENTRIES - proves no row is lost/duplicated across
    " the chunk boundary on the WARM (is_index_ready = true) COMPLETE-mode
    " read path.
    CONSTANTS lc_file_count TYPE i VALUE 5100.
    DATA lv_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lt_filter TYPE zif_abapgit_definitions=>ty_tadir_tt.

    build_bulk_commit(
      EXPORTING iv_file_count = lc_file_count
      IMPORTING ev_commit_sha = lv_commit_sha et_filter = lt_filter ).

    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(lo_filter) = NEW zcl_abapgit_object_filter_obj( it_filter = lt_filter ).

    " First call builds the COMPLETE index (rebuild_index) so the second
    " call below exercises select_rows_for_filter's own WARM read path.
    zcl_abapgit_ortec_obj_index=>get_files_for_filter(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_sha
      ii_obj_filter = lo_filter
      io_dot        = lo_dot
      iv_devclass   = '$PACK' ).

    DATA(lv_context) = zcl_abapgit_ortec_obj_cover=>compute_context_hash( iv_devclass = '$PACK' io_dot = lo_dot ).
    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_index=>is_index_ready(
        iv_repo_key = mc_repo iv_commit = lv_commit_sha iv_context_hash = lv_context )
      msg = 'Sanity: the index must be COMPLETE-ready before exercising the warm chunked read' ).

    DATA(lt_rows) = zcl_abapgit_ortec_obj_index=>select_rows_for_filter(
      iv_repo_key     = mc_repo
      iv_commit       = lv_commit_sha
      it_filter       = lt_filter
      iv_context_hash = lv_context ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_rows ) exp = lc_file_count
      msg = 'All rows must be returned across the c_filter_chunk_size boundary on the warm ' &&
            'COMPLETE-mode read path, not just the first chunk' ).
  ENDMETHOD.
ENDCLASS.
