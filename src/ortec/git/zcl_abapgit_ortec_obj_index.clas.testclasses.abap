CLASS ltcl_obj_index DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_OBJIDX'.
    METHODS setup. METHODS teardown.
    METHODS marker_required_for_ready FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_obj_index IMPLEMENTATION.
  METHOD setup.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    DELETE FROM zaog_obj_index WHERE repo_key = mc_repo.
  ENDMETHOD.
  METHOD teardown.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    DELETE FROM zaog_obj_index WHERE repo_key = mc_repo.
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
ENDCLASS.
