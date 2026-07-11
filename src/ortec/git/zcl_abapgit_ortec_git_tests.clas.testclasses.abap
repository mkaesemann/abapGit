CLASS ltcl_obj_store DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOG_TEST_01'.
    METHODS setup. METHODS teardown.
    METHODS store_and_get FOR TESTING RAISING cx_static_check.
    METHODS not_found FOR TESTING RAISING cx_static_check.
    METHODS get_objects_bulk FOR TESTING RAISING cx_static_check.
    METHODS get_objects_missing FOR TESTING RAISING cx_static_check.
    METHODS reachable_objects_graph FOR TESTING RAISING cx_static_check.
    METHODS reachable_objects_missing_tree FOR TESTING RAISING cx_static_check.
    METHODS missing_sha1s_none FOR TESTING RAISING cx_static_check.
    METHODS missing_sha1s_some FOR TESTING RAISING cx_static_check.
    METHODS object_state_constants FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_obj_store IMPLEMENTATION.
  METHOD setup. DELETE FROM zaog_obj_store WHERE repo_key = mc_repo. ENDMETHOD.
  METHOD teardown. DELETE FROM zaog_obj_store WHERE repo_key = mc_repo. ROLLBACK WORK. ENDMETHOD.
  METHOD store_and_get.
    DATA lv TYPE xstring. DATA ls TYPE zif_abapgit_definitions=>ty_object. lv = '48656C6C6F'.
    zcl_abapgit_ortec_obj_store=>store_object( iv_repo_key = mc_repo iv_sha1 = 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d' iv_type = 'blob' iv_data = lv ).
    ls = zcl_abapgit_ortec_obj_store=>get_object( iv_repo_key = mc_repo iv_sha1 = 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d' ).
    cl_abap_unit_assert=>assert_equals( act = ls-sha1 exp = 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d' ).
  ENDMETHOD.
  METHOD not_found.
    TRY. zcl_abapgit_ortec_obj_store=>get_object( iv_repo_key = mc_repo iv_sha1 = 'ffffffffffffffffffffffffffffffffffffffff' ). cl_abap_unit_assert=>fail( ). CATCH zcx_abapgit_ortec_git. ENDTRY.
  ENDMETHOD.
  METHOD get_objects_bulk.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_first TYPE xstring.
    DATA lv_second TYPE xstring.

    lv_first = '31'.
    lv_second = '32'.
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = '1111111111111111111111111111111111111111'
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_first ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = '2222222222222222222222222222222222222222'
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_second ).

    APPEND '1111111111111111111111111111111111111111' TO lt_sha1s.
    APPEND '2222222222222222222222222222222222222222' TO lt_sha1s.
    APPEND '1111111111111111111111111111111111111111' TO lt_sha1s.

    lt_objects = zcl_abapgit_ortec_obj_store=>get_objects(
      iv_repo_key = mc_repo
      it_sha1s    = lt_sha1s ).

    cl_abap_unit_assert=>assert_equals(
      act = lines( lt_objects )
      exp = 2
      msg = 'Duplicate SHA input is read once' ).
    READ TABLE lt_objects TRANSPORTING NO FIELDS
         WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-blob
                                  sha1 = '2222222222222222222222222222222222222222'.
    cl_abap_unit_assert=>assert_subrc( msg = 'Second blob was read' ).
  ENDMETHOD.
  METHOD get_objects_missing.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    APPEND 'ffffffffffffffffffffffffffffffffffffffff' TO lt_sha1s.
    TRY.
        zcl_abapgit_ortec_obj_store=>get_objects(
          iv_repo_key = mc_repo
          it_sha1s    = lt_sha1s ).
        cl_abap_unit_assert=>fail( 'Missing bulk object must raise' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.
  METHOD reachable_objects_graph.
    DATA lt_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node LIKE LINE OF lt_nodes.
    DATA ls_commit TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_expanded TYPE zif_abapgit_git_definitions=>ty_expanded_tt.
    DATA lv_blob_data TYPE xstring.
    DATA lv_blob_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_tree_data TYPE xstring.
    DATA lv_tree_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_commit_data TYPE xstring.
    DATA lv_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    lv_blob_data = '48656C6C6F'.
    lv_blob_sha = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).

    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_node-name  = 'hello.txt'.
    ls_node-sha1  = lv_blob_sha.
    APPEND ls_node TO lt_nodes.

    lv_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    lv_tree_sha = zcl_abapgit_hash=>sha1_tree( lv_tree_data ).

    ls_commit-tree = lv_tree_sha.
    ls_commit-author = 'Test <test@example.com> 0 +0000'.
    ls_commit-committer = 'Test <test@example.com> 0 +0000'.
    ls_commit-body = 'test'.
    lv_commit_data = zcl_abapgit_git_pack=>encode_commit( ls_commit ).
    lv_commit_sha = zcl_abapgit_hash=>sha1_commit( lv_commit_data ).

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_commit_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-commit
      iv_data     = lv_commit_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_tree_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-tree
      iv_data     = lv_tree_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_blob_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_blob_data ).

    lt_objects = zcl_abapgit_ortec_obj_store=>get_reachable_objects(
      iv_repo_key = mc_repo
      iv_commit   = lv_commit_sha ).

    cl_abap_unit_assert=>assert_equals(
      act = lines( lt_objects )
      exp = 3
      msg = 'Commit tree and blob are reachable' ).

    lt_expanded = zcl_abapgit_git_porcelain=>full_tree(
      it_objects = lt_objects
      iv_parent  = lv_commit_sha ).
    READ TABLE lt_expanded TRANSPORTING NO FIELDS WITH KEY path_name COMPONENTS path = '/' name = 'hello.txt'.
    cl_abap_unit_assert=>assert_subrc( msg = 'Reconstituted objects are full_tree-safe' ).
  ENDMETHOD.
  METHOD reachable_objects_missing_tree.
    DATA ls_commit TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lv_commit_data TYPE xstring.
    DATA lv_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    ls_commit-tree = '3333333333333333333333333333333333333333'.
    ls_commit-author = 'Test <test@example.com> 0 +0000'.
    ls_commit-committer = 'Test <test@example.com> 0 +0000'.
    ls_commit-body = 'missing tree'.
    lv_commit_data = zcl_abapgit_git_pack=>encode_commit( ls_commit ).
    lv_commit_sha = zcl_abapgit_hash=>sha1_commit( lv_commit_data ).

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_commit_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-commit
      iv_data     = lv_commit_data ).

    TRY.
        zcl_abapgit_ortec_obj_store=>get_reachable_objects(
          iv_repo_key = mc_repo
          iv_commit   = lv_commit_sha ).
        cl_abap_unit_assert=>fail( 'Missing reachable tree must raise' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.
  METHOD missing_sha1s_none.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_missing TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv TYPE xstring.
    lv = '31'.

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = '4444444444444444444444444444444444444444'
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv ).

    APPEND '4444444444444444444444444444444444444444' TO lt_sha1s.

    lt_missing = zcl_abapgit_ortec_obj_store=>get_missing_sha1s(
      iv_repo_key = mc_repo
      it_sha1s    = lt_sha1s ).

    cl_abap_unit_assert=>assert_initial(
      act = lt_missing
      msg = 'A fully-stored SHA1 must not be reported missing' ).
  ENDMETHOD.
  METHOD missing_sha1s_some.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_missing TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv TYPE xstring.
    lv = '31'.

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = '5555555555555555555555555555555555555555'
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv ).

    APPEND '5555555555555555555555555555555555555555' TO lt_sha1s.
    APPEND '6666666666666666666666666666666666666666' TO lt_sha1s.

    lt_missing = zcl_abapgit_ortec_obj_store=>get_missing_sha1s(
      iv_repo_key = mc_repo
      it_sha1s    = lt_sha1s ).

    cl_abap_unit_assert=>assert_equals(
      act = lines( lt_missing )
      exp = 1
      msg = 'Only the non-stored SHA1 should be reported missing' ).
    READ TABLE lt_missing WITH KEY table_line = '6666666666666666666666666666666666666666'
      TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( msg = 'The specific missing SHA1 must be in the result' ).
  ENDMETHOD.
  METHOD object_state_constants.
    " The boolean found/not-found model is deliberately abolished (target
    " design section 2): every object/path resolution must land on exactly
    " one of six explicit states. This locks the six string values so a
    " future typo/rename cannot silently change what callers compare
    " against.
    cl_abap_unit_assert=>assert_equals( act = zcl_abapgit_ortec_obj_store=>cs_object_state-loaded
      exp = 'LOADED' ).
    cl_abap_unit_assert=>assert_equals( act = zcl_abapgit_ortec_obj_store=>cs_object_state-indexed_needs_load
      exp = 'INDEXED_NEEDS_LOAD' ).
    cl_abap_unit_assert=>assert_equals( act = zcl_abapgit_ortec_obj_store=>cs_object_state-not_buffered
      exp = 'NOT_BUFFERED' ).
    cl_abap_unit_assert=>assert_equals( act = zcl_abapgit_ortec_obj_store=>cs_object_state-unknown_needs_fetch
      exp = 'UNKNOWN_NEEDS_FETCH' ).
    cl_abap_unit_assert=>assert_equals( act = zcl_abapgit_ortec_obj_store=>cs_object_state-confirmed_absent
      exp = 'CONFIRMED_ABSENT' ).
    cl_abap_unit_assert=>assert_equals( act = zcl_abapgit_ortec_obj_store=>cs_object_state-corrupt_or_incomplete
      exp = 'CORRUPT_OR_INCOMPLETE' ).
  ENDMETHOD.
ENDCLASS.

CLASS ltcl_missing_objects DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_MISOB'.
    METHODS setup. METHODS teardown.
    METHODS noop_when_nothing_missing FOR TESTING RAISING cx_static_check.
    METHODS no_fetch_without_url FOR TESTING RAISING cx_static_check.
    METHODS no_fetch_when_opt_in_off FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_missing_objects IMPLEMENTATION.
  METHOD setup. DELETE FROM zaog_obj_store WHERE repo_key = mc_repo. ENDMETHOD.
  METHOD teardown. DELETE FROM zaog_obj_store WHERE repo_key = mc_repo. ROLLBACK WORK. ENDMETHOD.
  METHOD noop_when_nothing_missing.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv TYPE xstring.
    lv = '31'.

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = '7777777777777777777777777777777777777777'
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv ).
    APPEND '7777777777777777777777777777777777777777' TO lt_sha1s.

    " Everything is already buffered, so this must return without ever
    " attempting a network call (a blank/unreachable URL would fail loudly
    " if a fetch were attempted).
    zcl_abapgit_ortec_missing_obj=>ensure_available(
      iv_repo_key = mc_repo
      iv_url      = 'https://example.invalid/not-a-real-remote.git'
      iv_commit   = '8888888888888888888888888888888888888888'
      it_sha1s    = lt_sha1s ).
  ENDMETHOD.
  METHOD no_fetch_without_url.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND '9999999999999999999999999999999999999999' TO lt_sha1s.

    " Object is not buffered and no URL is supplied - must raise immediately
    " without attempting any network access.
    TRY.
        zcl_abapgit_ortec_missing_obj=>ensure_available(
          iv_repo_key = mc_repo
          iv_url      = ''
          iv_commit   = '8888888888888888888888888888888888888888'
          it_sha1s    = lt_sha1s ).
        cl_abap_unit_assert=>fail( 'Missing object without a URL must raise' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.
  METHOD no_fetch_when_opt_in_off.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' TO lt_sha1s.

    " No repository has opted into the ORTEC write/protocol behavior for this
    " URL, so ensure_available must refuse to fetch and raise rather than
    " attempt a non-negotiated network call.
    TRY.
        zcl_abapgit_ortec_missing_obj=>ensure_available(
          iv_repo_key = mc_repo
          iv_url      = 'https://example.invalid/opt-in-off-repo.git'
          iv_commit   = '8888888888888888888888888888888888888888'
          it_sha1s    = lt_sha1s ).
        cl_abap_unit_assert=>fail( 'Missing object with opt-in inactive must raise' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.
ENDCLASS.

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

CLASS ltcl_ofs_delta DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_OFSDLT'.
    METHODS offset_single_byte    FOR TESTING RAISING cx_static_check.
    METHODS offset_multi_byte     FOR TESTING RAISING cx_static_check.
    METHODS apply_copy_and_insert FOR TESTING RAISING cx_static_check.
    METHODS resolve_ofs_direct    FOR TESTING RAISING cx_static_check.
    METHODS resolve_ofs_chain     FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_ofs_delta IMPLEMENTATION.

  METHOD offset_single_byte.
    DATA lv_data   TYPE xstring.
    DATA lv_offset TYPE i.

    " Vectors from target_design_phase5.md §1.2 (T1). The +1 continuation
    " bias is the single highest-risk line in the whole OFS_DELTA feature -
    " these vectors pin it exactly.
    lv_data = '00'.
    lv_offset = zcl_abapgit_ortec_delta=>get_offset( CHANGING cv_data = lv_data ).
    cl_abap_unit_assert=>assert_equals( act = lv_offset exp = 0 ).
    cl_abap_unit_assert=>assert_equals( act = xstrlen( lv_data ) exp = 0
      msg = 'A single-byte varint must consume exactly 1 byte' ).

    lv_data = '7F'.
    lv_offset = zcl_abapgit_ortec_delta=>get_offset( CHANGING cv_data = lv_data ).
    cl_abap_unit_assert=>assert_equals( act = lv_offset exp = 127 ).
  ENDMETHOD.

  METHOD offset_multi_byte.
    DATA lv_data   TYPE xstring.
    DATA lv_offset TYPE i.

    " Each vector is followed by a trailer byte 'AA' to prove the decoder
    " stops exactly at the varint boundary and leaves the remainder untouched
    " (important: cv_data must be correctly positioned for the caller to
    " continue parsing the delta instruction stream that follows).
    lv_data = '8000AA'.
    lv_offset = zcl_abapgit_ortec_delta=>get_offset( CHANGING cv_data = lv_data ).
    cl_abap_unit_assert=>assert_equals( act = lv_offset exp = 128 ).
    cl_abap_unit_assert=>assert_equals( act = lv_data exp = 'AA' ).

    lv_data = '807FAA'.
    lv_offset = zcl_abapgit_ortec_delta=>get_offset( CHANGING cv_data = lv_data ).
    cl_abap_unit_assert=>assert_equals( act = lv_offset exp = 255 ).
    cl_abap_unit_assert=>assert_equals( act = lv_data exp = 'AA' ).

    lv_data = '8100AA'.
    lv_offset = zcl_abapgit_ortec_delta=>get_offset( CHANGING cv_data = lv_data ).
    cl_abap_unit_assert=>assert_equals( act = lv_offset exp = 256 ).

    lv_data = 'FF7FAA'.
    lv_offset = zcl_abapgit_ortec_delta=>get_offset( CHANGING cv_data = lv_data ).
    cl_abap_unit_assert=>assert_equals( act = lv_offset exp = 16383 ).

    lv_data = '808000AA'.
    lv_offset = zcl_abapgit_ortec_delta=>get_offset( CHANGING cv_data = lv_data ).
    cl_abap_unit_assert=>assert_equals( act = lv_offset exp = 16512 ).
    cl_abap_unit_assert=>assert_equals( act = lv_data exp = 'AA' ).
  ENDMETHOD.

  METHOD apply_copy_and_insert.
    DATA lv_base   TYPE xstring.
    DATA lv_delta  TYPE xstring.
    DATA lv_result TYPE xstring.

    " base = "Hello". delta = base-size(5) result-size(6)
    " copy-op 0x90 (copy, size-byte0 present, no offset bytes -> offset 0)
    " + size-byte 0x05 (copy length 5) + insert-op 0x01 + literal 0x21 ('!').
    " Expected result = "Hello!" - see target_design_phase5.md §7.2 (T2).
    lv_base  = '48656C6C6F'.
    lv_delta = '050690050121'.

    lv_result = zcl_abapgit_ortec_delta=>apply( iv_base = lv_base iv_delta = lv_delta ).

    cl_abap_unit_assert=>assert_equals( act = lv_result exp = '48656C6C6F21' ).
  ENDMETHOD.

  METHOD resolve_ofs_direct.
    DATA lt_objects     TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object      LIKE LINE OF lt_objects.
    DATA lt_offset_map  TYPE zcl_abapgit_ortec_delta=>ty_offset_map_tt.
    DATA lt_ofs_meta    TYPE zcl_abapgit_ortec_delta=>ty_ofs_meta_tt.
    DATA lv_expected_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    " Object 1: full blob "Hello" at pack_offset 0.
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-data  = '48656C6C6F'.
    ls_object-sha1  = zcl_abapgit_hash=>sha1_blob( ls_object-data ).
    ls_object-index = 1.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 0 obj_index = 1 ) INTO TABLE lt_offset_map.

    " Object 2: OFS_DELTA at pack_offset 20, base_offset 0 (points back to
    " object 1). Same delta bytes as apply_copy_and_insert: "Hello" -> "Hello!".
    CLEAR ls_object.
    ls_object-type  = zcl_abapgit_ortec_delta=>c_type_ofs_d.
    ls_object-data  = '050690050121'.
    ls_object-index = 2.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 20 obj_index = 2 ) INTO TABLE lt_offset_map.
    INSERT VALUE #( obj_index = 2 base_offset = 0 ) INTO TABLE lt_ofs_meta.

    zcl_abapgit_ortec_delta=>resolve_all(
      EXPORTING
        it_offset_map = lt_offset_map
        it_ofs_meta   = lt_ofs_meta
        iv_repo_key   = mc_repo
      CHANGING
        ct_objects    = lt_objects ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 2.
    cl_abap_unit_assert=>assert_subrc( msg = 'Resolved OFS_DELTA object must remain in ct_objects' ).
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '48656C6C6F21'
      msg = 'OFS_DELTA must resolve to the base object located by pack offset' ).
    cl_abap_unit_assert=>assert_equals( act = ls_object-type
      exp = zif_abapgit_git_definitions=>c_type-blob
      msg = 'A resolved delta inherits its type from its ultimate base' ).

    lv_expected_sha = zcl_abapgit_hash=>sha1_blob( '48656C6C6F21' ).
    cl_abap_unit_assert=>assert_equals( act = ls_object-sha1 exp = lv_expected_sha
      msg = 'Resolved object must carry its real recomputed content SHA1' ).
  ENDMETHOD.

  METHOD resolve_ofs_chain.
    DATA lt_objects    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object     LIKE LINE OF lt_objects.
    DATA lt_offset_map TYPE zcl_abapgit_ortec_delta=>ty_offset_map_tt.
    DATA lt_ofs_meta   TYPE zcl_abapgit_ortec_delta=>ty_ofs_meta_tt.
    DATA lv_expected_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    " A 2-hop OFS_DELTA chain: obj1 "Hello" -> obj2 "Hello!" -> obj3 "Hello!!".
    " Proves dependency-ordered resolution: obj3's base (obj2) is itself an
    " unresolved delta at the time obj3 is visited - the case a one-pass
    " offset->sha rewrite would resolve incorrectly (see
    " target_design_phase5.md §2.1 / T4).

    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-data  = '48656C6C6F'. " "Hello"
    ls_object-sha1  = zcl_abapgit_hash=>sha1_blob( ls_object-data ).
    ls_object-index = 1.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 0 obj_index = 1 ) INTO TABLE lt_offset_map.

    CLEAR ls_object.
    ls_object-type  = zcl_abapgit_ortec_delta=>c_type_ofs_d.
    ls_object-data  = '050690050121'. " "Hello" -> "Hello!"
    ls_object-index = 2.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 20 obj_index = 2 ) INTO TABLE lt_offset_map.
    INSERT VALUE #( obj_index = 2 base_offset = 0 ) INTO TABLE lt_ofs_meta.

    CLEAR ls_object.
    ls_object-type  = zcl_abapgit_ortec_delta=>c_type_ofs_d.
    " base-size(6) result-size(7) copy(off=0,len=6) insert(1,'!') -> "Hello!!"
    ls_object-data  = '060790060121'.
    ls_object-index = 3.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 40 obj_index = 3 ) INTO TABLE lt_offset_map.
    INSERT VALUE #( obj_index = 3 base_offset = 20 ) INTO TABLE lt_ofs_meta.

    zcl_abapgit_ortec_delta=>resolve_all(
      EXPORTING
        it_offset_map = lt_offset_map
        it_ofs_meta   = lt_ofs_meta
        iv_repo_key   = mc_repo
      CHANGING
        ct_objects    = lt_objects ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 2.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '48656C6C6F21'
      msg = 'First hop of the chain must resolve to "Hello!"' ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 3.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '48656C6C6F2121'
      msg = 'Second hop of the chain must resolve to "Hello!!", proving the ' &&
            'base (object 2) was resolved before object 3 applied its delta' ).

    lv_expected_sha = zcl_abapgit_hash=>sha1_blob( '48656C6C6F2121' ).
    cl_abap_unit_assert=>assert_equals( act = ls_object-sha1 exp = lv_expected_sha ).
  ENDMETHOD.

ENDCLASS.

CLASS ltcl_switch DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    METHODS no_dump FOR TESTING.
    METHODS absent_strictness_default FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_switch IMPLEMENTATION.
  METHOD no_dump. DATA lv TYPE abap_bool. lv = zcl_abapgit_ortec_git_switch=>is_active_for_repo( 'https://dummy.test/repo.git' ). ENDMETHOD.
  METHOD absent_strictness_default.
    " D4: STRICT must be the shipping default. RELAXED exists only to
    " benchmark the completeness-check cost and must never ship as default.
    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_git_switch=>cs_absent_strictness-mode
      exp = zcl_abapgit_ortec_git_switch=>cs_absent_strictness-mode_strict ).
  ENDMETHOD.
ENDCLASS.

CLASS ltcl_repo_state DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_url TYPE string VALUE 'https://test.example.com/repo.git'.
    METHODS setup. METHODS teardown.
    METHODS get_or_create_key        FOR TESTING RAISING cx_static_check.
    METHODS get_or_create_idempotent FOR TESTING RAISING cx_static_check.
    METHODS state_roundtrip          FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_repo_state IMPLEMENTATION.
  METHOD setup.
    DATA lv_key TYPE c LENGTH 12.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    IF lv_key IS NOT INITIAL. zcl_abapgit_ortec_repo_state=>clear_state( lv_key ). ENDIF.
  ENDMETHOD.
  METHOD teardown.
    DATA lv_key TYPE c LENGTH 12.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    IF lv_key IS NOT INITIAL.
      zcl_abapgit_ortec_repo_state=>clear_state( lv_key ).
      DELETE FROM zaog_obj_store WHERE repo_key = lv_key.
    ENDIF. ROLLBACK WORK.
  ENDMETHOD.
  METHOD get_or_create_key.
    DATA lv_key TYPE c LENGTH 12.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    cl_abap_unit_assert=>assert_not_initial( act = lv_key msg = 'Key must be generated' ).
  ENDMETHOD.
  METHOD get_or_create_idempotent.
    DATA lv1 TYPE c LENGTH 12. DATA lv2 TYPE c LENGTH 12.
    lv1 = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    lv2 = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    cl_abap_unit_assert=>assert_equals( act = lv1 exp = lv2 msg = 'Must be idempotent' ).
  ENDMETHOD.
  METHOD state_roundtrip.
    DATA lv_key TYPE c LENGTH 12. DATA ls_state TYPE zcl_abapgit_ortec_repo_state=>ty_state.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    zcl_abapgit_ortec_repo_state=>update_after_fetch( iv_repo_key = lv_key
      iv_branch_name = 'refs/heads/main' iv_url = mc_url
      iv_commit = 'aabbccddee00112233445566778899aabbccddee' ).
    DATA lv_found TYPE c LENGTH 12.
    lv_found = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( mc_url ).
    cl_abap_unit_assert=>assert_equals( act = lv_found exp = lv_key msg = 'DB lookup should find key' ).
    ls_state = zcl_abapgit_ortec_repo_state=>get_state( iv_repo_key = lv_key iv_branch_name = 'refs/heads/main' ).
    cl_abap_unit_assert=>assert_equals( act = ls_state-fetch_commit
      exp = 'aabbccddee00112233445566778899aabbccddee' msg = 'Commit must match' ).
  ENDMETHOD.
ENDCLASS.

CLASS ltcl_persist_flow DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_url TYPE string VALUE 'https://test-persist.example.com/repo.git'.
    METHODS setup. METHODS teardown.
    METHODS persist_creates_state FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_persist_flow IMPLEMENTATION.
  METHOD setup.
    DATA lv_key TYPE c LENGTH 12.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    IF lv_key IS NOT INITIAL.
      zcl_abapgit_ortec_repo_state=>clear_state( lv_key ).
      DELETE FROM zaog_obj_store WHERE repo_key = lv_key.
    ENDIF.
  ENDMETHOD.
  METHOD teardown.
    DATA lv_key TYPE c LENGTH 12.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    IF lv_key IS NOT INITIAL.
      zcl_abapgit_ortec_repo_state=>clear_state( lv_key ).
      DELETE FROM zaog_obj_store WHERE repo_key = lv_key.
    ENDIF. ROLLBACK WORK.
  ENDMETHOD.
  METHOD persist_creates_state.
    DATA lt_obj TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_key TYPE c LENGTH 12.
    ls_obj-sha1 = 'aabbccddee00112233445566778899aabbccddee'. ls_obj-type = 'commit'. ls_obj-data = '436F6D6D6974'. APPEND ls_obj TO lt_obj.
    ls_obj-sha1 = '1122334455667788990011223344556677889900'. ls_obj-type = 'blob'. ls_obj-data = '48656C6C6F'. APPEND ls_obj TO lt_obj.
    TRY.
        zcl_abapgit_ortec_fastpath=>persist_pull_result( iv_url = mc_url iv_branch_name = 'refs/heads/main'
          iv_commit = 'aabbccddee00112233445566778899aabbccddee' it_objects = lt_obj ).
      CATCH zcx_abapgit_ortec_git. RETURN.
    ENDTRY.
    lv_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( mc_url ).
    IF lv_key IS INITIAL. RETURN. ENDIF.
    cl_abap_unit_assert=>assert_equals( exp = abap_true msg = 'Commit stored'
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = lv_key iv_sha1 = 'aabbccddee00112233445566778899aabbccddee' ) ).
    cl_abap_unit_assert=>assert_equals( exp = abap_true msg = 'Blob stored'
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = lv_key iv_sha1 = '1122334455667788990011223344556677889900' ) ).
    DATA ls_state TYPE zcl_abapgit_ortec_repo_state=>ty_state.
    ls_state = zcl_abapgit_ortec_repo_state=>get_state( iv_repo_key = lv_key iv_branch_name = 'refs/heads/main' ).
    cl_abap_unit_assert=>assert_equals( act = ls_state-fetch_commit
      exp = 'aabbccddee00112233445566778899aabbccddee' msg = 'State commit must match' ).
  ENDMETHOD.
ENDCLASS.

CLASS ltcl_pack_index DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOG_TEST_IX'.
    CONSTANTS mc_pack TYPE c LENGTH 32 VALUE 'TESTPACK00000000000000000000001A'.
    METHODS setup. METHODS teardown.
    METHODS store_and_get FOR TESTING RAISING cx_static_check.
    METHODS mark_decoded  FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_pack_index IMPLEMENTATION.
  METHOD setup. zcl_abapgit_ortec_pack_index=>cleanup_repo( mc_repo ). ENDMETHOD.
  METHOD teardown. zcl_abapgit_ortec_pack_index=>cleanup_repo( mc_repo ). ROLLBACK WORK. ENDMETHOD.
  METHOD store_and_get.
    DATA lt_e TYPE zcl_abapgit_ortec_pack_index=>tty_index_entries. DATA ls_e TYPE zcl_abapgit_ortec_pack_index=>ty_index_entry.
    ls_e-obj_index = 1. ls_e-obj_sha1 = 'aa11223344556677889900aabbccddeeff001122'. ls_e-obj_type = 'blob'. ls_e-uncomp_len = 100. ls_e-dec_status = 'P'. APPEND ls_e TO lt_e.
    ls_e-obj_index = 2. ls_e-obj_sha1 = 'bb11223344556677889900aabbccddeeff001122'. ls_e-obj_type = 'commit'. ls_e-dec_status = 'D'. APPEND ls_e TO lt_e.
    zcl_abapgit_ortec_pack_index=>store_entries( iv_repo_key = mc_repo iv_pack_id = mc_pack it_entries = lt_e ).
    DATA lt_p TYPE zcl_abapgit_ortec_pack_index=>tty_index_entries.
    lt_p = zcl_abapgit_ortec_pack_index=>get_pending( iv_repo_key = mc_repo iv_pack_id = mc_pack ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_p ) exp = 1 msg = 'Only 1 pending' ).
  ENDMETHOD.
  METHOD mark_decoded.
    DATA lt_e TYPE zcl_abapgit_ortec_pack_index=>tty_index_entries. DATA ls_e TYPE zcl_abapgit_ortec_pack_index=>ty_index_entry.
    ls_e-obj_index = 1. ls_e-obj_type = 'blob'. ls_e-dec_status = 'P'. APPEND ls_e TO lt_e.
    zcl_abapgit_ortec_pack_index=>store_entries( iv_repo_key = mc_repo iv_pack_id = mc_pack it_entries = lt_e ).
    zcl_abapgit_ortec_pack_index=>mark_decoded( iv_repo_key = mc_repo iv_pack_id = mc_pack iv_obj_index = 1 iv_obj_sha1 = 'cc11223344556677889900aabbccddeeff001122' ).
    DATA lt_p TYPE zcl_abapgit_ortec_pack_index=>tty_index_entries.
    lt_p = zcl_abapgit_ortec_pack_index=>get_pending( iv_repo_key = mc_repo iv_pack_id = mc_pack ).
    cl_abap_unit_assert=>assert_initial( act = lt_p msg = 'No pending after mark' ).
  ENDMETHOD.
ENDCLASS.

"! Tests for {@link ZCL_ABAPGIT_ORTEC_PACK_DEC}.
"! Verifies the full decode_and_persist and crash-resume flow.
"! All data is isolated by MC_REPO. Tests access real DB tables
"! (no SQL test doubles needed; cleanup is done in setup/teardown).
CLASS ltcl_pack_decoder DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    "! Repo key used to isolate all test DB writes
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOG_TEST_PD'.
    "! Fixed pack ID used when simulating a crashed session
    CONSTANTS mc_pack TYPE c LENGTH 32 VALUE 'TESTPACK00000000000000000000001A'.
    METHODS setup.
    METHODS teardown.
    "! decode_and_persist with pre-decoded objects: verifies fast path writes
    "! all five tables and cleans up raw_pack on success.
    METHODS decode_populates_all FOR TESTING RAISING cx_static_check.
    "! decode_and_persist without pre-decoded objects: exercises resumable_decode
    "! (full decompression) and verifies the same post-conditions.
    METHODS decode_from_pack     FOR TESTING RAISING cx_static_check.
    "! resume_decode when active session + raw_pack exist: re-decodes and
    "! persists all objects, marks session complete, removes raw_pack.
    METHODS resume_after_partial FOR TESTING RAISING cx_static_check.
    "! resume_decode with no active session: must return empty, no side effects.
    METHODS resume_no_session    FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_pack_decoder IMPLEMENTATION.
  METHOD setup.
    DELETE FROM zaog_obj_store  WHERE repo_key = mc_repo.
    DELETE FROM zaog_pack_meta  WHERE repo_key = mc_repo.
    DELETE FROM zaog_pack_idx   WHERE repo_key = mc_repo.
    DELETE FROM zaog_fetch_sess WHERE repo_key = mc_repo.
    DELETE FROM zaog_raw_pack   WHERE repo_key = mc_repo.
  ENDMETHOD.
  METHOD teardown.
    DELETE FROM zaog_obj_store  WHERE repo_key = mc_repo.
    DELETE FROM zaog_pack_meta  WHERE repo_key = mc_repo.
    DELETE FROM zaog_pack_idx   WHERE repo_key = mc_repo.
    DELETE FROM zaog_fetch_sess WHERE repo_key = mc_repo.
    DELETE FROM zaog_raw_pack   WHERE repo_key = mc_repo.
    ROLLBACK WORK.
  ENDMETHOD.

  METHOD decode_populates_all.
    DATA lt_obj  TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj  TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_pack TYPE xstring.
    ls_obj-sha1 = 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d'.
    ls_obj-type = 'blob'. ls_obj-data = '48656C6C6F'. ls_obj-index = 1.
    APPEND ls_obj TO lt_obj.
    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).
    DATA lt_res TYPE zif_abapgit_definitions=>ty_objects_tt.
    lt_res = zcl_abapgit_ortec_pack_dec=>decode_and_persist(
      iv_data    = lv_pack
      iv_repo_key = mc_repo
      it_objects  = lt_obj ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_res ) exp = 1 msg = '1 object returned' ).
    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = mc_repo iv_sha1 = 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d' )
      msg = 'Object must be in obj_store' ).
    DATA ls_meta TYPE zaog_pack_meta.
    SELECT SINGLE * FROM zaog_pack_meta INTO ls_meta WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_subrc( msg = 'pack_meta must exist' ).
    cl_abap_unit_assert=>assert_equals( act = ls_meta-status exp = 'C' msg = 'pack_meta complete' ).
    DATA lv_idx TYPE i.
    SELECT COUNT(*) FROM zaog_pack_idx INTO lv_idx WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_idx exp = 1 msg = 'pack_idx has 1 entry' ).
    DATA ls_sess TYPE zaog_fetch_sess.
    SELECT SINGLE * FROM zaog_fetch_sess INTO ls_sess WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_subrc( msg = 'fetch_sess must exist' ).
    cl_abap_unit_assert=>assert_equals( act = ls_sess-status exp = 'C' msg = 'session complete' ).
    DATA lv_raw TYPE i.
    SELECT COUNT(*) FROM zaog_raw_pack INTO lv_raw WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_raw exp = 0 msg = 'raw_pack cleaned up' ).
  ENDMETHOD.

  METHOD decode_from_pack.
    " it_objects intentionally NOT supplied: exercises resumable_decode.
    DATA lt_obj  TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj  TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_pack TYPE xstring.
    DATA lt_res  TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_sha1 TYPE zif_abapgit_git_definitions=>ty_sha1.
    ls_obj-data  = '48656C6C6F'. " ASCII: Hello
    ls_obj-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_obj-sha1  = zcl_abapgit_hash=>sha1( iv_type = ls_obj-type iv_data = ls_obj-data ).
    ls_obj-index = 1. APPEND ls_obj TO lt_obj.
    lv_sha1 = ls_obj-sha1.
    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).
    lt_res = zcl_abapgit_ortec_pack_dec=>decode_and_persist(
      iv_data     = lv_pack
      iv_repo_key = mc_repo ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_res ) exp = 1 msg = '1 object decoded from pack' ).
    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = mc_repo iv_sha1 = lv_sha1 )
      msg = 'Decoded object in obj_store' ).
    DATA ls_sess TYPE zaog_fetch_sess.
    SELECT SINGLE * FROM zaog_fetch_sess INTO ls_sess WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_subrc( msg = 'Session exists' ).
    cl_abap_unit_assert=>assert_equals( act = ls_sess-status exp = 'C' msg = 'Session complete' ).
    DATA lv_raw TYPE i.
    SELECT COUNT(*) FROM zaog_raw_pack INTO lv_raw WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_raw exp = 0 msg = 'raw_pack cleaned up after decode' ).
  ENDMETHOD.

  METHOD resume_after_partial.
    " Simulate crash: raw_pack stored + active session, nothing in obj_store yet.
    DATA lt_obj  TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj  TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_pack TYPE xstring.
    DATA ls_raw  TYPE zaog_raw_pack.
    DATA ls_sess TYPE zaog_fetch_sess.
    DATA lt_res  TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_sha1 TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_ts   TYPE timestampl.
    ls_obj-data  = '48656C6C6F'. " ASCII: Hello
    ls_obj-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_obj-sha1  = zcl_abapgit_hash=>sha1( iv_type = ls_obj-type iv_data = ls_obj-data ).
    ls_obj-index = 1. APPEND ls_obj TO lt_obj.
    lv_sha1 = ls_obj-sha1.
    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).
    " Insert crash-state into DB
    ls_raw-repo_key = mc_repo. ls_raw-pack_id = mc_pack. ls_raw-raw_data = lv_pack.
    MODIFY zaog_raw_pack FROM ls_raw.
    GET TIME STAMP FIELD lv_ts.
    ls_sess-session_id = 'RESSESTEST000000000000000000001A'.
    ls_sess-repo_key   = mc_repo. ls_sess-pack_id    = mc_pack.
    ls_sess-phase      = 'D'.     ls_sess-obj_done   = 0. ls_sess-obj_total = 1.
    ls_sess-status     = 'A'.     ls_sess-created_at = lv_ts. ls_sess-updated_at = lv_ts.
    INSERT zaog_fetch_sess FROM ls_sess.
    COMMIT WORK.
    " Resume
    lt_res = zcl_abapgit_ortec_pack_dec=>resume_decode( mc_repo ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_res ) exp = 1 msg = '1 object after resume' ).
    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = mc_repo iv_sha1 = lv_sha1 )
      msg = 'Object in obj_store after resume' ).
    SELECT SINGLE * FROM zaog_fetch_sess INTO ls_sess WHERE repo_key = mc_repo AND status = 'C'.
    cl_abap_unit_assert=>assert_subrc( msg = 'Session must be complete after resume' ).
    DATA lv_raw TYPE i.
    SELECT COUNT(*) FROM zaog_raw_pack INTO lv_raw WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_raw exp = 0 msg = 'raw_pack cleaned up after resume' ).
  ENDMETHOD.

  METHOD resume_no_session.
    DATA lt_res TYPE zif_abapgit_definitions=>ty_objects_tt.
    lt_res = zcl_abapgit_ortec_pack_dec=>resume_decode( mc_repo ).
    cl_abap_unit_assert=>assert_initial( act = lt_res msg = 'No session = empty result' ).
  ENDMETHOD.
ENDCLASS.

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

CLASS ltcl_git_roundtrip DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION. METHODS encode_decode FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_git_roundtrip IMPLEMENTATION.
  METHOD encode_decode.
    DATA lt_obj TYPE zif_abapgit_definitions=>ty_objects_tt. DATA ls_obj TYPE zif_abapgit_definitions=>ty_object.
    ls_obj-sha1 = zcl_abapgit_hash=>sha1_blob( CONV xstring( '48656C6C6F' ) ).
    ls_obj-type = 'blob'. ls_obj-data = '48656C6C6F'. ls_obj-index = 1. APPEND ls_obj TO lt_obj.
    DATA lv_pack TYPE xstring.
    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).
    cl_abap_unit_assert=>assert_not_initial( act = lv_pack ).
    DATA lt_dec TYPE zif_abapgit_definitions=>ty_objects_tt.
    lt_dec = zcl_abapgit_git_pack=>decode( lv_pack ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_dec ) exp = 1 ).
    cl_abap_unit_assert=>assert_equals( act = lt_dec[ 1 ]-sha1 exp = ls_obj-sha1 ).
    cl_abap_unit_assert=>assert_equals( act = lt_dec[ 1 ]-data exp = '48656C6C6F' ).
  ENDMETHOD.
ENDCLASS.
