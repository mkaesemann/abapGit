CLASS ltcl_obj_store DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOG_TEST_01'.
    METHODS setup. METHODS teardown.
    METHODS store_and_get FOR TESTING RAISING cx_static_check.
    METHODS not_found FOR TESTING RAISING cx_static_check.
    METHODS get_objects_bulk FOR TESTING RAISING cx_static_check.
    METHODS get_objects_missing FOR TESTING RAISING cx_static_check.
    METHODS available_ignores_missing FOR TESTING RAISING cx_static_check.
    METHODS available_deduplicates FOR TESTING RAISING cx_static_check.
    METHODS reachable_objects_graph FOR TESTING RAISING cx_static_check.
    METHODS reachable_objects_missing_tree FOR TESTING RAISING cx_static_check.
    METHODS reachable_sha1s_graph FOR TESTING RAISING cx_static_check.
    METHODS reachable_sha1s_missing_blob FOR TESTING RAISING cx_static_check.
    METHODS verify_closure_ok FOR TESTING RAISING cx_static_check.
    METHODS verify_closure_missing_tree FOR TESTING RAISING cx_static_check.
    METHODS verify_closure_missing_blob_ok FOR TESTING RAISING cx_static_check.
    METHODS missing_sha1s_none FOR TESTING RAISING cx_static_check.
    METHODS missing_sha1s_some FOR TESTING RAISING cx_static_check.
    METHODS object_state_constants FOR TESTING RAISING cx_static_check.
    METHODS active_repo_key_fallback FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_obj_store IMPLEMENTATION.
  METHOD setup. DELETE FROM zaog_obj_store WHERE repo_key = mc_repo. ENDMETHOD.
  METHOD teardown.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    ROLLBACK WORK.  "#EC CI_ROLLBACK
    " mv_cache_repo_key is CLASS-DATA (session-global) - reset it so a
    " set_active_repo_key( mc_repo ) call in one test cannot leak into an
    " unrelated test that relies on a blank/uninitialized active repo.
    zcl_abapgit_ortec_obj_store=>invalidate_cache( ).
  ENDMETHOD.
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
  METHOD available_ignores_missing.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_data TYPE xstring VALUE '415641494C41424C45'.
    CONSTANTS lc_present TYPE zif_abapgit_git_definitions=>ty_sha1
      VALUE '3333333333333333333333333333333333333333'.
    CONSTANTS lc_missing TYPE zif_abapgit_git_definitions=>ty_sha1
      VALUE 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'.

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lc_present
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_data ).

    APPEND lc_present TO lt_sha1s.
    APPEND lc_missing TO lt_sha1s.

    lt_objects = zcl_abapgit_ortec_obj_store=>get_available_objects(
      iv_repo_key = mc_repo
      it_sha1s    = lt_sha1s ).

    cl_abap_unit_assert=>assert_equals(
      act = lines( lt_objects )
      exp = 1
      msg = 'Missing candidates are ignored rather than raised' ).

    cl_abap_unit_assert=>assert_equals(
      act = lt_objects[ 1 ]-sha1
      exp = lc_present ).
  ENDMETHOD.

  METHOD available_deduplicates.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_data TYPE xstring VALUE '4445445550'.
    CONSTANTS lc_present TYPE zif_abapgit_git_definitions=>ty_sha1
      VALUE '4444444444444444444444444444444444444444'.

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lc_present
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_data ).

    APPEND lc_present TO lt_sha1s.
    APPEND lc_present TO lt_sha1s.
    APPEND lc_present TO lt_sha1s.

    lt_objects = zcl_abapgit_ortec_obj_store=>get_available_objects(
      iv_repo_key = mc_repo
      it_sha1s    = lt_sha1s ).

    cl_abap_unit_assert=>assert_equals(
      act = lines( lt_objects )
      exp = 1
      msg = 'Duplicate candidate SHA1 values are returned once' ).
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
  METHOD reachable_sha1s_graph.
    " get_reachable_sha1s must return the exact same commit+tree+blob SHA1
    " set as get_reachable_objects, without ever reading blob DATA - this
    " test only verifies the identity set is correct (that blob content is
    " never touched is verified by construction: this method contains no
    " get_objects call for blob SHA1s at all, only get_present_sha1s).
    DATA lt_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node LIKE LINE OF lt_nodes.
    DATA ls_commit TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
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

    lt_sha1s = zcl_abapgit_ortec_obj_store=>get_reachable_sha1s(
      iv_repo_key = mc_repo
      iv_commit   = lv_commit_sha ).

    cl_abap_unit_assert=>assert_equals(
      act = lines( lt_sha1s )
      exp = 3
      msg = 'Commit, tree and blob SHA1s are all reachable' ).
    READ TABLE lt_sha1s TRANSPORTING NO FIELDS WITH KEY table_line = lv_commit_sha.
    cl_abap_unit_assert=>assert_subrc( msg = 'Commit SHA1 present' ).
    READ TABLE lt_sha1s TRANSPORTING NO FIELDS WITH KEY table_line = lv_tree_sha.
    cl_abap_unit_assert=>assert_subrc( msg = 'Tree SHA1 present' ).
    READ TABLE lt_sha1s TRANSPORTING NO FIELDS WITH KEY table_line = lv_blob_sha.
    cl_abap_unit_assert=>assert_subrc( msg = 'Blob SHA1 present (proven via existence check, not content read)' ).
  ENDMETHOD.
  METHOD reachable_sha1s_missing_blob.
    " Regression for the SYSTEM_NO_ROLL memory fix: get_reachable_sha1s
    " proves blob presence via get_present_sha1s (SHA1-only) instead of
    " get_objects (which would fetch and require full blob DATA) - this
    " test pins that a genuinely missing blob is still correctly detected
    " and raises, exactly like get_reachable_objects would.
    DATA lt_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node LIKE LINE OF lt_nodes.
    DATA ls_commit TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lv_tree_data TYPE xstring.
    DATA lv_tree_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_commit_data TYPE xstring.
    DATA lv_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    " Blob deliberately never stored - only referenced by the tree.
    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_node-name  = 'missing.txt'.
    ls_node-sha1  = '5555555555555555555555555555555555555555'.
    APPEND ls_node TO lt_nodes.

    lv_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    lv_tree_sha = zcl_abapgit_hash=>sha1_tree( lv_tree_data ).

    ls_commit-tree = lv_tree_sha.
    ls_commit-author = 'Test <test@example.com> 0 +0000'.
    ls_commit-committer = 'Test <test@example.com> 0 +0000'.
    ls_commit-body = 'missing blob'.
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

    TRY.
        zcl_abapgit_ortec_obj_store=>get_reachable_sha1s(
          iv_repo_key = mc_repo
          iv_commit   = lv_commit_sha ).
        cl_abap_unit_assert=>fail( 'Missing reachable blob must raise' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.
  METHOD verify_closure_ok.
    " Package B design §6: verify_tree_closure must succeed for a fully
    " present commit+tree closure regardless of whether the referenced
    " blob is stored - it is deliberately blob-blind.
    DATA lt_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node LIKE LINE OF lt_nodes.
    DATA ls_commit TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lv_tree_data TYPE xstring.
    DATA lv_tree_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_commit_data TYPE xstring.
    DATA lv_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_node-name  = 'hello.txt'.
    ls_node-sha1  = '48656C6C6F48656C6C6F48656C6C6F48656C6C6F'.
    APPEND ls_node TO lt_nodes.

    lv_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    lv_tree_sha = zcl_abapgit_hash=>sha1_tree( lv_tree_data ).

    ls_commit-tree = lv_tree_sha.
    ls_commit-author = 'Test <test@example.com> 0 +0000'.
    ls_commit-committer = 'Test <test@example.com> 0 +0000'.
    ls_commit-body = 'closure ok'.
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

    " Blob deliberately never stored - must not matter.
    zcl_abapgit_ortec_obj_store=>verify_tree_closure(
      iv_repo_key = mc_repo
      iv_commit   = lv_commit_sha ).
  ENDMETHOD.
  METHOD verify_closure_missing_tree.
    DATA ls_commit TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lv_commit_data TYPE xstring.
    DATA lv_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    ls_commit-tree = '7777777777777777777777777777777777777777'.
    ls_commit-author = 'Test <test@example.com> 0 +0000'.
    ls_commit-committer = 'Test <test@example.com> 0 +0000'.
    ls_commit-body = 'closure missing tree'.
    lv_commit_data = zcl_abapgit_git_pack=>encode_commit( ls_commit ).
    lv_commit_sha = zcl_abapgit_hash=>sha1_commit( lv_commit_data ).

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_commit_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-commit
      iv_data     = lv_commit_data ).

    TRY.
        zcl_abapgit_ortec_obj_store=>verify_tree_closure(
          iv_repo_key = mc_repo
          iv_commit   = lv_commit_sha ).
        cl_abap_unit_assert=>fail( 'Missing reachable tree must raise' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.
  METHOD verify_closure_missing_blob_ok.
    " Regression pin: unlike get_reachable_sha1s, a missing blob referenced
    " by an otherwise-complete tree must NOT raise - this is the entire
    " point of verify_tree_closure existing separately (Package B design
    " §6): filtered/blobless historical blobs are promised, not corrupt.
    DATA lt_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node LIKE LINE OF lt_nodes.
    DATA ls_commit TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lv_tree_data TYPE xstring.
    DATA lv_tree_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_commit_data TYPE xstring.
    DATA lv_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_node-name  = 'missing.txt'.
    ls_node-sha1  = '8888888888888888888888888888888888888888'.
    APPEND ls_node TO lt_nodes.

    lv_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    lv_tree_sha = zcl_abapgit_hash=>sha1_tree( lv_tree_data ).

    ls_commit-tree = lv_tree_sha.
    ls_commit-author = 'Test <test@example.com> 0 +0000'.
    ls_commit-committer = 'Test <test@example.com> 0 +0000'.
    ls_commit-body = 'closure missing blob ok'.
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

    zcl_abapgit_ortec_obj_store=>verify_tree_closure(
      iv_repo_key = mc_repo
      iv_commit   = lv_commit_sha ).
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
  METHOD active_repo_key_fallback.
    " Regression coverage for the ES6 branch-switch incident: zcl_abapgit_git_
    " delta's delta-base fallback calls get_object with a blank iv_repo_key,
    " relying entirely on set_active_repo_key having been called first with
    " the correct repo (there is no repo context in that call chain's own
    " signature). Verify both halves: blank/wrong active key must fail, and
    " the correct one, once set, must resolve.
    DATA lv TYPE xstring.
    DATA ls TYPE zif_abapgit_definitions=>ty_object.
    lv = '48656C6C6F'.
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = '7777777777777777777777777777777777777777'
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv ).

    " No active repo key set (fresh/invalidated cache) - blank iv_repo_key
    " must fail rather than silently guessing.
    TRY.
        zcl_abapgit_ortec_obj_store=>get_object(
          iv_sha1 = '7777777777777777777777777777777777777777' ).
        cl_abap_unit_assert=>fail( 'Blank repo_key must not resolve without an active key' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.

    " A stale, unrelated active key must not leak into this lookup either.
    zcl_abapgit_ortec_obj_store=>set_active_repo_key( 'OTHER_REPO01' ).
    TRY.
        zcl_abapgit_ortec_obj_store=>get_object(
          iv_sha1 = '7777777777777777777777777777777777777777' ).
        cl_abap_unit_assert=>fail( 'A stale, unrelated active repo_key must not resolve this object' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.

    " Once explicitly set to the correct repo, the blank-iv_repo_key fallback
    " must resolve reliably.
    zcl_abapgit_ortec_obj_store=>set_active_repo_key( mc_repo ).
    ls = zcl_abapgit_ortec_obj_store=>get_object(
      iv_sha1 = '7777777777777777777777777777777777777777' ).
    cl_abap_unit_assert=>assert_equals(
      act = ls-sha1
      exp = '7777777777777777777777777777777777777777'
      msg = 'Blank iv_repo_key must resolve via the explicitly set active repo key' ).
  ENDMETHOD.
ENDCLASS.

CLASS ltcl_completeness_gate DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_CMPLT1'.
    METHODS setup. METHODS teardown.
    METHODS has_dangling_delta_base_none  FOR TESTING RAISING cx_static_check.
    METHODS has_dangling_delta_base_found FOR TESTING RAISING cx_static_check.
    "! Slice 2C: is_commit_complete now delegates entirely to
    "! zcl_abapgit_ortec_mat_state=>is_graph_have_eligible (Slice 1's O(1)
    "! certified-graph read) instead of walking zaog_obj_store. A commit is
    "! only have-eligible once explicitly certified via
    "! begin_attempt->mark_graph_complete (or ->mark_full_complete) -
    "! object-store presence alone is no longer sufficient, by design (no
    "! auto-backfill, Slice 1's own guarantee).
    METHODS complete_false_missing_object FOR TESTING RAISING cx_static_check.
    METHODS complete_true_when_ready FOR TESTING RAISING cx_static_check.
    "! Regression: completeness must NOT require the stage-filter index
    "! (zcl_abapgit_ortec_obj_index) to have ever been built for this
    "! commit - that index is only built by a filtered Stage/Diff
    "! resolution, so gating "have" eligibility on it meant a commit
    "! reached via a plain pull/branch-switch could never be offered as a
    "! have even when fully fetched, silently disabling incremental fetch
    "! for every branch that was never filter-staged.
    METHODS complete_true_without_index FOR TESTING RAISING cx_static_check.
    "! A commit with no zaog_commit_hist row at all (never certified) must
    "! be reported not-eligible, even when every reachable object is
    "! physically present in zaog_obj_store - proves the delegation to
    "! is_graph_have_eligible actually gates on certification, not on
    "! object presence.
    METHODS complete_false_uncertified FOR TESTING RAISING cx_static_check.
    "! hist_level = FULL_COMPLETE must also be reported have-eligible (not
    "! just GRAPH_COMPLETE).
    METHODS complete_true_full_complete FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_completeness_gate IMPLEMENTATION.
  METHOD setup.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    DELETE FROM zaog_obj_index WHERE repo_key = mc_repo.
    DELETE FROM zaog_pack_idx WHERE repo_key = mc_repo.
    DELETE FROM zaog_commit_hist WHERE repo_key = mc_repo.
  ENDMETHOD.
  METHOD teardown.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    DELETE FROM zaog_obj_index WHERE repo_key = mc_repo.
    DELETE FROM zaog_pack_idx WHERE repo_key = mc_repo.
    DELETE FROM zaog_commit_hist WHERE repo_key = mc_repo.
    ROLLBACK WORK.
  ENDMETHOD.

  METHOD has_dangling_delta_base_none.
    DATA lt_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node LIKE LINE OF lt_nodes.
    DATA ls_commit TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lv_blob_data TYPE xstring.
    DATA lv_blob_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_tree_data TYPE xstring.
    DATA lv_tree_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_commit_data TYPE xstring.
    DATA lv_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv_dangling TYPE abap_bool.

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

    APPEND lv_commit_sha TO lt_sha1s.
    APPEND lv_tree_sha TO lt_sha1s.
    APPEND lv_blob_sha TO lt_sha1s.

    lv_dangling = zcl_abapgit_ortec_obj_store=>has_dangling_delta_base(
      iv_repo_key = mc_repo
      it_sha1s    = lt_sha1s ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_dangling
      exp = abap_false
      msg = 'Objects without delta-base references must not be dangling' ).
  ENDMETHOD.

  METHOD has_dangling_delta_base_found.
    DATA lv_blob_data TYPE xstring.
    DATA lv_blob_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv_dangling TYPE abap_bool.
    DATA lt_entries TYPE zcl_abapgit_ortec_pack_index=>tty_index_entries.
    DATA ls_entry TYPE zcl_abapgit_ortec_pack_index=>ty_index_entry.

    lv_blob_data = '31'.
    lv_blob_sha = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_blob_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_blob_data ).

    ls_entry-obj_index = 1.
    ls_entry-obj_sha1 = lv_blob_sha.
    ls_entry-obj_type = zif_abapgit_git_definitions=>c_type-blob.
    ls_entry-dec_status = 'D'.
    ls_entry-delta_base = 'ffffffffffffffffffffffffffffffffffffffff'.
    APPEND ls_entry TO lt_entries.

    zcl_abapgit_ortec_pack_index=>store_entries(
      iv_repo_key = mc_repo
      iv_pack_id  = 'CMPLTTESTPACK000000000000000000'
      it_entries  = lt_entries ).

    APPEND lv_blob_sha TO lt_sha1s.

    lv_dangling = zcl_abapgit_ortec_obj_store=>has_dangling_delta_base(
      iv_repo_key = mc_repo
      it_sha1s    = lt_sha1s ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_dangling
      exp = abap_true
      msg = 'Missing recorded delta base must be detected as dangling' ).
  ENDMETHOD.

  METHOD complete_false_missing_object.
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
    DATA lv_complete TYPE abap_bool.

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
      iv_repo_key = mc_repo
      iv_sha1     = lv_commit_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-commit
      iv_data     = lv_commit_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_root_tree_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-tree
      iv_data     = lv_root_tree_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_src_tree_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-tree
      iv_data     = lv_src_tree_data ).
    " Deliberately do NOT store the blob, and deliberately never certify
    " this commit via zcl_abapgit_ortec_mat_state (Slice 2C: is_commit_complete
    " no longer walks zaog_obj_store at all - it delegates to
    " is_graph_have_eligible, which requires an explicit certification row).

    lv_complete = zcl_abapgit_ortec_fetch_neg=>is_commit_complete(
      iv_repo_key = mc_repo
      iv_commit   = lv_commit_sha ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_complete
      exp = abap_false
      msg = 'An uncertified commit must not be considered complete' ).
  ENDMETHOD.

  METHOD complete_false_uncertified.
    DATA lt_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node LIKE LINE OF lt_nodes.
    DATA ls_commit TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lv_blob_data TYPE xstring.
    DATA lv_blob_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_tree_data TYPE xstring.
    DATA lv_tree_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_commit_data TYPE xstring.
    DATA lv_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_complete TYPE abap_bool.

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

    " Every reachable object is fully present - but this commit is
    " deliberately NEVER certified (no begin_attempt/mark_graph_complete
    " call, so zaog_commit_hist has zero rows for it). Proves
    " is_commit_complete now gates on certification, not object presence.
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

    lv_complete = zcl_abapgit_ortec_fetch_neg=>is_commit_complete(
      iv_repo_key = mc_repo
      iv_commit   = lv_commit_sha ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_complete
      exp = abap_false
      msg = 'A fully-present-but-never-certified commit must not be have-eligible ' &&
            '(no auto-backfill from object presence, Slice 1 guarantee)' ).
  ENDMETHOD.

  METHOD complete_true_full_complete.
    DATA lv_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_attempt_id TYPE zcl_abapgit_ortec_mat_state=>ty_attempt_id.
    DATA lv_complete TYPE abap_bool.

    lv_commit_sha = '3333333333333333333333333333333333333333'.

    lv_attempt_id = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = mc_repo
      iv_commit   = lv_commit_sha ).
    zcl_abapgit_ortec_mat_state=>mark_graph_complete(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_sha
      iv_attempt_id = lv_attempt_id ).
    zcl_abapgit_ortec_mat_state=>mark_full_complete(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_sha
      iv_attempt_id = lv_attempt_id ).

    lv_complete = zcl_abapgit_ortec_fetch_neg=>is_commit_complete(
      iv_repo_key = mc_repo
      iv_commit   = lv_commit_sha ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_complete
      exp = abap_true
      msg = 'A FULL_COMPLETE commit must be have-eligible, not just GRAPH_COMPLETE' ).
  ENDMETHOD.

  METHOD complete_true_without_index.
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
    DATA lv_complete TYPE abap_bool.

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
      iv_repo_key = mc_repo
      iv_sha1     = lv_commit_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-commit
      iv_data     = lv_commit_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_root_tree_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-tree
      iv_data     = lv_root_tree_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_src_tree_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-tree
      iv_data     = lv_src_tree_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_blob_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_blob_data ).

    " Deliberately never call zcl_abapgit_ortec_obj_index=>get_files_for_filter
    " for this commit - the stage-filter index is never built, exactly like
    " a plain pull/branch-switch that never went through filtered Stage/Diff.
    " Certify the commit's graph via Slice 1's mat_state API - this is what
    " now governs have-eligibility (Slice 2C), not object-store presence or
    " the stage-filter index.
    DATA(lv_attempt_id) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = mc_repo
      iv_commit   = lv_commit_sha ).
    zcl_abapgit_ortec_mat_state=>mark_graph_complete(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_sha
      iv_attempt_id = lv_attempt_id ).

    lv_complete = zcl_abapgit_ortec_fetch_neg=>is_commit_complete(
      iv_repo_key = mc_repo
      iv_commit   = lv_commit_sha ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_complete
      exp = abap_true
      msg = 'A certified (GRAPH_COMPLETE) commit must be eligible as a have even when its ' &&
            'stage-filter index was never built (e.g. reached via a plain pull)' ).
  ENDMETHOD.

  METHOD complete_true_when_ready.
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
    DATA lv_complete TYPE abap_bool.

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
      iv_repo_key = mc_repo
      iv_sha1     = lv_commit_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-commit
      iv_data     = lv_commit_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_root_tree_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-tree
      iv_data     = lv_root_tree_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_src_tree_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-tree
      iv_data     = lv_src_tree_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_blob_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_blob_data ).

    lo_dot = zcl_abapgit_dot_abapgit=>build_default( ).
    lo_filter = NEW #( it_filter = VALUE #( ( object = 'PROG' obj_name = 'ZPROGRAM' ) ) ).

    lt_files = zcl_abapgit_ortec_obj_index=>get_files_for_filter(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_sha
      ii_obj_filter = lo_filter
      io_dot        = lo_dot
      iv_devclass   = '$PACK' ).

    cl_abap_unit_assert=>assert_equals(
      act = lines( lt_files )
      exp = 1
      msg = 'Filtered index build must resolve one matching file' ).

    " Certify the commit's graph via Slice 1's mat_state API - Slice 2C:
    " is_commit_complete no longer infers completeness from index/object-
    " store state, it delegates entirely to is_graph_have_eligible.
    DATA(lv_attempt_id) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = mc_repo
      iv_commit   = lv_commit_sha ).
    zcl_abapgit_ortec_mat_state=>mark_graph_complete(
      iv_repo_key   = mc_repo
      iv_commit     = lv_commit_sha
      iv_attempt_id = lv_attempt_id ).

    lv_complete = zcl_abapgit_ortec_fetch_neg=>is_commit_complete(
      iv_repo_key = mc_repo
      iv_commit   = lv_commit_sha ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_complete
      exp = abap_true
      msg = 'Index-ready AND certified commit must pass' ).
  ENDMETHOD.
ENDCLASS.
