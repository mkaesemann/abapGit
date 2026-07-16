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
    METHODS reachable_sha1s_graph FOR TESTING RAISING cx_static_check.
    METHODS reachable_sha1s_missing_blob FOR TESTING RAISING cx_static_check.
    METHODS missing_sha1s_none FOR TESTING RAISING cx_static_check.
    METHODS missing_sha1s_some FOR TESTING RAISING cx_static_check.
    METHODS object_state_constants FOR TESTING RAISING cx_static_check.
    METHODS active_repo_key_fallback FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_obj_store IMPLEMENTATION.
  METHOD setup. DELETE FROM zaog_obj_store WHERE repo_key = mc_repo. ENDMETHOD.
  METHOD teardown.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    ROLLBACK WORK.
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

CLASS ltcl_completeness_gate DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_CMPLT1'.
    METHODS setup. METHODS teardown.
    METHODS has_dangling_delta_base_none  FOR TESTING RAISING cx_static_check.
    METHODS has_dangling_delta_base_found FOR TESTING RAISING cx_static_check.
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
ENDCLASS.
CLASS ltcl_completeness_gate IMPLEMENTATION.
  METHOD setup.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    DELETE FROM zaog_obj_index WHERE repo_key = mc_repo.
    DELETE FROM zaog_pack_idx WHERE repo_key = mc_repo.
  ENDMETHOD.
  METHOD teardown.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    DELETE FROM zaog_obj_index WHERE repo_key = mc_repo.
    DELETE FROM zaog_pack_idx WHERE repo_key = mc_repo.
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
    " Deliberately do NOT store the blob - the commit's object graph is
    " genuinely incomplete, which is what completeness must actually catch
    " now that it no longer depends on the unrelated stage-filter index.

    lv_complete = zcl_abapgit_ortec_fetch_neg=>is_commit_complete(
      iv_repo_key = mc_repo
      iv_commit   = lv_commit_sha ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_complete
      exp = abap_false
      msg = 'A commit missing a reachable blob must not be considered complete' ).
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

    lv_complete = zcl_abapgit_ortec_fetch_neg=>is_commit_complete(
      iv_repo_key = mc_repo
      iv_commit   = lv_commit_sha ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_complete
      exp = abap_true
      msg = 'A fully fetched commit must be eligible as a have even when its ' &&
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

    lv_complete = zcl_abapgit_ortec_fetch_neg=>is_commit_complete(
      iv_repo_key = mc_repo
      iv_commit   = lv_commit_sha ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_complete
      exp = abap_true
      msg = 'Index-ready commit with complete reachable objects must pass' ).
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
    " Git's OFS_DELTA varint adds 1 before each shift (offset = (offset+1)<<7 | byte),
    " NOT a naive base-128 concatenation - this makes the encoding canonical/unique.
    " 127 -> (127+1)*128 + 127 = 16511, not the naive-concat value 16383.
    cl_abap_unit_assert=>assert_equals( act = lv_offset exp = 16511 ).

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

CLASS ltcl_ref_delta DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_REFDLT'.
    METHODS setup.
    METHODS teardown.
    METHODS base_after_dependent FOR TESTING RAISING cx_static_check.
    METHODS resolve_after_prior_in_pass FOR TESTING RAISING cx_static_check.
    METHODS two_thin_bases_do_not_collide FOR TESTING RAISING cx_static_check.
    METHODS chain_onto_later_unresolved FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_ref_delta IMPLEMENTATION.

  METHOD setup.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
  ENDMETHOD.

  METHOD teardown.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    ROLLBACK WORK.
  ENDMETHOD.

  METHOD base_after_dependent.
    " REF_DELTA carries no ordering guarantee (unlike OFS_DELTA, which is
    " always positioned strictly backwards in the pack byte stream): the
    " base object CAN legitimately appear AFTER the delta that depends on
    " it. Before the fix, resolve_one's base lookup for object 1 (a ref_d
    " entry whose OWN row also shows the searched-for sha1 as its own
    " unresolved placeholder) would match itself first and recurse forever
    " (spurious "chain exceeds maximum depth"), instead of finding the
    " real, already-resolved base at object 2 (a plain blob, positioned
    " AFTER object 1). This test pins the corrected behavior: the base
    " lookup must skip any still-unresolved ref_d/ofs_d candidate and find
    " the genuinely resolved one regardless of table position.
    DATA lt_objects      TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object       LIKE LINE OF lt_objects.
    DATA lt_offset_map   TYPE zcl_abapgit_ortec_delta=>ty_offset_map_tt.
    DATA lt_ofs_meta     TYPE zcl_abapgit_ortec_delta=>ty_ofs_meta_tt.
    DATA lv_expected_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_base_sha     TYPE zif_abapgit_git_definitions=>ty_sha1.

    " The true base, "Hello!" (6 bytes), is placed at index 2 - AFTER the
    " delta (index 1) that depends on it.
    lv_base_sha = zcl_abapgit_hash=>sha1_blob( '48656C6C6F21' ). " "Hello!"

    " Object 1: REF_DELTA whose declared base is the blob at object 2.
    " Delta instructions: base-size(6) result-size(7)
    " copy-op 0x90 (copy, size-byte0 present, no offset bytes -> offset 0)
    " + size-byte 0x06 (copy length 6) + insert-op 0x01 + literal 0x21 ('!').
    " Applying this against the real 6-byte base "Hello!" must produce
    " "Hello!!" (7 bytes).
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_base_sha. " declared base, NOT this entry's own identity
    ls_object-data  = '060790060121'.
    ls_object-index = 1.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 0 obj_index = 1 ) INTO TABLE lt_offset_map.

    " Object 2: the real, already-resolved base, positioned AFTER object 1.
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-data  = '48656C6C6F21'. " "Hello!"
    ls_object-sha1  = lv_base_sha.
    ls_object-index = 2.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 20 obj_index = 2 ) INTO TABLE lt_offset_map.

    zcl_abapgit_ortec_delta=>resolve_all(
      EXPORTING
        it_offset_map = lt_offset_map
        it_ofs_meta   = lt_ofs_meta
        iv_repo_key   = mc_repo
      CHANGING
        ct_objects    = lt_objects ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 1.
    cl_abap_unit_assert=>assert_subrc( msg = 'Resolved REF_DELTA object must remain in ct_objects' ).
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '48656C6C6F2121'
      msg = 'REF_DELTA must resolve against its real base even when that base is ' &&
            'positioned after the delta in the pack, not self-match or misresolve' ).
    cl_abap_unit_assert=>assert_equals( act = ls_object-type
      exp = zif_abapgit_git_definitions=>c_type-blob
      msg = 'A resolved delta inherits its type from its ultimate base' ).

    lv_expected_sha = zcl_abapgit_hash=>sha1_blob( '48656C6C6F2121' ).
    cl_abap_unit_assert=>assert_equals( act = ls_object-sha1 exp = lv_expected_sha
      msg = 'Resolved object must carry its real recomputed content SHA1' ).
  ENDMETHOD.

  METHOD resolve_after_prior_in_pass.
    " Regression for the MODIFY fix: resolve_one previously promoted a
    " resolved delta via direct field-symbol writes to <ls_object>-sha1, a
    " component of the "sha" secondary sorted key. That does not update the
    " key's internal structure (documented ABAP behavior), so any base
    " lookup performed AFTER at least one prior promotion in the same
    " resolve_all pass risked matching the wrong row via a now-stale key.
    " This test forces exactly that ordering: object 2 is fully resolved
    " and promoted BEFORE object 3 performs its own base lookup for a true
    " base (object 4) positioned even later.
    DATA lt_objects      TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object       LIKE LINE OF lt_objects.
    DATA lt_offset_map   TYPE zcl_abapgit_ortec_delta=>ty_offset_map_tt.
    DATA lt_ofs_meta     TYPE zcl_abapgit_ortec_delta=>ty_ofs_meta_tt.
    DATA lv_expected_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_base_sha     TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_hi_sha       TYPE zif_abapgit_git_definitions=>ty_sha1.

    lv_hi_sha   = zcl_abapgit_hash=>sha1_blob( '4869' ). " "Hi"
    lv_base_sha = zcl_abapgit_hash=>sha1_blob( '48656C6C6F21' ). " "Hello!"

    " Object 1: plain blob "Hi" (2 bytes).
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-data  = '4869'.
    ls_object-sha1  = lv_hi_sha.
    ls_object-index = 1.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 0 obj_index = 1 ) INTO TABLE lt_offset_map.

    " Object 2: REF_DELTA depending on object 1, resolved FIRST by
    " resolve_all's sequential walk (index 2 < 3), forcing a promotion
    " (and, with the fix, a MODIFY) before object 3's own lookup runs.
    " base-size(2) result-size(3) copy(off=0,len=2) insert(1,'!') -> "Hi!".
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_hi_sha.
    ls_object-data  = '020390020121'.
    ls_object-index = 2.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 10 obj_index = 2 ) INTO TABLE lt_offset_map.

    " Object 3: the entry under test - REF_DELTA whose true base (object 4)
    " is positioned AFTER it, unrelated to objects 1/2.
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_base_sha.
    ls_object-data  = '060790060121'.
    ls_object-index = 3.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 20 obj_index = 3 ) INTO TABLE lt_offset_map.

    " Object 4: the true base, "Hello!" (6 bytes), positioned after object 3.
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-data  = '48656C6C6F21'.
    ls_object-sha1  = lv_base_sha.
    ls_object-index = 4.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 30 obj_index = 4 ) INTO TABLE lt_offset_map.

    zcl_abapgit_ortec_delta=>resolve_all(
      EXPORTING
        it_offset_map = lt_offset_map
        it_ofs_meta   = lt_ofs_meta
        iv_repo_key   = mc_repo
      CHANGING
        ct_objects    = lt_objects ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 2.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '486921'
      msg = 'The unrelated, earlier-resolved delta must still resolve correctly to "Hi!"' ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 3.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '48656C6C6F2121'
      msg = 'The entry under test must resolve against its real, later-positioned base ' &&
            'even after another delta was already promoted earlier in the same pass' ).

    lv_expected_sha = zcl_abapgit_hash=>sha1_blob( '48656C6C6F2121' ).
    cl_abap_unit_assert=>assert_equals( act = ls_object-sha1 exp = lv_expected_sha ).
  ENDMETHOD.

  METHOD two_thin_bases_do_not_collide.
    " Regression for a gap found alongside the tabix hotfix:
    " zcl_abapgit_ortec_obj_store=>get_object never populates the returned
    " object's -index field, so every thin-fetched base previously defaulted
    " to index = 0. If two different REF_DELTA entries in the same
    " resolve_all pass each need a DIFFERENT thin base (neither present in
    " ct_objects), both fetched bases would collide on index = 0 and an
    " index-keyed lookup could bind the wrong delta to the wrong base. This
    " test forces exactly two distinct thin fetches in one pass and asserts
    " each delta resolves against its own, correct base.
    DATA lt_objects    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object     LIKE LINE OF lt_objects.
    DATA lt_offset_map TYPE zcl_abapgit_ortec_delta=>ty_offset_map_tt.
    DATA lt_ofs_meta   TYPE zcl_abapgit_ortec_delta=>ty_ofs_meta_tt.
    DATA lv_base1_sha  TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_base2_sha  TYPE zif_abapgit_git_definitions=>ty_sha1.

    lv_base1_sha = zcl_abapgit_hash=>sha1_blob( '41414141' ). " "AAAA"
    lv_base2_sha = zcl_abapgit_hash=>sha1_blob( '42424242' ). " "BBBB"

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_base1_sha iv_type = 'blob' iv_data = '41414141' ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_base2_sha iv_type = 'blob' iv_data = '42424242' ).

    " Both bases are absent from ct_objects - each delta must go through
    " the thin-base persistent-store fetch, not an in-pack lookup.
    " base-size(4) result-size(5) copy(off=0,len=4) insert(1,'!') -> base+'!'.
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_base1_sha.
    ls_object-data  = '040590040121'.
    ls_object-index = 1.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 0 obj_index = 1 ) INTO TABLE lt_offset_map.

    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_base2_sha.
    ls_object-data  = '040590040121'.
    ls_object-index = 2.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 10 obj_index = 2 ) INTO TABLE lt_offset_map.

    zcl_abapgit_ortec_delta=>resolve_all(
      EXPORTING
        it_offset_map = lt_offset_map
        it_ofs_meta   = lt_ofs_meta
        iv_repo_key   = mc_repo
      CHANGING
        ct_objects    = lt_objects ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 1.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '4141414121'
      msg = 'The first delta must resolve against its own thin base "AAAA", not the second' ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 2.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '4242424221'
      msg = 'The second delta must resolve against its own thin base "BBBB", not the first' ).
  ENDMETHOD.

  METHOD chain_onto_later_unresolved.
    " Regression for the multi-pass fixpoint fix: A (index 1) is a REF_DELTA
    " declaring a dependency on B's REAL identity, but B (index 2, positioned
    " AFTER A) is ITSELF still an unresolved REF_DELTA at pack-scan time - its
    " -sha1 field currently holds ITS OWN declared dependency (C's identity),
    " not yet B's real identity. A single ascending pass over the pack can
    " never find B for A (B's row does not carry B's real identity until AFTER
    " B itself is resolved, and resolve_all would already have moved past
    " object 1 by the time object 2 is reached). Before the fix this raised
    " "Delta base not found" even though the whole chain is fully resolvable
    " from within this single pack. This is also the shape of the real-world
    " "Delta base not found in pack/store (N missing)" failure: a raw
    " pre-scan (in zcl_abapgit_ortec_pack_dec) that only recognizes
    " ALREADY-non-delta objects as "in the pack" would flag B's needed
    " identity as external/missing purely because B had not been resolved
    " yet, even though it never leaves this pack.
    DATA lt_objects      TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object       LIKE LINE OF lt_objects.
    DATA lt_offset_map   TYPE zcl_abapgit_ortec_delta=>ty_offset_map_tt.
    DATA lt_ofs_meta     TYPE zcl_abapgit_ortec_delta=>ty_ofs_meta_tt.
    DATA lv_expected_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_b_sha        TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_c_sha        TYPE zif_abapgit_git_definitions=>ty_sha1.

    " C = "Hi" (2 bytes), the real, plain base at the end of the chain.
    lv_c_sha = zcl_abapgit_hash=>sha1_blob( '4869' ).
    " B = "Hi!" (3 bytes), produced by applying object 2's delta to C.
    lv_b_sha = zcl_abapgit_hash=>sha1_blob( '486921' ).

    " Object 1 (A): REF_DELTA declaring a dependency on B's real identity.
    " Delta: base-size(3) result-size(4), copy(off=0,len=3), insert(1,'!')
    " -> applying against B's eventual content "Hi!" yields "Hi!!".
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_b_sha. " declared dependency: B's real identity
    ls_object-data  = '030490030121'.
    ls_object-index = 1.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 0 obj_index = 1 ) INTO TABLE lt_offset_map.

    " Object 2 (B): REF_DELTA declaring a dependency on C's real identity -
    " ITS OWN -sha1 is C's identity, NOT B's identity, until resolved.
    " Positioned AFTER object 1, the dependent that needs B.
    " Delta: base-size(2) result-size(3), copy(off=0,len=2), insert(1,'!')
    " -> applying against C's content "Hi" yields "Hi!".
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_c_sha. " declared dependency: C's real identity
    ls_object-data  = '020390020121'.
    ls_object-index = 2.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 10 obj_index = 2 ) INTO TABLE lt_offset_map.

    " Object 3 (C): the real, plain base at the very end of the chain.
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-data  = '4869'.
    ls_object-sha1  = lv_c_sha.
    ls_object-index = 3.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 20 obj_index = 3 ) INTO TABLE lt_offset_map.

    zcl_abapgit_ortec_delta=>resolve_all(
      EXPORTING
        it_offset_map = lt_offset_map
        it_ofs_meta   = lt_ofs_meta
        iv_repo_key   = mc_repo
      CHANGING
        ct_objects    = lt_objects ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 2.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '486921'
      msg = 'The intermediate delta (B) must resolve against its real base (C) ' &&
            'even though B itself is only reached later in the ascending pass' ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 1.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '48692121'
      msg = 'The dependent delta (A) must resolve against its real base (B) even ' &&
            'though B was still an unresolved delta - not yet a plain object - the ' &&
            'first time A was attempted' ).

    lv_expected_sha = zcl_abapgit_hash=>sha1_blob( '48692121' ).
    cl_abap_unit_assert=>assert_equals( act = ls_object-sha1 exp = lv_expected_sha ).
  ENDMETHOD.

ENDCLASS.

CLASS ltcl_filtered_fetch DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_FLTFCH'.
    METHODS setup. METHODS teardown.
    "! Blank inputs must short-circuit before any network attempt - proven
    "! implicitly: a real HTTP call would fail/hang in this test
    "! environment, so a clean abap_false return proves the early guard
    "! fired instead.
    METHODS blank_inputs_no_network FOR TESTING RAISING cx_static_check.
    "! A commit already present locally must short-circuit to TRUE without
    "! attempting any network call - same implicit proof as above.
    METHODS already_local_short_circuits FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_filtered_fetch IMPLEMENTATION.
  METHOD setup. DELETE FROM zaog_obj_store WHERE repo_key = mc_repo. ENDMETHOD.
  METHOD teardown.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    ROLLBACK WORK.
  ENDMETHOD.

  METHOD blank_inputs_no_network.
    DATA lv_applicable TYPE abap_bool.

    lv_applicable = zcl_abapgit_ortec_fastpath=>try_filtered_commit_fetch(
      iv_url         = ''
      iv_branch_name = 'refs/heads/main'
      iv_commit      = ''
      iv_repo_key    = mc_repo ).

    cl_abap_unit_assert=>assert_equals( act = lv_applicable exp = abap_false
      msg = 'Blank URL/commit must never attempt a network call' ).
  ENDMETHOD.

  METHOD already_local_short_circuits.
    DATA lv_commit_data TYPE xstring.
    DATA lv_commit_sha  TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_applicable  TYPE abap_bool.

    lv_commit_data = '48656C6C6F'.
    lv_commit_sha = zcl_abapgit_hash=>sha1_commit( lv_commit_data ).

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_commit_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-commit
      iv_data     = lv_commit_data ).

    lv_applicable = zcl_abapgit_ortec_fastpath=>try_filtered_commit_fetch(
      iv_url         = 'https://filtered-fetch-test.example.com/repo.git'
      iv_branch_name = 'refs/heads/main'
      iv_commit      = lv_commit_sha
      iv_repo_key    = mc_repo ).

    cl_abap_unit_assert=>assert_equals( act = lv_applicable exp = abap_true
      msg = 'An already-locally-present commit must short-circuit to TRUE without any network attempt' ).
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
    METHODS stale_tip_invalidated FOR TESTING RAISING cx_static_check.
    METHODS invalidate_all_history_wide FOR TESTING RAISING cx_static_check.
    "! Regression: get_complete_commits must union commit_hist and
    "! repo_state fetch_commit entries, not treat commit_hist as the sole
    "! source whenever it has any row at all.
    METHODS commits_union_repo_state FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_repo_state IMPLEMENTATION.
  METHOD setup.
    DATA lv_key TYPE c LENGTH 12.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    IF lv_key IS NOT INITIAL.
      zcl_abapgit_ortec_repo_state=>clear_state( lv_key ).
      DELETE FROM zaog_commit_hist WHERE repo_key = lv_key.
    ENDIF.
  ENDMETHOD.
  METHOD teardown.
    DATA lv_key TYPE c LENGTH 12.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    IF lv_key IS NOT INITIAL.
      zcl_abapgit_ortec_repo_state=>clear_state( lv_key ).
      DELETE FROM zaog_obj_store WHERE repo_key = lv_key.
      DELETE FROM zaog_commit_hist WHERE repo_key = lv_key.
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
  METHOD stale_tip_invalidated.
    " Phase 7 coverage: the "stale-tip fallback" behavior relied on by
    " zcl_abapgit_ortec_filter_walk (and the walk/walk_tree repair path) is
    " driven by invalidate_tip_commit removing the "fully materialised"
    " signal that get_complete_commits/have-negotiation trust. Once a live
    " remote tip no longer matches what's cached, invalidating the tip must
    " make the read path treat it as no longer safe to serve from the local
    " store - forcing a fallback/re-fetch instead of silently serving stale
    " data.
    DATA lv_key TYPE c LENGTH 12.
    DATA lt_commits TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    CONSTANTS lc_commit TYPE zif_abapgit_git_definitions=>ty_sha1
      VALUE 'cccc000000000000000000000000000000000009'.

    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    zcl_abapgit_ortec_repo_state=>update_after_fetch(
      iv_repo_key = lv_key iv_branch_name = 'refs/heads/main'
      iv_url = mc_url iv_commit = lc_commit ).

    " ZAOG_COMMIT_HIST is what actually marks a commit as fully materialised
    " for get_complete_commits/have-negotiation purposes.
    INSERT zaog_commit_hist FROM @( VALUE #(
      repo_key = lv_key commit_sha1 = lc_commit branch_name = 'refs/heads/main' ) ).
    COMMIT WORK.

    lt_commits = zcl_abapgit_ortec_repo_state=>get_complete_commits( lv_key ).
    READ TABLE lt_commits WITH KEY table_line = lc_commit TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc(
      msg = 'Commit must be considered fully materialised before invalidation' ).

    zcl_abapgit_ortec_repo_state=>invalidate_tip_commit(
      iv_repo_key = lv_key iv_commit = lc_commit iv_branch_name = 'refs/heads/main' ).

    CLEAR lt_commits.
    lt_commits = zcl_abapgit_ortec_repo_state=>get_complete_commits( lv_key ).
    READ TABLE lt_commits WITH KEY table_line = lc_commit TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_equals( act = sy-subrc exp = 4
      msg = 'A stale/invalidated tip must no longer be considered fully materialised, ' &&
            'forcing the read path to fall back instead of trusting cached data' ).

    DATA(ls_state) = zcl_abapgit_ortec_repo_state=>get_state(
      iv_repo_key = lv_key iv_branch_name = 'refs/heads/main' ).
    cl_abap_unit_assert=>assert_initial( act = ls_state-fetch_commit
      msg = 'fetch_commit must be blanked so a stale tip cannot be reused for Phase 3 reconstitution' ).
  ENDMETHOD.
  METHOD invalidate_all_history_wide.
    " ES6 incident coverage: pull_by_branch's self-heal must guarantee an
    " empty have-set on retry (forcing a full/deepen pack), not just clear
    " the ONE commit/branch that happened to fail its walk. Reproduces two
    " branches sharing one repo, both marked fully materialised, then
    " verifies invalidate_all_history wipes ZAOG_COMMIT_HIST for the WHOLE
    " repo and blanks fetch_commit for EVERY branch, not just one.
    DATA lv_key TYPE c LENGTH 12.
    DATA lt_commits TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    CONSTANTS lc_commit_main TYPE zif_abapgit_git_definitions=>ty_sha1
      VALUE 'dddd000000000000000000000000000000000001'.
    CONSTANTS lc_commit_dev TYPE zif_abapgit_git_definitions=>ty_sha1
      VALUE 'dddd000000000000000000000000000000000002'.

    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    zcl_abapgit_ortec_repo_state=>update_after_fetch(
      iv_repo_key = lv_key iv_branch_name = 'refs/heads/main'
      iv_url = mc_url iv_commit = lc_commit_main ).
    zcl_abapgit_ortec_repo_state=>update_after_fetch(
      iv_repo_key = lv_key iv_branch_name = 'refs/heads/dev'
      iv_url = mc_url iv_commit = lc_commit_dev ).
    INSERT zaog_commit_hist FROM @( VALUE #(
      repo_key = lv_key commit_sha1 = lc_commit_main branch_name = 'refs/heads/main' ) ).
    INSERT zaog_commit_hist FROM @( VALUE #(
      repo_key = lv_key commit_sha1 = lc_commit_dev branch_name = 'refs/heads/dev' ) ).
    COMMIT WORK.

    lt_commits = zcl_abapgit_ortec_repo_state=>get_complete_commits( lv_key ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_commits ) exp = 2
      msg = 'Both branches'' commits must be considered fully materialised before invalidation' ).

    zcl_abapgit_ortec_repo_state=>invalidate_all_history( lv_key ).

    lt_commits = zcl_abapgit_ortec_repo_state=>get_complete_commits( lv_key ).
    cl_abap_unit_assert=>assert_initial( act = lt_commits
      msg = 'invalidate_all_history must leave NO commit advertisable as a have, ' &&
            'so the retry degrades to a full/deepen pack instead of repeating the same thin fetch' ).

    DATA(ls_main) = zcl_abapgit_ortec_repo_state=>get_state(
      iv_repo_key = lv_key iv_branch_name = 'refs/heads/main' ).
    DATA(ls_dev) = zcl_abapgit_ortec_repo_state=>get_state(
      iv_repo_key = lv_key iv_branch_name = 'refs/heads/dev' ).
    cl_abap_unit_assert=>assert_initial( act = ls_main-fetch_commit
      msg = 'fetch_commit must be blanked for EVERY branch, not just the one that failed its walk' ).
    cl_abap_unit_assert=>assert_initial( act = ls_dev-fetch_commit
      msg = 'fetch_commit must be blanked for EVERY branch, not just the one that failed its walk' ).
  ENDMETHOD.
  METHOD commits_union_repo_state.
    " Regression: get_complete_commits previously used zaog_commit_hist as
    " the ONLY source whenever it had ANY row at all for the repo, silently
    " hiding every OTHER branch's own recorded fetch_commit in
    " zaog_repo_state from have-negotiation - even though those branches
    " were fully fetched and typically share most of their object graph as
    " common ancestry with the branch being switched to. Reproduces exactly
    " that: one branch tracked only in commit_hist, a second tracked only
    " via its own repo_state fetch_commit (never added to commit_hist) -
    " both must be offered as candidates.
    DATA lv_key TYPE c LENGTH 12.
    DATA lt_commits TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    CONSTANTS lc_commit_hist_only TYPE zif_abapgit_git_definitions=>ty_sha1
      VALUE 'eeee000000000000000000000000000000000001'.
    CONSTANTS lc_commit_state_only TYPE zif_abapgit_git_definitions=>ty_sha1
      VALUE 'eeee000000000000000000000000000000000002'.

    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    zcl_abapgit_ortec_repo_state=>update_after_fetch(
      iv_repo_key = lv_key iv_branch_name = 'refs/heads/history-branch'
      iv_url = mc_url iv_commit = lc_commit_hist_only ).
    zcl_abapgit_ortec_repo_state=>update_after_fetch(
      iv_repo_key = lv_key iv_branch_name = 'refs/heads/state-only-branch'
      iv_url = mc_url iv_commit = lc_commit_state_only ).
    INSERT zaog_commit_hist FROM @( VALUE #(
      repo_key = lv_key commit_sha1 = lc_commit_hist_only branch_name = 'refs/heads/history-branch' ) ).
    COMMIT WORK.

    lt_commits = zcl_abapgit_ortec_repo_state=>get_complete_commits( lv_key ).

    READ TABLE lt_commits WITH KEY table_line = lc_commit_hist_only TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc(
      msg = 'The commit_hist-tracked commit must be a candidate' ).
    READ TABLE lt_commits WITH KEY table_line = lc_commit_state_only TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc(
      msg = 'A second branch tracked ONLY via its own repo_state fetch_commit ' &&
            '(never added to commit_hist) must ALSO be a candidate, not hidden ' &&
            'just because commit_hist happens to have an unrelated row' ).
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
    "! decode_and_persist when resumable_decode fails after temp rows were
    "! already written (corrupt trailing pack SHA1): verifies the CATCH
    "! zcx_abapgit_exception cleanup block removes every temp/meta/raw row
    "! it created, instead of leaving orphaned data for a future resume
    "! attempt to stumble over.
    METHODS cleanup_after_decode_failure FOR TESTING RAISING cx_static_check.
    "! Regression: resumable_decode's bulk external-delta-base prefetch merge
    "! never set the merged objects' -index, so every prefetched base
    "! defaulted to index = 0 and collided in resolve_all's UNIQUE-KEY
    "! obj_index side-index, causing a delta correctly found by SHA1 to be
    "! resolved against a completely different, unrelated prefetched base
    "! ("Delta base identity mismatch"). Forces two distinct external bases
    "! to be bulk-prefetched in one decode_and_persist call.
    METHODS prefetch_bases_do_not_collide FOR TESTING RAISING cx_static_check.
    "! peek_object_count must read the pack-header object count without
    "! decompressing anything, return 0 for a real zero-object pack (the
    "! "you already have everything" server response), and return -1
    "! (never 0) for data too short/malformed to contain a header, so
    "! callers cannot mistake "can't tell yet" for "confirmed empty".
    METHODS peek_object_count_cases FOR TESTING RAISING cx_static_check.
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

  METHOD cleanup_after_decode_failure.
    DATA lt_obj    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj    TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_pack   TYPE xstring.
    DATA lv_len    TYPE i.
    DATA lt_res    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_caught TYPE abap_bool.
    DATA lv_count  TYPE i.

    ls_obj-data  = '48656C6C6F'. " ASCII: Hello
    ls_obj-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_obj-sha1  = zcl_abapgit_hash=>sha1( iv_type = ls_obj-type iv_data = ls_obj-data ).
    ls_obj-index = 1.
    APPEND ls_obj TO lt_obj.

    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).

    " Corrupt only the last byte of the trailing 20-byte pack SHA1 so every
    " object still decompresses and parses correctly, and temp rows are
    " persisted as usual; only the final trailer-integrity check fails,
    " forcing decode_and_persist into its CATCH zcx_abapgit_exception
    " cleanup path with real, already-committed temp data to remove.
    lv_len = xstrlen( lv_pack ) - 1.
    lv_pack = lv_pack(lv_len) && 'FF'.

    lv_caught = abap_false.
    TRY.
        lt_res = zcl_abapgit_ortec_pack_dec=>decode_and_persist(
          iv_data     = lv_pack
          iv_repo_key = mc_repo ).
      CATCH zcx_abapgit_exception.
        lv_caught = abap_true.
    ENDTRY.
    cl_abap_unit_assert=>assert_true( act = lv_caught msg = 'Corrupt trailer must raise zcx_abapgit_exception' ).

    SELECT COUNT(*) FROM zaog_obj_store INTO lv_count WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 0 msg = 'obj_store temp rows cleaned up after failure' ).

    SELECT COUNT(*) FROM zaog_pack_idx INTO lv_count WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 0 msg = 'pack_idx rows cleaned up after failure' ).

    SELECT COUNT(*) FROM zaog_pack_meta INTO lv_count WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 0 msg = 'pack_meta row cleaned up after failure' ).

    SELECT COUNT(*) FROM zaog_raw_pack INTO lv_count WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 0 msg = 'raw_pack cleaned up after failure' ).
  ENDMETHOD.

  METHOD prefetch_bases_do_not_collide.
    DATA lv_base1_data TYPE xstring.
    DATA lv_base2_data TYPE xstring.
    DATA lv_base1_sha   TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_base2_sha   TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_base1_raw   TYPE x LENGTH 20.
    DATA lv_base2_raw   TYPE x LENGTH 20.
    DATA lv_delta       TYPE xstring.
    DATA lv_compressed  TYPE xstring.
    DATA lv_adler       TYPE zif_abapgit_git_definitions=>ty_adler32.
    DATA lv_pack_magic  TYPE x LENGTH 4 VALUE '5041434B'.
    DATA lv_version     TYPE x LENGTH 4 VALUE '00000002'.
    DATA lv_obj_count   TYPE x LENGTH 4 VALUE '00000002'.
    DATA lv_zlib_hdr    TYPE x LENGTH 2 VALUE '789C'.
    DATA lv_type_len    TYPE x LENGTH 1 VALUE '76'. " ref_d (0x70) | length 6, no continuation
    DATA lv_pack        TYPE xstring.
    DATA lv_trailer_hex TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_trailer_raw TYPE x LENGTH 20.
    DATA lt_res         TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_expect1_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_expect2_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    lv_base1_data = '41414141'. " "AAAA"
    lv_base2_data = '42424242'. " "BBBB"
    lv_base1_sha = zcl_abapgit_hash=>sha1_blob( lv_base1_data ).
    lv_base2_sha = zcl_abapgit_hash=>sha1_blob( lv_base2_data ).

    " Both bases stored externally (as if from a prior fetch) - NEITHER is
    " included in this pack, forcing resumable_decode's bulk external-base
    " prefetch (the buggy merge loop) to fire for both.
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_base1_sha iv_type = 'blob' iv_data = lv_base1_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_base2_sha iv_type = 'blob' iv_data = lv_base2_data ).

    " Delta: base-size(4) result-size(5), copy(off=0,len=4), insert(1,'!')
    " -> applies identically to any 4-byte base, producing base & "!". Both
    " pack entries use this exact same delta; they are distinguished only
    " by their declared (20-byte raw) base SHA1.
    lv_delta = '040590040121'.
    cl_abap_gzip=>compress_binary( EXPORTING raw_in = lv_delta IMPORTING gzip_out = lv_compressed ).
    lv_adler = zcl_abapgit_hash=>adler32( lv_delta ).

    lv_base1_raw = to_upper( lv_base1_sha ).
    lv_base2_raw = to_upper( lv_base2_sha ).

    CONCATENATE lv_pack_magic lv_version lv_obj_count INTO lv_pack IN BYTE MODE.
    CONCATENATE lv_pack lv_type_len lv_base1_raw lv_zlib_hdr lv_compressed lv_adler
      INTO lv_pack IN BYTE MODE.
    CONCATENATE lv_pack lv_type_len lv_base2_raw lv_zlib_hdr lv_compressed lv_adler
      INTO lv_pack IN BYTE MODE.

    lv_trailer_hex = zcl_abapgit_hash=>sha1_raw( lv_pack ).
    lv_trailer_raw = to_upper( lv_trailer_hex ).
    CONCATENATE lv_pack lv_trailer_raw INTO lv_pack IN BYTE MODE.

    lt_res = zcl_abapgit_ortec_pack_dec=>decode_and_persist(
      iv_data     = lv_pack
      iv_repo_key = mc_repo ).

    lv_expect1_sha = zcl_abapgit_hash=>sha1_blob( '4141414121' ). " "AAAA!"
    lv_expect2_sha = zcl_abapgit_hash=>sha1_blob( '4242424221' ). " "BBBB!"

    READ TABLE lt_res TRANSPORTING NO FIELDS
      WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-blob sha1 = lv_expect1_sha.
    cl_abap_unit_assert=>assert_subrc(
      msg = 'The delta declaring base1 must resolve against base1 ("AAAA!"), not collide with base2' ).

    READ TABLE lt_res TRANSPORTING NO FIELDS
      WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-blob sha1 = lv_expect2_sha.
    cl_abap_unit_assert=>assert_subrc(
      msg = 'The delta declaring base2 must resolve against base2 ("BBBB!"), not collide with base1' ).
  ENDMETHOD.

  METHOD peek_object_count_cases.
    DATA lv_data TYPE xstring.

    " Too short to contain a full 12-byte header.
    lv_data = '5041434B0000'.
    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_pack_dec=>peek_object_count( lv_data )
      exp = -1
      msg = 'Data shorter than the header must return -1, never 0' ).

    " Well-formed PACK header (magic + version 2) declaring zero objects -
    " the real "you already have everything" server response shape.
    lv_data = '5041434B0000000200000000'.
    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_pack_dec=>peek_object_count( lv_data )
      exp = 0
      msg = 'A well-formed zero-object pack header must be recognized as 0' ).

    " Well-formed PACK header declaring 3 objects.
    lv_data = '5041434B0000000200000003'.
    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_pack_dec=>peek_object_count( lv_data )
      exp = 3
      msg = 'A non-zero declared object count must be read correctly' ).

    " Long enough but missing the PACK magic entirely.
    lv_data = '000000000000000000000000'.
    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_pack_dec=>peek_object_count( lv_data )
      exp = -1
      msg = 'Data without the PACK magic must return -1, never 0' ).
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

CLASS ltcl_fastpath_protocol DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    METHODS buffer_emits_shallow_lines FOR TESTING RAISING cx_static_check.
    METHODS buffer_skips_shallow_forced FOR TESTING RAISING cx_static_check.
    METHODS parse_collects_shallow FOR TESTING RAISING cx_static_check.
    METHODS parse_ignores_bad_shallow FOR TESTING RAISING cx_static_check.
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
ENDCLASS.

CLASS ltcl_ortec_git_exception DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    "! Regression: zcx_abapgit_ortec_git never populates the T100 message
    "! infrastructure (if_t100_message~t100key is left blank), so without a
    "! get_text( ) override the inherited cx_root default returns generic/
    "! empty text instead of mv_text - confirmed live via a blank
    "! "thin: , non-thin: " cascade-failure message reaching Michael despite
    "! both underlying exceptions having real text set via raise( iv_text ).
    METHODS get_text_returns_mv_text FOR TESTING RAISING cx_static_check.
    "! Regression: by the time an exception reaches a CATCH block or a
    "! debugger breakpoint, the call stack that led to RAISE EXCEPTION
    "! (always inside this class's own static raise( ) method) has already
    "! unwound - without capturing it at construction time, the only
    "! inspectable "source position" points inside raise( ) itself, never
    "! the actual calling code that decided to raise. Asserts the filtered
    "! call stack's top frame is THIS test method (not RAISE or
    "! CONSTRUCTOR), proving the class's own frames were correctly removed.
    METHODS source_position_to_caller FOR TESTING RAISING cx_static_check.
    "! Regression: serve_cached_when_nothing_new's failures previously gave
    "! upload_pack_by_branch/_by_commit no way to know a fresh, haves-free
    "! retry might succeed where the server's "nothing new" claim disagreed
    "! with the local cache - confirmed live across multiple objects/call
    "! paths. Asserts raise( iv_retry_without_haves = abap_true ) is
    "! correctly readable back off the caught exception instance.
    METHODS retry_without_haves_flag_set FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_ortec_git_exception IMPLEMENTATION.
  METHOD get_text_returns_mv_text.
    DATA lx_direct TYPE REF TO zcx_abapgit_ortec_git.
    TRY.
        zcx_abapgit_ortec_git=>raise( 'a specific, non-generic failure detail' ).
      CATCH zcx_abapgit_ortec_git INTO lx_direct.
    ENDTRY.
    cl_abap_unit_assert=>assert_equals(
      act = lx_direct->get_text( )
      exp = 'a specific, non-generic failure detail'
      msg = 'get_text( ) must return mv_text, not a generic/blank cx_root default' ).
  ENDMETHOD.
  METHOD source_position_to_caller.
    DATA lx_direct TYPE REF TO zcx_abapgit_ortec_git.
    DATA lv_program_name TYPE progname.
    DATA lv_include_name TYPE progname.
    DATA lv_source_line  TYPE i.

    TRY.
        zcx_abapgit_ortec_git=>raise( 'source position regression check' ).
      CATCH zcx_abapgit_ortec_git INTO lx_direct.
    ENDTRY.

    cl_abap_unit_assert=>assert_not_initial(
      act = lx_direct->mt_callstack
      msg = 'mt_callstack must be captured at raise time' ).
    cl_abap_unit_assert=>assert_equals(
      act = lx_direct->mt_callstack[ 1 ]-blockname
      exp = 'SOURCE_POSITION_TO_CALLER'
      msg = 'The top of the filtered call stack must be the actual caller, ' &&
            'not RAISE or CONSTRUCTOR from inside zcx_abapgit_ortec_git itself' ).

    cl_abap_unit_assert=>assert_not_initial(
      act = lx_direct->ms_src_info-line
      msg = 'ms_src_info must be populated at construction time so it ' &&
            'survives a debugger breakpoint after the stack has unwound' ).

    lx_direct->get_source_position(
      IMPORTING
        program_name = lv_program_name
        include_name = lv_include_name
        source_line  = lv_source_line ).
    cl_abap_unit_assert=>assert_equals(
      act = lv_source_line
      exp = lx_direct->ms_src_info-line
      msg = 'get_source_position( ) must be consistent with ms_src_info' ).
  ENDMETHOD.
  METHOD retry_without_haves_flag_set.
    DATA lx_default TYPE REF TO zcx_abapgit_ortec_git.
    DATA lx_retryable TYPE REF TO zcx_abapgit_ortec_git.

    TRY.
        zcx_abapgit_ortec_git=>raise( 'not retryable by default' ).
      CATCH zcx_abapgit_ortec_git INTO lx_default.
    ENDTRY.
    cl_abap_unit_assert=>assert_equals(
      act = lx_default->mv_retry_without_haves
      exp = abap_false
      msg = 'mv_retry_without_haves must default to false when not passed' ).

    TRY.
        zcx_abapgit_ortec_git=>raise(
          iv_text                = 'retryable failure'
          iv_retry_without_haves = abap_true ).
      CATCH zcx_abapgit_ortec_git INTO lx_retryable.
    ENDTRY.
    cl_abap_unit_assert=>assert_equals(
      act = lx_retryable->mv_retry_without_haves
      exp = abap_true
      msg = 'mv_retry_without_haves must be readable back off the caught exception' ).
  ENDMETHOD.
ENDCLASS.

CLASS ltcl_spike_a_db_base_parity DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_SPIKEA'.
    METHODS setup.
    METHODS teardown.
    "! Streaming-decoder design Spike A (see .memory/state.md "Live crash
    "! confirms..." entry / streaming decoder open question 8): proves that
    "! zcl_abapgit_ortec_delta=>apply's result is identical whether its base
    "! object's bytes come from an in-memory literal or are freshly read
    "! back from zaog_obj_store via zcl_abapgit_ortec_obj_store=>get_object -
    "! the exact substitution the streaming resolver design depends on
    "! (DB-backed bases instead of a shared in-memory ct_objects table).
    "! apply() is a pure function (IMPORTING iv_base/iv_delta TYPE xstring,
    "! no shared-state dependency), so this also incidentally proves the
    "! store/get_object round-trip preserves bytes exactly - if it didn't,
    "! this would be the first place to notice.
    METHODS db_base_matches_in_memory FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_spike_a_db_base_parity IMPLEMENTATION.
  METHOD setup.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
  ENDMETHOD.
  METHOD teardown.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    ROLLBACK WORK.
  ENDMETHOD.

  METHOD db_base_matches_in_memory.
    " Same hand-verified vector already used elsewhere in this test file
    " (ltcl_ref_delta): base "Hello!" (6 bytes) + delta 060790060121
    " (copy 6 bytes from offset 0, then insert literal '!') must produce
    " "Hello!!" (7 bytes).
    DATA lv_base_in_memory TYPE xstring VALUE '48656C6C6F21'.
    DATA lv_base_from_db   TYPE xstring.
    DATA lv_delta          TYPE xstring VALUE '060790060121'.
    DATA lv_result_memory  TYPE xstring.
    DATA lv_result_db      TYPE xstring.
    DATA lv_base_sha       TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA ls_object         TYPE zif_abapgit_definitions=>ty_object.

    lv_base_sha = zcl_abapgit_hash=>sha1_blob( lv_base_in_memory ).

    " Persist the SAME base bytes for real, through the actual production
    " persistence path (store_object), then read them back through the
    " actual production read path (get_object) - not a shortcut/mock.
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_base_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_base_in_memory ).

    ls_object = zcl_abapgit_ortec_obj_store=>get_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_base_sha ).
    lv_base_from_db = ls_object-data.

    cl_abap_unit_assert=>assert_equals(
      act = lv_base_from_db
      exp = lv_base_in_memory
      msg = 'store_object/get_object round-trip must preserve base bytes exactly - ' &&
            'the streaming resolver design depends on this' ).

    lv_result_memory = zcl_abapgit_ortec_delta=>apply(
      iv_base  = lv_base_in_memory
      iv_delta = lv_delta ).

    lv_result_db = zcl_abapgit_ortec_delta=>apply(
      iv_base  = lv_base_from_db
      iv_delta = lv_delta ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_result_memory
      exp = '48656C6C6F2121'
      msg = 'Sanity check: applying the known-good vector against the in-memory ' &&
            'base must produce the expected "Hello!!" result' ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_result_db
      exp = lv_result_memory
      msg = 'SPIKE A: apply() must produce an IDENTICAL result whether its base ' &&
            'came from an in-memory literal or was freshly read back from ' &&
            'zaog_obj_store via get_object - this is the core assumption the ' &&
            'streaming delta-resolver redesign depends on (DB-backed bases ' &&
            'instead of a shared in-memory ct_objects table)' ).
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

CLASS ltcl_cache_admin DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_CADMIN'.
    METHODS setup. METHODS teardown.
    METHODS overview_aggregates_counts FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_cache_admin IMPLEMENTATION.
  METHOD setup.
    DELETE FROM zaog_obj_store  WHERE repo_key = mc_repo.
    DELETE FROM zaog_obj_index  WHERE repo_key = mc_repo.
    DELETE FROM zaog_pack_meta  WHERE repo_key = mc_repo.
    DELETE FROM zaog_commit_hist WHERE repo_key = mc_repo.
    DELETE FROM zaog_fetch_sess WHERE repo_key = mc_repo.
    DELETE FROM zaog_repo_state WHERE repo_key = mc_repo.
  ENDMETHOD.
  METHOD teardown.
    DELETE FROM zaog_obj_store  WHERE repo_key = mc_repo.
    DELETE FROM zaog_obj_index  WHERE repo_key = mc_repo.
    DELETE FROM zaog_pack_meta  WHERE repo_key = mc_repo.
    DELETE FROM zaog_commit_hist WHERE repo_key = mc_repo.
    DELETE FROM zaog_fetch_sess WHERE repo_key = mc_repo.
    DELETE FROM zaog_repo_state WHERE repo_key = mc_repo.
    ROLLBACK WORK.
  ENDMETHOD.
  METHOD overview_aggregates_counts.
    DATA ls_state  TYPE zaog_repo_state.
    DATA ls_idx    TYPE zaog_obj_index.
    DATA ls_pack   TYPE zaog_pack_meta.
    DATA ls_commit TYPE zaog_commit_hist.
    DATA ls_sess   TYPE zaog_fetch_sess.
    DATA lv_ts     TYPE timestampl.
    DATA lv_data   TYPE xstring.

    TRY.

        GET TIME STAMP FIELD lv_ts.

        ls_state-repo_key    = mc_repo.
        ls_state-branch_name = 'refs/heads/main'.
        ls_state-remote_url  = 'https://test-cache-admin.example.com/repo.git'.
        ls_state-curr_commit = 'aaaa000000000000000000000000000000000001'.
        ls_state-is_shallow  = abap_false.
        MODIFY zaog_repo_state FROM ls_state.

        lv_data = '48656C6C6F'.
        zcl_abapgit_ortec_obj_store=>store_object(
          iv_repo_key = mc_repo iv_sha1 = 'bbbb000000000000000000000000000000000001'
          iv_type = zif_abapgit_git_definitions=>c_type-blob iv_data = lv_data ).
        zcl_abapgit_ortec_obj_store=>store_object(
          iv_repo_key = mc_repo iv_sha1 = 'bbbb000000000000000000000000000000000002'
          iv_type = zif_abapgit_git_definitions=>c_type-blob iv_data = lv_data ).

        ls_idx-repo_key    = mc_repo.
        ls_idx-commit_sha1 = 'aaaa000000000000000000000000000000000001'.
        ls_idx-obj_type    = 'PROG'.
        ls_idx-obj_name    = 'ZTEST'.
        ls_idx-path_hash   = zcl_abapgit_hash=>sha1_string( '/' ).
        ls_idx-file_path   = '/'.
        MODIFY zaog_obj_index FROM ls_idx.

        ls_pack-repo_key   = mc_repo.
        ls_pack-pack_id    = 'TESTPACK00000000000000000000CADM'.
        ls_pack-total_size = 2097152. " exactly 2 MB, so pack_mb_disk asserts cleanly
        ls_pack-status     = 'C'.
        ls_pack-raw_stored = abap_true.
        ls_pack-received_at = lv_ts.
        MODIFY zaog_pack_meta FROM ls_pack.

        ls_commit-repo_key    = mc_repo.
        ls_commit-commit_sha1 = 'aaaa000000000000000000000000000000000001'.
        ls_commit-branch_name = 'refs/heads/main'.
        ls_commit-fetched_at  = lv_ts.
        MODIFY zaog_commit_hist FROM ls_commit.

        ls_sess-session_id = 'CADMINTEST000000000000000000001A'.
        ls_sess-repo_key   = mc_repo.
        ls_sess-branch_name = 'refs/heads/main'.
        ls_sess-phase      = 'D'.
        ls_sess-status     = 'A'.
        ls_sess-created_at = lv_ts.
        ls_sess-updated_at = lv_ts.
        MODIFY zaog_fetch_sess FROM ls_sess.

        COMMIT WORK AND WAIT.

      CATCH cx_root INTO DATA(lx_diag_seed).
        cl_abap_unit_assert=>fail( |DIAG SEED { cl_abap_classdescr=>get_class_name( lx_diag_seed ) }: { lx_diag_seed->get_text( ) }| ).
    ENDTRY.

    TRY.
        DATA(lt_overview) = zcl_abapgit_ortec_cache_admin=>get_overview( ).
      CATCH cx_root INTO DATA(lx_diag_sql).
        cl_abap_unit_assert=>fail( |DIAG { cl_abap_classdescr=>get_class_name( lx_diag_sql ) }: { lx_diag_sql->get_text( ) }| ).
    ENDTRY.
    READ TABLE lt_overview INTO DATA(ls_overview) WITH KEY repo_key = mc_repo.
    cl_abap_unit_assert=>assert_subrc( msg = 'Overview must contain the seeded test repo' ).

    cl_abap_unit_assert=>assert_equals( act = ls_overview-remote_url
      exp = 'https://test-cache-admin.example.com/repo.git' msg = 'remote_url must match' ).
    cl_abap_unit_assert=>assert_equals( act = ls_overview-obj_count exp = 2
      msg = 'obj_count must count both stored blobs' ).
    cl_abap_unit_assert=>assert_equals( act = ls_overview-idx_entries exp = 1
      msg = 'idx_entries must count the seeded index row' ).
    cl_abap_unit_assert=>assert_equals( act = ls_overview-pack_count exp = 1
      msg = 'pack_count must count the seeded pack' ).
    cl_abap_unit_assert=>assert_equals( act = ls_overview-pack_mb_disk exp = '2.00'
      msg = 'pack_mb_disk must reflect the 2 MB raw_stored pack' ).
    cl_abap_unit_assert=>assert_equals( act = ls_overview-commit_count exp = 1
      msg = 'commit_count must count the seeded commit' ).
    cl_abap_unit_assert=>assert_equals( act = ls_overview-open_sessions exp = 1
      msg = 'open_sessions must count the seeded active session' ).
  ENDMETHOD.
ENDCLASS.
