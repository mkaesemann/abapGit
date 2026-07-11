CLASS ltcl_git_porcelain DEFINITION DEFERRED.
CLASS zcl_abapgit_git_porcelain DEFINITION LOCAL FRIENDS ltcl_git_porcelain.

CLASS ltcl_git_porcelain DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  PRIVATE SECTION.
    CONSTANTS mc_repo_a TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key VALUE 'ZAOGT_RK_A'.
    CONSTANTS mc_repo_b TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key VALUE 'ZAOGT_RK_B'.

    METHODS:
      setup,
      teardown,
      append
        IMPORTING iv_path TYPE string
                  iv_name TYPE string,
      single_file FOR TESTING
        RAISING zcx_abapgit_exception,
      two_files_same_path FOR TESTING
        RAISING zcx_abapgit_exception,
      root_empty FOR TESTING
        RAISING zcx_abapgit_exception,
      namespaces FOR TESTING
        RAISING zcx_abapgit_exception,
      more_sub FOR TESTING
        RAISING zcx_abapgit_exception,
      sub FOR TESTING
        RAISING zcx_abapgit_exception,
      walk_tree_repo_key_isolation FOR TESTING
        RAISING cx_static_check.

    DATA: mt_expanded TYPE zif_abapgit_git_definitions=>ty_expanded_tt,
          mt_trees    TYPE zcl_abapgit_git_porcelain=>ty_trees_tt.

ENDCLASS.

CLASS ltcl_git_porcelain IMPLEMENTATION.

  METHOD setup.
    CLEAR mt_expanded.
    CLEAR mt_trees.
    DELETE FROM zaog_obj_store WHERE repo_key = @mc_repo_a.
    DELETE FROM zaog_obj_store WHERE repo_key = @mc_repo_b.
    zcl_abapgit_ortec_obj_store=>invalidate_cache( ).
  ENDMETHOD.

  METHOD teardown.
    DELETE FROM zaog_obj_store WHERE repo_key = @mc_repo_a.
    DELETE FROM zaog_obj_store WHERE repo_key = @mc_repo_b.
    zcl_abapgit_ortec_obj_store=>invalidate_cache( ).
    ROLLBACK WORK.
  ENDMETHOD.

  METHOD walk_tree_repo_key_isolation.

    " Regression test for D7 concrete instance: walk_tree must resolve objects using the
    " explicitly passed repo_key, never a stale session-cached repo_key from an earlier
    " ORTEC object-store read of a *different* repository.

    DATA lt_nodes      TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node       LIKE LINE OF lt_nodes.
    DATA lv_blob_data  TYPE xstring.
    DATA lv_blob_sha   TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_tree_data  TYPE xstring.
    DATA lv_tree_sha   TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_other_data TYPE xstring.
    DATA lv_other_sha  TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lt_objects    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_expanded   TYPE zif_abapgit_git_definitions=>ty_expanded_tt.

    " Build a one-file tree that lives only in repo A.
    lv_blob_data = '48656C6C6F'.
    lv_blob_sha  = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).

    CLEAR ls_node.
    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_node-name  = 'repo_a_only.txt'.
    ls_node-sha1  = lv_blob_sha.
    APPEND ls_node TO lt_nodes.

    lv_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    lv_tree_sha  = zcl_abapgit_hash=>sha1_tree( lv_tree_data ).

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo_a
      iv_sha1     = lv_tree_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-tree
      iv_data     = lv_tree_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo_a
      iv_sha1     = lv_blob_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_blob_data ).

    " Store an unrelated blob in repo B and read it back, which sets the ORTEC object
    " store's session-level cached repo_key to repo B - simulating an earlier operation
    " in the same session that touched a different repository.
    lv_other_data = '576F726C64'.
    lv_other_sha  = zcl_abapgit_hash=>sha1_blob( lv_other_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo_b
      iv_sha1     = lv_other_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_other_data ).
    zcl_abapgit_ortec_obj_store=>get_object(
      iv_repo_key = mc_repo_b
      iv_sha1     = lv_other_sha ).

    " Walk repo A's tree with an explicit repo_key while the session cache still points
    " at repo B. it_objects is empty so walk_tree must fall back to the ORTEC store.
    lt_expanded = zcl_abapgit_git_porcelain=>walk_tree(
      it_objects  = lt_objects
      iv_tree     = lv_tree_sha
      iv_base     = '/'
      iv_repo_key = mc_repo_a ).

    cl_abap_unit_assert=>assert_equals(
      act = lines( lt_expanded )
      exp = 1
      msg = 'walk_tree must resolve the tree via the explicit repo_key, not the stale session cache' ).

    READ TABLE lt_expanded WITH KEY name = 'repo_a_only.txt' TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( msg = 'Expected file from repo A was not returned by walk_tree' ).

  ENDMETHOD.

  METHOD append.

    FIELD-SYMBOLS: <ls_expanded> LIKE LINE OF mt_expanded.


    APPEND INITIAL LINE TO mt_expanded ASSIGNING <ls_expanded>.
    <ls_expanded>-path  = iv_path.
    <ls_expanded>-name  = iv_name.
    <ls_expanded>-sha1  = 'a'.
    <ls_expanded>-chmod = zif_abapgit_git_definitions=>c_chmod-file.

  ENDMETHOD.

  METHOD single_file.

    append( iv_path = '/'
            iv_name = 'foobar.txt' ).

    mt_trees = zcl_abapgit_git_porcelain=>build_trees( mt_expanded ).

    cl_abap_unit_assert=>assert_equals(
      act = lines( mt_trees )
      exp = 1 ).

  ENDMETHOD.

  METHOD two_files_same_path.

    append( iv_path = '/'
            iv_name = 'foo.txt' ).

    append( iv_path = '/'
            iv_name = 'bar.txt' ).

    mt_trees = zcl_abapgit_git_porcelain=>build_trees( mt_expanded ).

    cl_abap_unit_assert=>assert_equals(
      act = lines( mt_trees )
      exp = 1 ).

  ENDMETHOD.

  METHOD sub.

    append( iv_path = '/'
            iv_name = 'foo.txt' ).

    append( iv_path = '/sub/'
            iv_name = 'bar.txt' ).

    mt_trees = zcl_abapgit_git_porcelain=>build_trees( mt_expanded ).

    cl_abap_unit_assert=>assert_equals(
      act = lines( mt_trees )
      exp = 2 ).

  ENDMETHOD.

  METHOD more_sub.

    FIELD-SYMBOLS: <ls_tree> LIKE LINE OF mt_trees.

    append( iv_path = '/src/foo_a/foo_a1/'
            iv_name = 'a1.txt' ).

    append( iv_path = '/src/foo_a/foo_a2/'
            iv_name = 'a2.txt' ).

    mt_trees = zcl_abapgit_git_porcelain=>build_trees( mt_expanded ).

    cl_abap_unit_assert=>assert_equals(
      act = lines( mt_trees )
      exp = 5 ).

    LOOP AT mt_trees ASSIGNING <ls_tree>.
      cl_abap_unit_assert=>assert_not_initial( <ls_tree>-data ).
    ENDLOOP.

  ENDMETHOD.

  METHOD namespaces.

    FIELD-SYMBOLS: <ls_tree> LIKE LINE OF mt_trees.

    append( iv_path = '/src/#foo#a/#foo#a1/'
            iv_name = 'a1.txt' ).

    append( iv_path = '/src/#foo#a/#foo#a2/'
            iv_name = 'a2.txt' ).

    mt_trees = zcl_abapgit_git_porcelain=>build_trees( mt_expanded ).

    cl_abap_unit_assert=>assert_equals(
      act = lines( mt_trees )
      exp = 5 ).

    LOOP AT mt_trees ASSIGNING <ls_tree>.
      cl_abap_unit_assert=>assert_not_initial( <ls_tree>-data ).
    ENDLOOP.

  ENDMETHOD.

  METHOD root_empty.

    append( iv_path = '/sub/'
            iv_name = 'bar.txt' ).

    mt_trees = zcl_abapgit_git_porcelain=>build_trees( mt_expanded ).

* so 2 total trees are expected: '/' and '/sub/'
    cl_abap_unit_assert=>assert_equals(
      act = lines( mt_trees )
      exp = 2 ).

  ENDMETHOD.

ENDCLASS.
