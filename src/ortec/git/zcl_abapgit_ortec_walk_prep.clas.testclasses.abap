CLASS zcl_abapgit_ortec_walk_prep DEFINITION LOCAL FRIENDS ltcl_walk_prep.

CLASS ltcl_walk_prep DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key VALUE 'ZAOG_TST_WP'.

    METHODS setup.
    METHODS teardown.
    METHODS cleanup_repo
      IMPORTING iv_repo TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.

    METHODS build_blob
      IMPORTING iv_text       TYPE string
      RETURNING VALUE(rs_obj) TYPE zif_abapgit_definitions=>ty_object.

    METHODS build_tree
      IMPORTING it_nodes      TYPE zcl_abapgit_git_pack=>ty_nodes_tt
      RETURNING VALUE(rs_obj) TYPE zif_abapgit_definitions=>ty_object.

    METHODS store_obj
      IMPORTING iv_repo TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                is_obj  TYPE zif_abapgit_definitions=>ty_object.

    METHODS tree_from_store_and_nested FOR TESTING RAISING cx_static_check.
    METHODS sibling_frontier_trees     FOR TESTING RAISING cx_static_check.
    METHODS missing_tree_raises        FOR TESTING RAISING cx_static_check.
    METHODS wrong_type_tree_raises     FOR TESTING RAISING cx_static_check.
    METHODS it_objects_tree_added      FOR TESTING RAISING cx_static_check.
    METHODS duplicate_child_once       FOR TESTING RAISING cx_static_check.

    METHODS complete_graph_is_noop FOR TESTING RAISING cx_static_check.
    METHODS fetch_blobs_bulk_consumes FOR TESTING RAISING cx_static_check.
    METHODS bulk_drains_across_batches FOR TESTING RAISING cx_static_check.
    METHODS oversized_blob_single_batch FOR TESTING RAISING cx_static_check.

    METHODS fetch_window_progress FOR TESTING RAISING cx_static_check.
    METHODS duplicate_remaining_drained FOR TESTING RAISING cx_static_check.
ENDCLASS.

CLASS ltcl_walk_prep IMPLEMENTATION.
  METHOD setup.
    cleanup_repo( mc_repo ).
  ENDMETHOD.

  METHOD teardown.
    cleanup_repo( mc_repo ).
  ENDMETHOD.

  METHOD cleanup_repo.
    ROLLBACK WORK.                                     "#EC CI_ROLLBACK
    DELETE FROM zaog_obj_store WHERE repo_key = iv_repo.
    DELETE FROM zaog_commit_hist WHERE repo_key = iv_repo.
    DELETE FROM zaog_repo_state WHERE repo_key = iv_repo.
    COMMIT WORK.
    zcl_abapgit_ortec_obj_store=>invalidate_cache( ).
  ENDMETHOD.

  METHOD fetch_window_progress.

    DATA lt_sha1s     TYPE
      zif_abapgit_git_definitions=>ty_sha1_tt.

    DATA lt_remaining TYPE
      zif_abapgit_git_definitions=>ty_sha1_tt.

    DATA lv_data TYPE xstring.

    DO 3 TIMES.

      lv_data =
        zcl_abapgit_convert=>string_to_xstring_utf8(
          |fetch-window-{ sy-index }| ).

      DATA(lv_sha1) =
        zcl_abapgit_hash=>sha1_blob( lv_data ).

      zcl_abapgit_ortec_obj_store=>store_object(
        iv_repo_key = mc_repo
        iv_sha1     = lv_sha1
        iv_type     =
          zif_abapgit_git_definitions=>c_type-blob
        iv_data     = lv_data ).

      APPEND lv_sha1 TO lt_sha1s.

    ENDDO.

    lt_remaining = lt_sha1s.

    DATA(lt_objects) =
      zcl_abapgit_ortec_walk_prep=>fetch_blobs_bulk(
        EXPORTING
          iv_repo_key = mc_repo
          it_sha1s    = lt_remaining
        CHANGING
          ct_remaining_sha1s = lt_remaining ).

    cl_abap_unit_assert=>assert_equals(
      act = lines( lt_objects )
      exp = 3 ).

    cl_abap_unit_assert=>assert_initial(
      act = lt_remaining ).

  ENDMETHOD.


  METHOD duplicate_remaining_drained.
    DATA lt_requested TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_remaining TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_data TYPE xstring.
    DATA lv_loaded_sha1 TYPE zif_abapgit_git_definitions=>ty_sha1.

    lv_data = '4455504C4943415445'.
    lv_loaded_sha1 = zcl_abapgit_hash=>sha1_blob( lv_data ).

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_loaded_sha1
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_data ).

    APPEND lv_loaded_sha1 TO lt_requested.
    APPEND lv_loaded_sha1 TO lt_remaining.
    APPEND lv_loaded_sha1 TO lt_remaining.

    lt_objects = zcl_abapgit_ortec_walk_prep=>fetch_blobs_bulk(
      EXPORTING
        iv_repo_key = mc_repo
        it_sha1s    = lt_requested
      CHANGING
        ct_remaining_sha1s = lt_remaining ).

    cl_abap_unit_assert=>assert_equals(
      act = lines( lt_objects )
      exp = 1
      msg = 'The loaded blob is returned exactly once' ).

    cl_abap_unit_assert=>assert_initial(
      act = lt_remaining
      msg = 'Every duplicate occurrence is removed by the linear rebuild' ).
  ENDMETHOD.

  METHOD build_blob.
    DATA lv_data TYPE xstring.

    TRY.
        lv_data = zcl_abapgit_convert=>string_to_xstring_utf8( iv_text ).
        rs_obj-type = zif_abapgit_git_definitions=>c_type-blob.
        rs_obj-sha1 = zcl_abapgit_hash=>sha1_blob( lv_data ).
        rs_obj-data = lv_data.
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        cl_abap_unit_assert=>fail( msg = lx_error->get_text( ) ).
    ENDTRY.
  ENDMETHOD.

  METHOD build_tree.
    DATA lv_data TYPE xstring.

    TRY.
        lv_data = zcl_abapgit_git_pack=>encode_tree( it_nodes ).
        rs_obj-type = zif_abapgit_git_definitions=>c_type-tree.
        rs_obj-sha1 = zcl_abapgit_hash=>sha1_tree( lv_data ).
        rs_obj-data = lv_data.
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        cl_abap_unit_assert=>fail( msg = lx_error->get_text( ) ).
    ENDTRY.
  ENDMETHOD.

  METHOD store_obj.
    TRY.
        zcl_abapgit_ortec_obj_store=>store_object(
            iv_repo_key = iv_repo
            iv_sha1     = is_obj-sha1
            iv_type     = is_obj-type
            iv_data     = is_obj-data ).
      CATCH zcx_abapgit_ortec_git INTO DATA(lx_error).
        cl_abap_unit_assert=>fail( msg = lx_error->get_text( ) ).
    ENDTRY.
  ENDMETHOD.

  METHOD tree_from_store_and_nested.
    DATA ls_blob_obj   TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_child_obj  TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_root_obj   TYPE zif_abapgit_definitions=>ty_object.
    DATA lt_child_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA lt_root_nodes  TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_child_node  TYPE zcl_abapgit_git_pack=>ty_node.
    DATA ls_root_node   TYPE zcl_abapgit_git_pack=>ty_node.
    DATA lt_ct_objects  TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_tree_objs   TYPE zif_abapgit_definitions=>ty_objects_tt.

    ls_blob_obj = build_blob( 'child' ).
    store_obj( iv_repo = mc_repo is_obj = ls_blob_obj ).

    ls_child_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_child_node-name  = 'leaf.txt'.
    ls_child_node-sha1  = ls_blob_obj-sha1.
    APPEND ls_child_node TO lt_child_nodes.

    ls_child_obj = build_tree( lt_child_nodes ).
    store_obj( iv_repo = mc_repo is_obj = ls_child_obj ).

    ls_root_node-chmod = zif_abapgit_git_definitions=>c_chmod-dir.
    ls_root_node-name  = 'sub'.
    ls_root_node-sha1  = ls_child_obj-sha1.
    APPEND ls_root_node TO lt_root_nodes.

    ls_root_obj = build_tree( lt_root_nodes ).
    store_obj( iv_repo = mc_repo is_obj = ls_root_obj ).

    lt_tree_objs = zcl_abapgit_ortec_walk_prep=>warm_trees(
      EXPORTING
        iv_repo_key  = mc_repo
        iv_root_tree = ls_root_obj-sha1
        it_objects   = VALUE zif_abapgit_definitions=>ty_objects_tt( )
      CHANGING
        ct_objects   = lt_ct_objects ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_ct_objects ) exp = 2 ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_tree_objs ) exp = 2 ).
  ENDMETHOD.

  METHOD sibling_frontier_trees.
    DATA ls_first_obj   TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_second_obj  TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_root_obj    TYPE zif_abapgit_definitions=>ty_object.
    DATA lt_first_nodes  TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA lt_second_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA lt_root_nodes   TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node        TYPE zcl_abapgit_git_pack=>ty_node.
    DATA lt_ct_objects  TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_tree_objs   TYPE zif_abapgit_definitions=>ty_objects_tt.

    DATA ls_first_blob TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_second_blob TYPE zif_abapgit_definitions=>ty_object.
    DATA lt_first_blob_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA lt_second_blob_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_first_blob_node TYPE zcl_abapgit_git_pack=>ty_node.
    DATA ls_second_blob_node TYPE zcl_abapgit_git_pack=>ty_node.

    ls_first_blob = build_blob( 'one' ).
    store_obj( iv_repo = mc_repo is_obj = ls_first_blob ).
    ls_first_blob_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_first_blob_node-name = 'one.txt'.
    ls_first_blob_node-sha1 = ls_first_blob-sha1.
    APPEND ls_first_blob_node TO lt_first_blob_nodes.

    ls_second_blob = build_blob( 'two' ).
    store_obj( iv_repo = mc_repo is_obj = ls_second_blob ).
    ls_second_blob_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_second_blob_node-name = 'two.txt'.
    ls_second_blob_node-sha1 = ls_second_blob-sha1.
    APPEND ls_second_blob_node TO lt_second_blob_nodes.

    ls_first_obj = build_tree( lt_first_blob_nodes ).
    store_obj( iv_repo = mc_repo is_obj = ls_first_obj ).
    ls_second_obj = build_tree( lt_second_blob_nodes ).
    store_obj( iv_repo = mc_repo is_obj = ls_second_obj ).

    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-dir.
    ls_node-name  = 'one'.
    ls_node-sha1  = ls_first_obj-sha1.
    APPEND ls_node TO lt_root_nodes.

    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-dir.
    ls_node-name  = 'two'.
    ls_node-sha1  = ls_second_obj-sha1.
    APPEND ls_node TO lt_root_nodes.

    ls_root_obj = build_tree( lt_root_nodes ).
    store_obj( iv_repo = mc_repo is_obj = ls_root_obj ).

    lt_tree_objs = zcl_abapgit_ortec_walk_prep=>warm_trees(
      EXPORTING
        iv_repo_key  = mc_repo
        iv_root_tree = ls_root_obj-sha1
        it_objects   = VALUE zif_abapgit_definitions=>ty_objects_tt( )
      CHANGING
        ct_objects   = lt_ct_objects ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_ct_objects ) exp = 3 ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_tree_objs ) exp = 3 ).
  ENDMETHOD.

  METHOD missing_tree_raises.
    DATA ls_root_obj   TYPE zif_abapgit_definitions=>ty_object.
    DATA lt_root_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node       TYPE zcl_abapgit_git_pack=>ty_node.
    DATA lv_ok         TYPE abap_bool.
    DATA lt_ct_objects TYPE zif_abapgit_definitions=>ty_objects_tt.

    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-dir.
    ls_node-name  = 'missing'.
    ls_node-sha1  = '1111111111111111111111111111111111111111'.
    APPEND ls_node TO lt_root_nodes.

    ls_root_obj = build_tree( lt_root_nodes ).
    store_obj( iv_repo = mc_repo is_obj = ls_root_obj ).

    TRY.
        zcl_abapgit_ortec_walk_prep=>warm_trees(
          EXPORTING
            iv_repo_key  = mc_repo
            iv_root_tree = ls_root_obj-sha1
            it_objects   = VALUE zif_abapgit_definitions=>ty_objects_tt( )
          CHANGING
            ct_objects   = lt_ct_objects ).
        lv_ok = abap_false.
      CATCH zcx_abapgit_ortec_git.
        lv_ok = abap_true.
    ENDTRY.

    cl_abap_unit_assert=>assert_equals( act = lv_ok exp = abap_true ).
  ENDMETHOD.

  METHOD wrong_type_tree_raises.
    DATA ls_blob_obj   TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_root_obj   TYPE zif_abapgit_definitions=>ty_object.
    DATA lt_root_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node       TYPE zcl_abapgit_git_pack=>ty_node.
    DATA lv_ok         TYPE abap_bool.
    DATA lt_ct_objects TYPE zif_abapgit_definitions=>ty_objects_tt.

    ls_blob_obj = build_blob( 'blob' ).
    store_obj( iv_repo = mc_repo is_obj = ls_blob_obj ).

    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-dir.
    ls_node-name  = 'wrong'.
    ls_node-sha1  = ls_blob_obj-sha1.
    APPEND ls_node TO lt_root_nodes.

    ls_root_obj = build_tree( lt_root_nodes ).
    store_obj( iv_repo = mc_repo is_obj = ls_root_obj ).

    TRY.
        zcl_abapgit_ortec_walk_prep=>warm_trees(
          EXPORTING
            iv_repo_key  = mc_repo
            iv_root_tree = ls_root_obj-sha1
            it_objects   = VALUE zif_abapgit_definitions=>ty_objects_tt( )
          CHANGING
            ct_objects   = lt_ct_objects ).
        lv_ok = abap_false.
      CATCH zcx_abapgit_ortec_git.
        lv_ok = abap_true.
    ENDTRY.

    cl_abap_unit_assert=>assert_equals( act = lv_ok exp = abap_true ).
  ENDMETHOD.

  METHOD it_objects_tree_added.
    DATA ls_child_obj  TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_root_obj   TYPE zif_abapgit_definitions=>ty_object.
    DATA lt_child_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA lt_root_nodes  TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node       TYPE zcl_abapgit_git_pack=>ty_node.
    DATA lt_objects    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_ct_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_tree_objs  TYPE zif_abapgit_definitions=>ty_objects_tt.

    ls_child_obj = build_tree( lt_child_nodes ).
    APPEND ls_child_obj TO lt_objects.

    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-dir.
    ls_node-name  = 'child'.
    ls_node-sha1  = ls_child_obj-sha1.
    APPEND ls_node TO lt_root_nodes.

    ls_root_obj = build_tree( lt_root_nodes ).
    APPEND ls_root_obj TO lt_objects.

    lt_tree_objs = zcl_abapgit_ortec_walk_prep=>warm_trees(
      EXPORTING
        iv_repo_key  = mc_repo
        iv_root_tree = ls_root_obj-sha1
        it_objects   = lt_objects
      CHANGING
        ct_objects   = lt_ct_objects ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_ct_objects ) exp = 2 ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_tree_objs ) exp = 2 ).
  ENDMETHOD.

  METHOD duplicate_child_once.
    DATA ls_child_obj  TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_root_obj   TYPE zif_abapgit_definitions=>ty_object.
    DATA lt_child_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA lt_root_nodes  TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node       TYPE zcl_abapgit_git_pack=>ty_node.
    DATA lt_ct_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_tree_objs  TYPE zif_abapgit_definitions=>ty_objects_tt.

    ls_child_obj = build_tree( lt_child_nodes ).
    store_obj( iv_repo = mc_repo is_obj = ls_child_obj ).

    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-dir.
    ls_node-name  = 'one'.
    ls_node-sha1  = ls_child_obj-sha1.
    APPEND ls_node TO lt_root_nodes.

    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-dir.
    ls_node-name  = 'two'.
    ls_node-sha1  = ls_child_obj-sha1.
    APPEND ls_node TO lt_root_nodes.

    ls_root_obj = build_tree( lt_root_nodes ).
    store_obj( iv_repo = mc_repo is_obj = ls_root_obj ).

    lt_tree_objs = zcl_abapgit_ortec_walk_prep=>warm_trees(
      EXPORTING
        iv_repo_key  = mc_repo
        iv_root_tree = ls_root_obj-sha1
        it_objects   = VALUE zif_abapgit_definitions=>ty_objects_tt( )
      CHANGING
        ct_objects   = lt_ct_objects ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_ct_objects ) exp = 2 ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_tree_objs ) exp = 2 ).
  ENDMETHOD.

  METHOD complete_graph_is_noop.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_tree_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_tree_node TYPE zcl_abapgit_git_pack=>ty_node.
    DATA lv_blob_data TYPE xstring.
    DATA lv_blob_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_tree_data TYPE xstring.
    DATA lv_tree_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA ls_object TYPE zif_abapgit_definitions=>ty_object.
    DATA lt_ct_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_blob_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    lv_blob_data = '48656C6C6F'.
    lv_blob_sha = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).

    ls_tree_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_tree_node-name  = 'hello.txt'.
    ls_tree_node-sha1  = lv_blob_sha.
    APPEND ls_tree_node TO lt_tree_nodes.

    lv_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_tree_nodes ).
    lv_tree_sha = zcl_abapgit_hash=>sha1_tree( lv_tree_data ).

    ls_object-sha1 = lv_blob_sha.
    ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-data = lv_blob_data.
    APPEND ls_object TO lt_objects.

    ls_object-sha1 = lv_tree_sha.
    ls_object-type = zif_abapgit_git_definitions=>c_type-tree.
    ls_object-data = lv_tree_data.
    APPEND ls_object TO lt_objects.

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_blob_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_blob_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_tree_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-tree
      iv_data     = lv_tree_data ).

    lt_blob_sha1s = zcl_abapgit_ortec_walk_prep=>prewarm(
      EXPORTING
      iv_repo_key  = mc_repo
      iv_commit    = '0000000000000000000000000000000000000000'
      iv_url       = 'https://example.test/repo.git'
      iv_root_tree = lv_tree_sha
      it_objects   = lt_objects
      CHANGING ct_objects = lt_ct_objects ).

    cl_abap_unit_assert=>assert_initial( act = lt_blob_sha1s msg = 'Complete graph returns no warm blob SHA1s' ).
    cl_abap_unit_assert=>assert_initial( act = lt_ct_objects msg = 'Complete graph leaves warm object buffer empty' ).
  ENDMETHOD.

  METHOD fetch_blobs_bulk_consumes.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_remaining TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_blob_data TYPE xstring.
    DATA lv_first_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_second_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    lv_blob_data = '41'.
    lv_first_sha = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_first_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_blob_data ).

    lv_blob_data = '42'.
    lv_second_sha = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_second_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_blob_data ).

    APPEND lv_first_sha TO lt_sha1s.
    APPEND lv_second_sha TO lt_sha1s.
    lt_remaining = lt_sha1s.

    lt_objects = zcl_abapgit_ortec_walk_prep=>fetch_blobs_bulk(
      EXPORTING
      iv_repo_key         = mc_repo
      it_sha1s            = lt_sha1s
      CHANGING ct_remaining_sha1s = lt_remaining ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_objects ) exp = 2 msg = 'Bulk fetch returns every available blob' ).
    cl_abap_unit_assert=>assert_initial( act = lt_remaining msg = 'Bulk fetch drains the remaining SHA1 list' ).
  ENDMETHOD.

  METHOD bulk_drains_across_batches.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_remaining TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_blob_data TYPE xstring.
    DATA lv_large_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_small_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    lv_blob_data = zcl_abapgit_convert=>string_to_xstring_utf8( iv_string = repeat( val = 'A' occ = 268435457 ) ).
    lv_large_sha = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_large_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_blob_data ).

    lv_blob_data = '42'.
    lv_small_sha = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_small_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_blob_data ).

    APPEND lv_large_sha TO lt_sha1s.
    APPEND lv_small_sha TO lt_sha1s.
    lt_remaining = lt_sha1s.

    lt_objects = zcl_abapgit_ortec_walk_prep=>fetch_blobs_bulk(
      EXPORTING
      iv_repo_key         = mc_repo
      it_sha1s            = lt_sha1s
      CHANGING ct_remaining_sha1s = lt_remaining ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_objects ) exp = 1 msg = 'First batch returns only the oversized blob' ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_remaining ) exp = 1 msg = 'The small blob remains for a later batch' ).
    READ TABLE lt_remaining WITH KEY table_line = lv_small_sha TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( msg = 'The small blob is still pending after the first batch' ).

    CLEAR lt_objects.
    lt_objects = zcl_abapgit_ortec_walk_prep=>fetch_blobs_bulk(
      EXPORTING
      iv_repo_key         = mc_repo
      it_sha1s            = lt_remaining
      CHANGING ct_remaining_sha1s = lt_remaining ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_objects ) exp = 1 msg = 'Second batch returns the remaining small blob' ).
    cl_abap_unit_assert=>assert_initial( act = lt_remaining msg = 'The remaining SHA1 list is fully drained after the second batch' ).
  ENDMETHOD.

  METHOD oversized_blob_single_batch.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_remaining TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_blob_data TYPE xstring.
    DATA lv_blob_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    lv_blob_data = zcl_abapgit_convert=>string_to_xstring_utf8( iv_string = repeat( val = 'A' occ = 268435457 ) ).
    lv_blob_sha = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_blob_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_blob_data ).

    APPEND lv_blob_sha TO lt_sha1s.
    lt_remaining = lt_sha1s.

    lt_objects = zcl_abapgit_ortec_walk_prep=>fetch_blobs_bulk(
      EXPORTING
      iv_repo_key         = mc_repo
      it_sha1s            = lt_sha1s
      CHANGING ct_remaining_sha1s = lt_remaining ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_objects ) exp = 1 msg = 'Oversized blob still returns a single object' ).
    cl_abap_unit_assert=>assert_initial( act = lt_remaining msg = 'Oversized blob still drains remaining SHA1s' ).
  ENDMETHOD.
ENDCLASS.
