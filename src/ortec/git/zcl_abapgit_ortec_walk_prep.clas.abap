CLASS zcl_abapgit_ortec_walk_prep DEFINITION
  PUBLIC FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    CLASS-METHODS prewarm
      IMPORTING iv_repo_key          TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                iv_commit            TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_url               TYPE string
                iv_root_tree         TYPE zif_abapgit_git_definitions=>ty_sha1
                it_objects           TYPE zif_abapgit_definitions=>ty_objects_tt
      CHANGING  ct_objects           TYPE zif_abapgit_definitions=>ty_objects_tt
      RETURNING VALUE(rt_blob_sha1s) TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS fetch_blobs_bulk
      IMPORTING iv_repo_key        TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                it_sha1s           TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      CHANGING  ct_remaining_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RETURNING VALUE(rt_objects)  TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_ortec_git.

  PRIVATE SECTION.
    CLASS-METHODS warm_trees
      IMPORTING iv_repo_key            TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                iv_root_tree           TYPE zif_abapgit_git_definitions=>ty_sha1
                it_objects             TYPE zif_abapgit_definitions=>ty_objects_tt
      CHANGING  ct_objects             TYPE zif_abapgit_definitions=>ty_objects_tt
      RETURNING VALUE(rt_tree_objects) TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS collect_blob_shas
      IMPORTING it_tree_objects      TYPE zif_abapgit_definitions=>ty_objects_tt
      RETURNING VALUE(rt_blob_sha1s) TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS topup_missing_blobs
      IMPORTING iv_repo_key   TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                iv_url        TYPE string
                iv_commit     TYPE zif_abapgit_git_definitions=>ty_sha1
                it_blob_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS has_complete_object_graph
      IMPORTING iv_root_tree       TYPE zif_abapgit_git_definitions=>ty_sha1
                it_objects         TYPE zif_abapgit_definitions=>ty_objects_tt
      RETURNING VALUE(rv_complete) TYPE abap_bool.
ENDCLASS.


CLASS zcl_abapgit_ortec_walk_prep IMPLEMENTATION.
  METHOD collect_blob_shas.
    DATA lt_nodes      TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA lt_seen_blobs TYPE HASHED TABLE OF zif_abapgit_git_definitions=>ty_sha1 WITH UNIQUE KEY table_line.
    DATA lt_blob_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    FIELD-SYMBOLS <ls_node>        LIKE LINE OF lt_nodes.

    FIELD-SYMBOLS <ls_tree_object> LIKE LINE OF it_tree_objects.

    LOOP AT it_tree_objects ASSIGNING <ls_tree_object>.
      TRY.
          lt_nodes = zcl_abapgit_git_pack=>decode_tree( <ls_tree_object>-data ).
        CATCH zcx_abapgit_exception.
          zcx_abapgit_ortec_git=>raise( |Tree { <ls_tree_object>-sha1 } could not be decoded| ).
      ENDTRY.

      LOOP AT lt_nodes ASSIGNING <ls_node>.
        CASE <ls_node>-chmod.
          WHEN zif_abapgit_git_definitions=>c_chmod-file
            OR zif_abapgit_git_definitions=>c_chmod-executable
            OR zif_abapgit_git_definitions=>c_chmod-symbolic_link.
            IF NOT line_exists( lt_seen_blobs[ table_line = <ls_node>-sha1 ] ).
              INSERT <ls_node>-sha1 INTO TABLE lt_seen_blobs.
              APPEND <ls_node>-sha1 TO lt_blob_sha1s.
            ENDIF.
          WHEN OTHERS.
            CONTINUE.
        ENDCASE.
      ENDLOOP.
    ENDLOOP.

    rt_blob_sha1s = lt_blob_sha1s.
  ENDMETHOD.

  METHOD fetch_blobs_bulk.
    DATA lt_metadata    TYPE STANDARD TABLE OF zaog_obj_store WITH DEFAULT KEY.
    DATA lr_sha1s       TYPE RANGE OF zaog_obj_store-obj_sha1.
    DATA ls_metadata    LIKE LINE OF lt_metadata.
    DATA lv_this_bytes  TYPE i.
    DATA lv_budget      TYPE i VALUE 268435456.
    DATA lt_candidates  TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv_total_bytes TYPE i.
    DATA ls_object      TYPE zif_abapgit_definitions=>ty_object.
    FIELD-SYMBOLS <lv_sha1> LIKE LINE OF it_sha1s.

    IF iv_repo_key IS INITIAL OR it_sha1s IS INITIAL.
      RETURN.
    ENDIF.

    LOOP AT it_sha1s ASSIGNING <lv_sha1> WHERE table_line IS NOT INITIAL.
      APPEND VALUE #( sign   = 'I'
                      option = 'EQ'
                      low    = <lv_sha1> ) TO lr_sha1s.
    ENDLOOP.

    IF lr_sha1s IS INITIAL.
      RETURN.
    ENDIF.

    SELECT obj_sha1, obj_size FROM zaog_obj_store
      INTO CORRESPONDING FIELDS OF TABLE @lt_metadata
      WHERE repo_key  = @iv_repo_key
        AND obj_sha1 IN @lr_sha1s
        AND status    = 'R'.

    LOOP AT it_sha1s ASSIGNING <lv_sha1> WHERE table_line IS NOT INITIAL.
      READ TABLE lt_metadata INTO ls_metadata WITH KEY obj_sha1 = <lv_sha1>.
      IF sy-subrc <> 0.
        " Genuinely missing from the object store even after prewarm's own
        " topup_missing_blobs step - this must never be silently skipped
        " (that would leave a hole in rt_files, violating the
        " "not-buffered != deletion" invariant) and must still be drained
        " from ct_remaining_sha1s, or pull()'s batching WHILE loop would
        " spin forever on a SHA1 that can never be satisfied.
        READ TABLE ct_remaining_sha1s WITH KEY table_line = <lv_sha1> TRANSPORTING NO FIELDS.
        IF sy-subrc = 0.
          DELETE ct_remaining_sha1s INDEX sy-tabix.
        ENDIF.
        zcx_abapgit_ortec_git=>raise( |Blob { <lv_sha1> } not found in object store| ).
      ENDIF.

      lv_this_bytes = ls_metadata-obj_size.
      IF lv_this_bytes > lv_budget.
        CLEAR lt_candidates.
        APPEND <lv_sha1> TO lt_candidates.
        EXIT.
      ENDIF.

      IF lv_total_bytes + lv_this_bytes > lv_budget.
        EXIT.
      ENDIF.

      APPEND <lv_sha1> TO lt_candidates.
      lv_total_bytes += lv_this_bytes.
    ENDLOOP.

    IF lt_candidates IS INITIAL.
      RETURN.
    ENDIF.

    DATA(lt_objects) = zcl_abapgit_ortec_obj_store=>get_objects(
                           iv_repo_key   = iv_repo_key
                           it_sha1s      = lt_candidates
                           iv_bulk_fetch = abap_true ).

    LOOP AT lt_objects INTO ls_object.
      IF NOT line_exists( lt_candidates[ table_line = ls_object-sha1 ] ).
        CONTINUE.
      ENDIF.
      READ TABLE ct_remaining_sha1s WITH KEY table_line = ls_object-sha1 TRANSPORTING NO FIELDS.
      IF sy-subrc = 0.
        DELETE ct_remaining_sha1s INDEX sy-tabix.
      ENDIF.
      APPEND ls_object TO rt_objects.
    ENDLOOP.
  ENDMETHOD.

  METHOD has_complete_object_graph.
    DATA lt_queue       TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv_tree_sha    TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA ls_tree_object TYPE zif_abapgit_definitions=>ty_object.
    DATA lt_nodes       TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node        TYPE zcl_abapgit_git_pack=>ty_node.

    IF iv_root_tree IS INITIAL.
      rv_complete = abap_true.
      RETURN.
    ENDIF.

    APPEND iv_root_tree TO lt_queue.

    WHILE lt_queue IS NOT INITIAL.
      READ TABLE lt_queue INDEX 1 INTO lv_tree_sha.
      DELETE lt_queue INDEX 1.

      READ TABLE it_objects INTO ls_tree_object
           WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-tree
                                    sha1 = lv_tree_sha.
      IF sy-subrc <> 0.
        rv_complete = abap_false.
        RETURN.
      ENDIF.

      TRY.
          lt_nodes = zcl_abapgit_git_pack=>decode_tree( ls_tree_object-data ).
        CATCH zcx_abapgit_exception.
          rv_complete = abap_false.
          RETURN.
      ENDTRY.

      LOOP AT lt_nodes INTO ls_node.
        CASE ls_node-chmod.
          WHEN zif_abapgit_git_definitions=>c_chmod-dir.
            APPEND ls_node-sha1 TO lt_queue.
          WHEN zif_abapgit_git_definitions=>c_chmod-file
            OR zif_abapgit_git_definitions=>c_chmod-executable
            OR zif_abapgit_git_definitions=>c_chmod-symbolic_link
            OR zif_abapgit_git_definitions=>c_chmod-submodule.
            IF NOT line_exists( it_objects[
                                              KEY type
                                              type = zif_abapgit_git_definitions=>c_type-blob
                                              sha1 = ls_node-sha1 ] ).
              rv_complete = abap_false.
              RETURN.
            ENDIF.
          WHEN OTHERS.
            rv_complete = abap_false.
            RETURN.
        ENDCASE.
      ENDLOOP.
    ENDWHILE.

    rv_complete = abap_true.
  ENDMETHOD.

  METHOD prewarm.
    DATA lt_tree_objects  TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_blob_sha1s    TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_missing_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    IF iv_url IS INITIAL OR iv_root_tree IS INITIAL.
      RETURN.
    ENDIF.

    IF has_complete_object_graph(
           iv_root_tree = iv_root_tree
           it_objects   = it_objects ) = abap_true.
      RETURN.
    ENDIF.

    lt_tree_objects = warm_trees(
                        EXPORTING
                          iv_repo_key  = iv_repo_key
                          iv_root_tree = iv_root_tree
                          it_objects   = it_objects
                        CHANGING
                          ct_objects   = ct_objects ).

    lt_blob_sha1s = collect_blob_shas( lt_tree_objects ).
    rt_blob_sha1s = lt_blob_sha1s.

    IF lt_blob_sha1s IS INITIAL.
      RETURN.
    ENDIF.

    lt_missing_sha1s = zcl_abapgit_ortec_obj_store=>get_missing_sha1s(
                           iv_repo_key = iv_repo_key
                           it_sha1s    = lt_blob_sha1s ).

    IF lt_missing_sha1s IS INITIAL.
      RETURN.
    ENDIF.

    topup_missing_blobs(
        iv_repo_key   = iv_repo_key
        iv_url        = iv_url
        iv_commit     = iv_commit
        it_blob_sha1s = lt_missing_sha1s ).
  ENDMETHOD.

  METHOD topup_missing_blobs.
    DATA lt_missing_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    lt_missing_sha1s = zcl_abapgit_ortec_obj_store=>get_missing_sha1s(
                           iv_repo_key = iv_repo_key
                           it_sha1s    = it_blob_sha1s ).

    IF lt_missing_sha1s IS INITIAL.
      RETURN.
    ENDIF.

    zcl_abapgit_ortec_missing_obj=>ensure_available(
        iv_repo_key = iv_repo_key
        iv_url      = iv_url
        iv_commit   = iv_commit
        it_sha1s    = lt_missing_sha1s ).
  ENDMETHOD.

  METHOD warm_trees.
    DATA lt_tree_objects  TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_nodes         TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA lt_current_trees TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_seen_trees    TYPE HASHED TABLE OF zif_abapgit_git_definitions=>ty_sha1 WITH UNIQUE KEY table_line.
    DATA lt_next_trees    TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv_tree_sha      TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA ls_tree_object   TYPE zif_abapgit_definitions=>ty_object.

    FIELD-SYMBOLS <ls_node> LIKE LINE OF lt_nodes.

    APPEND iv_root_tree TO lt_current_trees.
    INSERT iv_root_tree INTO TABLE lt_seen_trees.

    WHILE lt_current_trees IS NOT INITIAL.
      CLEAR lt_next_trees.
      LOOP AT lt_current_trees INTO lv_tree_sha.
        CLEAR ls_tree_object.
        READ TABLE it_objects INTO ls_tree_object
             WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-tree
                                      sha1 = lv_tree_sha.
        IF sy-subrc <> 0.
          TRY.
              ls_tree_object = zcl_abapgit_ortec_obj_store=>get_object(
                                   iv_repo_key = iv_repo_key
                                   iv_sha1     = lv_tree_sha ).
            CATCH zcx_abapgit_ortec_git.
              CONTINUE.
          ENDTRY.
        ENDIF.

        IF ls_tree_object-type <> zif_abapgit_git_definitions=>c_type-tree.
          CONTINUE.
        ENDIF.

        IF NOT line_exists( ct_objects[
                                          KEY type
                                          type = zif_abapgit_git_definitions=>c_type-tree
                                          sha1 = ls_tree_object-sha1 ] ).
          APPEND ls_tree_object TO ct_objects.
          APPEND ls_tree_object TO lt_tree_objects.
        ENDIF.

        TRY.
            lt_nodes = zcl_abapgit_git_pack=>decode_tree( ls_tree_object-data ).
          CATCH zcx_abapgit_exception.
            zcx_abapgit_ortec_git=>raise( |Tree { ls_tree_object-sha1 } could not be decoded| ).
        ENDTRY.

        LOOP AT lt_nodes ASSIGNING <ls_node>.
          CASE <ls_node>-chmod.
            WHEN zif_abapgit_git_definitions=>c_chmod-dir.
              IF NOT line_exists( lt_seen_trees[ table_line = <ls_node>-sha1 ] ).
                INSERT <ls_node>-sha1 INTO TABLE lt_seen_trees.
                APPEND <ls_node>-sha1 TO lt_next_trees.
              ENDIF.
            WHEN OTHERS.
              CONTINUE.
          ENDCASE.
        ENDLOOP.
      ENDLOOP.
      lt_current_trees = lt_next_trees.
    ENDWHILE.

    rt_tree_objects = lt_tree_objects.
  ENDMETHOD.
ENDCLASS.
