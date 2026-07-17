CLASS zcl_abapgit_ortec_porcelain DEFINITION
  PUBLIC FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    TYPES:
      BEGIN OF ty_pull_result,
        files   TYPE zif_abapgit_git_definitions=>ty_files_tt,
        objects TYPE zif_abapgit_definitions=>ty_objects_tt,
        commit  TYPE zif_abapgit_git_definitions=>ty_sha1,
      END OF ty_pull_result.

    CLASS-METHODS pull_by_branch
      IMPORTING iv_url           TYPE string
                iv_branch_name   TYPE string
                iv_deepen_level  TYPE i      DEFAULT 1
                iv_pull_url      TYPE string OPTIONAL
      RETURNING VALUE(rs_result) TYPE ty_pull_result
      RAISING   zcx_abapgit_exception.

    CLASS-METHODS pull_by_commit
      IMPORTING iv_url           TYPE string
                iv_commit_hash   TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_deepen_level  TYPE i      DEFAULT 1
                iv_pull_url      TYPE string OPTIONAL
      RETURNING VALUE(rs_result) TYPE ty_pull_result
      RAISING   zcx_abapgit_exception.

  PRIVATE SECTION.
    CLASS-METHODS pull
      IMPORTING iv_commit         TYPE zif_abapgit_git_definitions=>ty_sha1
                VALUE(it_objects) TYPE zif_abapgit_definitions=>ty_objects_tt
                iv_repo_key       TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key OPTIONAL
                iv_url            TYPE string                                   OPTIONAL
      RETURNING VALUE(rt_files)   TYPE zif_abapgit_git_definitions=>ty_files_tt
      RAISING   zcx_abapgit_exception.

    CLASS-METHODS walk
      IMPORTING it_objects       TYPE zif_abapgit_definitions=>ty_objects_tt
                iv_sha1          TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_path          TYPE string
                iv_repo_key      TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key    OPTIONAL
                iv_url           TYPE string                                      OPTIONAL
                iv_commit        TYPE zif_abapgit_git_definitions=>ty_sha1        OPTIONAL
                it_blob_objects  TYPE zif_abapgit_definitions=>ty_objects_tt      OPTIONAL
                it_blob_manifest TYPE zif_abapgit_git_definitions=>ty_expanded_tt OPTIONAL
      CHANGING  ct_files         TYPE zif_abapgit_git_definitions=>ty_files_tt
      RAISING   zcx_abapgit_exception.

    CLASS-METHODS walk_tree
      IMPORTING it_objects         TYPE zif_abapgit_definitions=>ty_objects_tt
                iv_tree            TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_base            TYPE string
                iv_repo_key        TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key OPTIONAL
      RETURNING VALUE(rt_expanded) TYPE zif_abapgit_git_definitions=>ty_expanded_tt
      RAISING   zcx_abapgit_exception.
ENDCLASS.


CLASS zcl_abapgit_ortec_porcelain IMPLEMENTATION.
  METHOD pull.
    DATA lt_blob_manifest   TYPE zif_abapgit_git_definitions=>ty_expanded_tt.
    DATA lt_batch_manifest  TYPE zif_abapgit_git_definitions=>ty_expanded_tt.
    DATA ls_object          TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_commit          TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lv_root_tree       TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lt_blob_sha1s      TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_objects         TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_blob_manifest   LIKE LINE OF lt_blob_manifest.
    DATA lt_remaining_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_batch_objects   TYPE zif_abapgit_definitions=>ty_objects_tt.

    READ TABLE it_objects INTO ls_object
         WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-commit
                                  sha1 = iv_commit.
    IF sy-subrc <> 0.
      zcx_abapgit_exception=>raise( 'Commit/Branch not found.' ).
    ENDIF.

    ls_commit = zcl_abapgit_git_pack=>decode_commit( ls_object-data ).
    lv_root_tree = ls_commit-tree.

    TRY.
        lt_blob_sha1s = zcl_abapgit_ortec_walk_prep=>prewarm(
                          EXPORTING
                            iv_repo_key  = iv_repo_key
                            iv_commit    = iv_commit
                            iv_url       = iv_url
                            iv_root_tree = lv_root_tree
                            it_objects   = it_objects
                          CHANGING
                            ct_objects   = lt_objects ).
      CATCH zcx_abapgit_ortec_git INTO DATA(lx_prewarm).
        zcx_abapgit_exception=>raise_with_text( lx_prewarm ).
    ENDTRY.

    APPEND LINES OF lt_objects TO it_objects.

    lt_blob_manifest = walk_tree(
                           it_objects  = it_objects
                           iv_tree     = lv_root_tree
                           iv_base     = '/'
                           iv_repo_key = iv_repo_key ).

    CLEAR lt_blob_sha1s.
    LOOP AT lt_blob_manifest INTO ls_blob_manifest.
      IF ls_blob_manifest-chmod <> zif_abapgit_git_definitions=>c_chmod-file.
        CONTINUE.
      ENDIF.

      IF NOT line_exists( it_objects[
                                        KEY type
                                        type = zif_abapgit_git_definitions=>c_type-blob
                                        sha1 = ls_blob_manifest-sha1 ] ).
        " Only blobs NOT already resident in it_objects need batch-fetching -
        " walk_tree's manifest lists EVERY reachable blob unconditionally, so
        " without this filter a COMPLETE (non-sparse) it_objects would still
        " force the batching path for every blob in the repo, defeating the
        " no-op equivalence for today's standard full-pull case and risking
        " an infinite/empty-yield loop for a first-ever pull whose blobs
        " live only in it_objects, not yet in the persistent object store.
        APPEND ls_blob_manifest-sha1 TO lt_blob_sha1s.
      ENDIF.
    ENDLOOP.

    lt_remaining_sha1s = lt_blob_sha1s.
    IF lt_remaining_sha1s IS INITIAL.
      walk(
        EXPORTING
          it_objects  = it_objects
          iv_sha1     = lv_root_tree
          iv_path     = '/'
          iv_repo_key = iv_repo_key
          iv_url      = iv_url
          iv_commit   = iv_commit
        CHANGING
          ct_files    = rt_files ).
    ELSE.
      WHILE lt_remaining_sha1s IS NOT INITIAL.
        CLEAR lt_batch_objects.
        CLEAR lt_batch_manifest.
        TRY.
            lt_batch_objects = zcl_abapgit_ortec_walk_prep=>fetch_blobs_bulk(
                                 EXPORTING
                                   iv_repo_key        = iv_repo_key
                                   it_sha1s           = lt_remaining_sha1s
                                 CHANGING
                                   ct_remaining_sha1s = lt_remaining_sha1s ).
          CATCH zcx_abapgit_ortec_git INTO DATA(lx_fetch_blobs).
            zcx_abapgit_exception=>raise_with_text( lx_fetch_blobs ).
        ENDTRY.

        LOOP AT lt_batch_objects INTO ls_object.
          READ TABLE lt_blob_manifest INTO ls_blob_manifest
               WITH KEY sha1 = ls_object-sha1.
          IF sy-subrc = 0.
            APPEND ls_blob_manifest TO lt_batch_manifest.
          ENDIF.
        ENDLOOP.

        walk(
          EXPORTING
            it_objects       = it_objects
            iv_sha1          = lv_root_tree
            iv_path          = '/'
            iv_repo_key      = iv_repo_key
            iv_url           = iv_url
            iv_commit        = iv_commit
            it_blob_objects  = lt_batch_objects
            it_blob_manifest = lt_batch_manifest
          CHANGING
            ct_files         = rt_files ).
      ENDWHILE.
    ENDIF.
  ENDMETHOD.

  METHOD pull_by_branch.
    DATA lv_ortec_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.
    DATA lx_pull           TYPE REF TO zcx_abapgit_exception.
    DATA lv_pull_error     TYPE string.

    zcl_abapgit_git_transport=>upload_pack_by_branch(
      EXPORTING
        iv_url          = iv_url
        iv_branch_name  = iv_branch_name
        iv_deepen_level = iv_deepen_level
      IMPORTING
        et_objects      = rs_result-objects
        ev_branch       = rs_result-commit ).

    IF zcl_abapgit_ortec_git_switch=>is_active_for_repo( iv_url ) = abap_true.
      lv_ortec_repo_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( iv_url ).
    ELSE.
      lv_ortec_repo_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( iv_url ).
    ENDIF.

    TRY.
        rs_result-files = pull(
                              iv_commit   = rs_result-commit
                              it_objects  = rs_result-objects
                              iv_repo_key = lv_ortec_repo_key
                              iv_url      = iv_pull_url ).
      CATCH zcx_abapgit_exception INTO lx_pull.
        lv_pull_error = lx_pull->get_text( ).
        IF     zcl_abapgit_ortec_git_switch=>is_active_for_repo( iv_url )  = abap_true
           AND lv_ortec_repo_key IS NOT INITIAL
           AND lv_pull_error CS 'Walk,'.
          " The walk failed because the persistent store has some objects
          " but not every blob/tree reachable from the fetched commit, even
          " though ZAOG_COMMIT_HIST/ZAOG_REPO_STATE claim otherwise for at
          " least one advertised have. Repair strategy: invalidate ALL
          " recorded history/have-state for the whole repo (not just this
          " branch's fetch_commit) so the retry cannot advertise ANY commit
          " as already complete, forcing the server to fall back to a
          " full/deepen, self-contained pack. Per-branch/per-commit
          " invalidation is not reliable here because haves are shared
          " across all branches of a repo, and we don't know which shared
          " ancestor is actually incomplete.
          " The object store itself is kept intact: its objects still serve
          " as delta-base context inside decode_and_persist, and other
          " branches cached for the same repo simply redo have-negotiation
          " on their own next fetch.
          TRY.
              zcl_abapgit_ortec_repo_state=>invalidate_all_history( iv_repo_key = lv_ortec_repo_key ).
              COMMIT WORK.

              CLEAR rs_result.
              zcl_abapgit_git_transport=>upload_pack_by_branch(
                EXPORTING
                  iv_url          = iv_url
                  iv_branch_name  = iv_branch_name
                  iv_deepen_level = iv_deepen_level
                IMPORTING
                  et_objects      = rs_result-objects
                  ev_branch       = rs_result-commit ).

              rs_result-files = pull(
                                    iv_commit   = rs_result-commit
                                    it_objects  = rs_result-objects
                                    iv_repo_key = lv_ortec_repo_key
                                    iv_url      = iv_pull_url ).
            CATCH zcx_abapgit_ortec_git.
              RAISE EXCEPTION lx_pull.
            CATCH zcx_abapgit_exception.
              RAISE EXCEPTION lx_pull.
          ENDTRY.
        ENDIF.
        RAISE EXCEPTION lx_pull.
    ENDTRY.

    " ORTEC: persist objects in persistent store after successful pull - this
    " mirrors the standard zcl_abapgit_git_porcelain=>pull_by_branch's own
    " post-pull persistence hook exactly. Without this call, the persistent
    " object store/index (ZAOG_OBJ_STORE/ZAOG_OBJ_INDEX/ZAOG_REPO_STATE) never
    " learns about files pulled through this mirror, leaving the Stage/Diff/
    " status overview's filtered read path comparing against a STALE snapshot
    " that predates this pull - the exact cause of a live bug (2026-07-17)
    " where freshly-pulled, unchanged classes were wrongly shown with a
    " "deleted in remote" status badge, even though a direct diff correctly
    " reported no differences.
    TRY.
        zcl_abapgit_ortec_fastpath=>persist_pull_result(
          iv_url         = iv_url
          iv_branch_name = iv_branch_name
          iv_commit      = rs_result-commit
          it_objects     = rs_result-objects
          iv_repo_key    = lv_ortec_repo_key ).
      CATCH zcx_abapgit_ortec_git.
        " ORTEC: persistence failure is non-critical, continue normally
    ENDTRY.
  ENDMETHOD.

  METHOD pull_by_commit.
    DATA lv_ortec_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.

    zcl_abapgit_git_transport=>upload_pack_by_commit(
      EXPORTING
        iv_url          = iv_url
        iv_hash         = iv_commit_hash
        iv_deepen_level = iv_deepen_level
      IMPORTING
        et_objects      = rs_result-objects
        ev_commit       = rs_result-commit ).

    IF zcl_abapgit_ortec_git_switch=>is_active_for_repo( iv_url ) = abap_true.
      lv_ortec_repo_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( iv_url ).
    ELSE.
      lv_ortec_repo_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( iv_url ).
    ENDIF.

    rs_result-files = pull(
                          iv_commit   = rs_result-commit
                          it_objects  = rs_result-objects
                          iv_repo_key = lv_ortec_repo_key
                          iv_url      = iv_pull_url ).
  ENDMETHOD.

  METHOD walk.
    DATA lt_nodes        TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_ortec_object TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_file_path    TYPE string.
    DATA lv_path         TYPE string.

    FIELD-SYMBOLS <ls_node> LIKE LINE OF lt_nodes.

    DATA ls_file          LIKE LINE OF ct_files.
    DATA ls_blob_manifest LIKE LINE OF it_blob_manifest.

    FIELD-SYMBOLS <ls_tree> LIKE LINE OF it_objects.
    FIELD-SYMBOLS <ls_blob> LIKE LINE OF it_objects.

    READ TABLE it_objects ASSIGNING <ls_tree>
         WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-tree
                                  sha1 = iv_sha1.
    IF sy-subrc = 0.
      lt_nodes = zcl_abapgit_git_pack=>decode_tree( <ls_tree>-data ).
    ELSE.
      TRY.
          ls_ortec_object = zcl_abapgit_ortec_obj_store=>get_object(
                                iv_repo_key = iv_repo_key
                                iv_sha1     = iv_sha1 ).
          IF ls_ortec_object-type <> zif_abapgit_git_definitions=>c_type-tree.
            zcx_abapgit_exception=>raise( 'Walk, tree not found' ).
          ENDIF.
          lt_nodes = zcl_abapgit_git_pack=>decode_tree( ls_ortec_object-data ).
        CATCH zcx_abapgit_ortec_git.
          zcx_abapgit_exception=>raise( 'Walk, tree not found' ).
      ENDTRY.
    ENDIF.

    LOOP AT lt_nodes ASSIGNING <ls_node>.
      IF <ls_node>-chmod <> zif_abapgit_git_definitions=>c_chmod-file.
        CONTINUE.
      ENDIF.

      CLEAR ls_file.
      lv_file_path = iv_path.
      ls_file-filename = <ls_node>-name.
      READ TABLE it_blob_manifest INTO ls_blob_manifest
           WITH KEY sha1 = <ls_node>-sha1.
      IF sy-subrc = 0.
        lv_file_path = ls_blob_manifest-path.
        ls_file-filename = ls_blob_manifest-name.
      ENDIF.

      IF it_blob_objects IS NOT INITIAL.
        READ TABLE it_blob_objects INTO ls_ortec_object
             WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-blob
                                      sha1 = <ls_node>-sha1.
        IF sy-subrc = 0.
          ls_file-path = lv_file_path.
          ls_file-data = ls_ortec_object-data.
          ls_file-sha1 = ls_ortec_object-sha1.
          APPEND ls_file TO ct_files.
          CONTINUE.
        ENDIF.
        CONTINUE.
      ENDIF.

      READ TABLE it_objects ASSIGNING <ls_blob>
           WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-blob
                                    sha1 = <ls_node>-sha1.
      IF sy-subrc = 0.
        ls_file-path = lv_file_path.
        ls_file-data = <ls_blob>-data.
        ls_file-sha1 = <ls_blob>-sha1.
        APPEND ls_file TO ct_files.
        CONTINUE.
      ENDIF.

      TRY.
          ls_ortec_object = zcl_abapgit_ortec_obj_store=>get_object(
                                iv_repo_key = iv_repo_key
                                iv_sha1     = <ls_node>-sha1 ).
          IF ls_ortec_object-type <> zif_abapgit_git_definitions=>c_type-blob.
            zcx_abapgit_exception=>raise( 'Walk, blob not found' ).
          ENDIF.
          ls_file-path = lv_file_path.
          ls_file-data = ls_ortec_object-data.
          ls_file-sha1 = ls_ortec_object-sha1.
          APPEND ls_file TO ct_files.
          CONTINUE.
        CATCH zcx_abapgit_ortec_git.
          zcx_abapgit_exception=>raise( 'Walk, blob not found' ).
      ENDTRY.
    ENDLOOP.

    LOOP AT lt_nodes ASSIGNING <ls_node> WHERE chmod = zif_abapgit_git_definitions=>c_chmod-dir.
      CONCATENATE iv_path <ls_node>-name '/' INTO lv_path.

      walk(
        EXPORTING
          it_objects       = it_objects
          iv_sha1          = <ls_node>-sha1
          iv_path          = lv_path
          iv_repo_key      = iv_repo_key
          iv_url           = iv_url
          iv_commit        = iv_commit
          it_blob_objects  = it_blob_objects
          it_blob_manifest = it_blob_manifest
        CHANGING
          ct_files         = ct_files ).
    ENDLOOP.
  ENDMETHOD.

  METHOD walk_tree.
    DATA lt_nodes        TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_object       TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_ortec_object TYPE zif_abapgit_definitions=>ty_object.
    DATA lt_expanded     TYPE zif_abapgit_git_definitions=>ty_expanded_tt.

    FIELD-SYMBOLS <ls_node> LIKE LINE OF lt_nodes.
    FIELD-SYMBOLS <ls_exp>  LIKE LINE OF rt_expanded.

    READ TABLE it_objects INTO ls_object
         WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-tree
                                  sha1 = iv_tree.
    IF sy-subrc = 0.
      lt_nodes = zcl_abapgit_git_pack=>decode_tree( ls_object-data ).
    ELSE.
      TRY.
          ls_ortec_object = zcl_abapgit_ortec_obj_store=>get_object(
                                iv_repo_key = iv_repo_key
                                iv_sha1     = iv_tree ).
          IF ls_ortec_object-type <> zif_abapgit_git_definitions=>c_type-tree.
            zcx_abapgit_exception=>raise( 'walk_tree, tree not found' ).
          ENDIF.
          lt_nodes = zcl_abapgit_git_pack=>decode_tree( ls_ortec_object-data ).
        CATCH zcx_abapgit_ortec_git.
          zcx_abapgit_exception=>raise( 'tree not found' ).
      ENDTRY.
    ENDIF.

    LOOP AT lt_nodes ASSIGNING <ls_node>.
      CASE <ls_node>-chmod.
        WHEN zif_abapgit_git_definitions=>c_chmod-file
            OR zif_abapgit_git_definitions=>c_chmod-executable
            OR zif_abapgit_git_definitions=>c_chmod-symbolic_link
            OR zif_abapgit_git_definitions=>c_chmod-submodule.
          APPEND INITIAL LINE TO rt_expanded ASSIGNING <ls_exp>.
          <ls_exp>-path  = iv_base.
          <ls_exp>-name  = <ls_node>-name.
          <ls_exp>-sha1  = <ls_node>-sha1.
          <ls_exp>-chmod = <ls_node>-chmod.
        WHEN zif_abapgit_git_definitions=>c_chmod-dir.
          lt_expanded = walk_tree(
                            it_objects  = it_objects
                            iv_tree     = <ls_node>-sha1
                            iv_base     = |{ iv_base }{ <ls_node>-name }/|
                            iv_repo_key = iv_repo_key ).
          APPEND LINES OF lt_expanded TO rt_expanded.
        WHEN OTHERS.
          zcx_abapgit_exception=>raise( |walk_tree: unknown chmod { <ls_node>-chmod }| ).
      ENDCASE.
    ENDLOOP.
  ENDMETHOD.
ENDCLASS.
