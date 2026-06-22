"! <p class="shorttext synchronized">ORTEC Git Object Store - Performance Optimized</p>
"! Strategies 2-5: Session cache, bulk preload (55K threshold), cursor streaming, DB optimization
CLASS zcl_abapgit_ortec_obj_store DEFINITION
  PUBLIC FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    TYPES ty_repo_key TYPE c LENGTH 12.

    CLASS-METHODS store_object
      IMPORTING iv_repo_key TYPE ty_repo_key
                iv_sha1     TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_type     TYPE zif_abapgit_git_definitions=>ty_type
                iv_data     TYPE xstring
                iv_pack_id  TYPE c OPTIONAL
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS store_objects
      IMPORTING iv_repo_key TYPE ty_repo_key
                it_objects  TYPE zif_abapgit_definitions=>ty_objects_tt
                iv_pack_id  TYPE c OPTIONAL
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS get_object
      IMPORTING iv_repo_key      TYPE ty_repo_key OPTIONAL
                iv_sha1          TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rs_object) TYPE zif_abapgit_definitions=>ty_object
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS get_objects
      IMPORTING iv_repo_key       TYPE ty_repo_key
                it_sha1s          TYPE zif_abapgit_git_definitions=>ty_sha1_tt
                iv_bulk_fetch     TYPE abap_bool DEFAULT abap_false
      RETURNING VALUE(rt_objects) TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS get_reachable_objects
      IMPORTING iv_repo_key       TYPE ty_repo_key
                iv_commit         TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rt_objects) TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS exists
      IMPORTING iv_repo_key      TYPE ty_repo_key
                iv_sha1          TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rv_exists) TYPE abap_bool.

    CLASS-METHODS get_known_commits
      IMPORTING iv_repo_key       TYPE ty_repo_key
      RETURNING VALUE(rt_commits) TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    CLASS-METHODS get_all_objects
      IMPORTING iv_repo_key       TYPE ty_repo_key
      RETURNING VALUE(rt_objects) TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS clear_repo
      IMPORTING iv_repo_key TYPE ty_repo_key
      RAISING   zcx_abapgit_ortec_git.

    "! STRATEGY 2: Clear session cache
    CLASS-METHODS invalidate_cache.

    "! Parse parent commit SHA1s from a stored commit object.
    "! Reads obj_data from ZAOG_OBJ_STORE and scans the Git commit
    "! header for parent lines (stops at the first empty line).
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_sha1 |
    "! Commit SHA1 to read parents from
    "! @parameter rt_parents |
    "! Zero or more parent SHA1s (0=root, 1=normal, 2+=merge)
    CLASS-METHODS get_commit_parents
      IMPORTING iv_repo_key       TYPE ty_repo_key
                iv_sha1           TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rt_parents) TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

  PROTECTED SECTION.
  PRIVATE SECTION.
    "! STRATEGY 2: Cache table
    TYPES BEGIN OF ty_cache_entry.
    INCLUDE TYPE zaog_obj_store.
    TYPES END OF ty_cache_entry.

    TYPES ty_obj_store_tt TYPE STANDARD TABLE OF zaog_obj_store WITH DEFAULT KEY.
    TYPES ty_sha1_set TYPE HASHED TABLE OF zif_abapgit_git_definitions=>ty_sha1
                      WITH UNIQUE KEY table_line.
    TYPES:
      BEGIN OF ty_sha1_row,
        sha1 TYPE zif_abapgit_git_definitions=>ty_sha1,
      END OF ty_sha1_row.
    TYPES ty_sha1_rows TYPE STANDARD TABLE OF ty_sha1_row WITH DEFAULT KEY.

    CLASS-DATA mt_cache          TYPE HASHED TABLE OF ty_cache_entry
                WITH UNIQUE KEY repo_key obj_sha1.
    CLASS-DATA mv_cache_repo_key TYPE ty_repo_key.
    CLASS-DATA mv_full_cache_repo_key TYPE ty_repo_key.

    CONSTANTS c_select_package_size TYPE i VALUE 1000.

    CLASS-METHODS get_timestamp
      RETURNING VALUE(rv_ts) TYPE timestampl.

    CLASS-METHODS is_cache_valid
      IMPORTING iv_repo_key     TYPE ty_repo_key
      RETURNING VALUE(rv_valid) TYPE abap_bool.

    CLASS-METHODS populate_cache
      IMPORTING iv_repo_key TYPE ty_repo_key
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS read_object_rows
      IMPORTING iv_repo_key    TYPE ty_repo_key
                it_sha1s       TYPE ty_sha1_rows
      RETURNING VALUE(rt_rows) TYPE ty_obj_store_tt.
ENDCLASS.



CLASS zcl_abapgit_ortec_obj_store IMPLEMENTATION.


  METHOD store_object.
    DATA ls_row TYPE zaog_obj_store.

    ls_row-repo_key   = iv_repo_key.
    ls_row-obj_sha1   = iv_sha1.
    ls_row-obj_type   = iv_type.
    ls_row-obj_data   = iv_data.
    ls_row-obj_size   = xstrlen( iv_data ).
    ls_row-pack_id    = iv_pack_id.
    ls_row-created_at = get_timestamp( ).
    ls_row-status     = 'R'.
    MODIFY zaog_obj_store FROM ls_row.
    IF sy-subrc <> 0.
      zcx_abapgit_ortec_git=>raise( |Failed to store object { iv_sha1 }| ).
    ENDIF.
    invalidate_cache( ).
  ENDMETHOD.


  METHOD store_objects.
    DATA lv_ts   TYPE timestampl.
    DATA ls_row  TYPE zaog_obj_store.
    DATA lt_rows TYPE STANDARD TABLE OF zaog_obj_store.
    FIELD-SYMBOLS <ls_obj> LIKE LINE OF it_objects.

    lv_ts = get_timestamp( ).
    LOOP AT it_objects ASSIGNING <ls_obj>.
      CLEAR ls_row.
      ls_row-repo_key   = iv_repo_key.
      ls_row-obj_sha1   = <ls_obj>-sha1.
      ls_row-obj_type   = <ls_obj>-type.
      ls_row-obj_data   = <ls_obj>-data.
      ls_row-obj_size   = xstrlen( <ls_obj>-data ).
      ls_row-pack_id    = iv_pack_id.
      ls_row-created_at = lv_ts.
      ls_row-status     = 'R'.
      APPEND ls_row TO lt_rows.
    ENDLOOP.
    IF lt_rows IS NOT INITIAL.
      MODIFY zaog_obj_store FROM TABLE lt_rows.
    ENDIF.
    invalidate_cache( ).
  ENDMETHOD.


  METHOD get_object.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_effective_key TYPE ty_repo_key.

    " If no repo_key provided, use the currently cached repo (set by prior populate_cache)
    lv_effective_key = iv_repo_key.
    IF lv_effective_key IS INITIAL AND mv_cache_repo_key IS NOT INITIAL.
      lv_effective_key = mv_cache_repo_key.
    ENDIF.

    IF lv_effective_key IS INITIAL.
      zcx_abapgit_ortec_git=>raise( 'Repository key missing for object store read' ).
    ENDIF.

    APPEND iv_sha1 TO lt_sha1s.
    lt_objects = get_objects( iv_repo_key = lv_effective_key
                              it_sha1s    = lt_sha1s ).
    READ TABLE lt_objects INTO rs_object INDEX 1.
    IF sy-subrc = 0.
      RETURN.
    ENDIF.

    zcx_abapgit_ortec_git=>raise( |Object { iv_sha1 } not found in store| ).
  ENDMETHOD.


  METHOD get_objects.
    DATA lt_unique_sha1s TYPE ty_sha1_set.
    DATA lt_missing_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_package TYPE ty_sha1_rows.
    DATA lt_db_rows TYPE ty_obj_store_tt.
    DATA lt_rows TYPE ty_obj_store_tt.
    DATA lt_found_sha1s TYPE ty_sha1_set.
    DATA ls_cache_entry TYPE ty_cache_entry.
    DATA ls_object TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_sha1 TYPE ty_sha1_row.

    FIELD-SYMBOLS <lv_sha1> LIKE LINE OF it_sha1s.
    FIELD-SYMBOLS <ls_row> LIKE LINE OF lt_rows.

    IF iv_repo_key IS INITIAL.
      zcx_abapgit_ortec_git=>raise( 'Repository key missing for object store read' ).
    ENDIF.

    LOOP AT it_sha1s ASSIGNING <lv_sha1>
        WHERE table_line IS NOT INITIAL.
      INSERT <lv_sha1> INTO TABLE lt_unique_sha1s.
    ENDLOOP.

    IF lt_unique_sha1s IS INITIAL.
      RETURN.
    ENDIF.

    LOOP AT lt_unique_sha1s ASSIGNING <lv_sha1>.
      READ TABLE mt_cache INTO ls_cache_entry
           WITH TABLE KEY repo_key = iv_repo_key obj_sha1 = <lv_sha1>.
      IF sy-subrc = 0 AND ls_cache_entry-status = 'R'.
        APPEND ls_cache_entry TO lt_rows.
      ELSE.
        APPEND <lv_sha1> TO lt_missing_sha1s.
      ENDIF.
    ENDLOOP.

    IF iv_bulk_fetch = abap_true.
      LOOP AT lt_missing_sha1s ASSIGNING <lv_sha1>.
        ls_sha1-sha1 = <lv_sha1>.
        APPEND ls_sha1 TO lt_package.
      ENDLOOP.

      IF lt_package IS NOT INITIAL.
        lt_db_rows = read_object_rows( iv_repo_key = iv_repo_key
                                       it_sha1s    = lt_package ).
        APPEND LINES OF lt_db_rows TO lt_rows.
      ENDIF.
    ELSE.
      LOOP AT lt_missing_sha1s ASSIGNING <lv_sha1>.
        ls_sha1-sha1 = <lv_sha1>.
        APPEND ls_sha1 TO lt_package.
        IF lines( lt_package ) >= c_select_package_size.
          lt_db_rows = read_object_rows( iv_repo_key = iv_repo_key
                                         it_sha1s    = lt_package ).
          APPEND LINES OF lt_db_rows TO lt_rows.
          CLEAR lt_package.
        ENDIF.
      ENDLOOP.

      IF lt_package IS NOT INITIAL.
        lt_db_rows = read_object_rows( iv_repo_key = iv_repo_key
                                       it_sha1s    = lt_package ).
        APPEND LINES OF lt_db_rows TO lt_rows.
      ENDIF.
    ENDIF.

    mv_cache_repo_key = iv_repo_key.

    LOOP AT lt_rows ASSIGNING <ls_row>.
      INSERT <ls_row>-obj_sha1 INTO TABLE lt_found_sha1s.
      MOVE-CORRESPONDING <ls_row> TO ls_cache_entry.
      INSERT ls_cache_entry INTO TABLE mt_cache.
      CLEAR ls_object.
      ls_object-sha1 = <ls_row>-obj_sha1.
      ls_object-type = <ls_row>-obj_type.
      ls_object-data = <ls_row>-obj_data.
      APPEND ls_object TO rt_objects.
    ENDLOOP.

    LOOP AT lt_unique_sha1s ASSIGNING <lv_sha1>.
      READ TABLE lt_found_sha1s WITH TABLE KEY table_line = <lv_sha1> TRANSPORTING NO FIELDS.
      IF sy-subrc <> 0.
        zcx_abapgit_ortec_git=>raise( |Object { <lv_sha1> } not found in store| ).
      ENDIF.
    ENDLOOP.
  ENDMETHOD.


  METHOD get_reachable_objects.
    DATA lt_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_commit_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_current_trees TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_next_trees TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_tree_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_blob_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_blob_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_seen_trees TYPE ty_sha1_set.
    DATA lt_seen_blobs TYPE ty_sha1_set.
    DATA lt_seen_objects TYPE ty_sha1_set.
    DATA ls_commit_object TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_commit TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lt_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.

    FIELD-SYMBOLS <ls_tree_object> LIKE LINE OF lt_tree_objects.
    FIELD-SYMBOLS <ls_blob_object> LIKE LINE OF lt_blob_objects.
    FIELD-SYMBOLS <ls_node> LIKE LINE OF lt_nodes.

    " Pre-load all objects for this repo into the session cache in one SELECT,
    " so every get_objects call below is a pure in-memory cache lookup.
    populate_cache( iv_repo_key ).

    APPEND iv_commit TO lt_commit_sha.
    lt_commit_objects = get_objects( iv_repo_key   = iv_repo_key
                                     it_sha1s      = lt_commit_sha
                                     iv_bulk_fetch = abap_true ).
    READ TABLE lt_commit_objects INTO ls_commit_object INDEX 1.
    IF sy-subrc <> 0 OR ls_commit_object-type <> zif_abapgit_git_definitions=>c_type-commit.
      zcx_abapgit_ortec_git=>raise( |Commit { iv_commit } not found in store| ).
    ENDIF.

    TRY.
        ls_commit = zcl_abapgit_git_pack=>decode_commit( ls_commit_object-data ).
      CATCH zcx_abapgit_exception.
        zcx_abapgit_ortec_git=>raise( |Commit { iv_commit } could not be decoded| ).
    ENDTRY.

    APPEND ls_commit_object TO rt_objects.
    INSERT ls_commit_object-sha1 INTO TABLE lt_seen_objects.

    IF ls_commit-tree IS INITIAL.
      zcx_abapgit_ortec_git=>raise( |Commit { iv_commit } has no tree| ).
    ENDIF.

    APPEND ls_commit-tree TO lt_current_trees.
    INSERT ls_commit-tree INTO TABLE lt_seen_trees.

    WHILE lt_current_trees IS NOT INITIAL.
      CLEAR lt_next_trees.
      lt_tree_objects = get_objects( iv_repo_key   = iv_repo_key
                                     it_sha1s      = lt_current_trees
                                     iv_bulk_fetch = abap_true ).

      LOOP AT lt_tree_objects ASSIGNING <ls_tree_object>.
        IF <ls_tree_object>-type <> zif_abapgit_git_definitions=>c_type-tree.
          zcx_abapgit_ortec_git=>raise( |Object { <ls_tree_object>-sha1 } is not a tree| ).
        ENDIF.

        READ TABLE lt_seen_objects WITH TABLE KEY table_line = <ls_tree_object>-sha1 TRANSPORTING NO FIELDS.
        IF sy-subrc <> 0.
          APPEND <ls_tree_object> TO rt_objects.
          INSERT <ls_tree_object>-sha1 INTO TABLE lt_seen_objects.
        ENDIF.

        TRY.
            lt_nodes = zcl_abapgit_git_pack=>decode_tree( <ls_tree_object>-data ).
          CATCH zcx_abapgit_exception.
            zcx_abapgit_ortec_git=>raise( |Tree { <ls_tree_object>-sha1 } could not be decoded| ).
        ENDTRY.

        LOOP AT lt_nodes ASSIGNING <ls_node>.
          CASE <ls_node>-chmod.
            WHEN zif_abapgit_git_definitions=>c_chmod-dir.
              READ TABLE lt_seen_trees WITH TABLE KEY table_line = <ls_node>-sha1 TRANSPORTING NO FIELDS.
              IF sy-subrc <> 0.
                INSERT <ls_node>-sha1 INTO TABLE lt_seen_trees.
                APPEND <ls_node>-sha1 TO lt_next_trees.
              ENDIF.
            WHEN zif_abapgit_git_definitions=>c_chmod-file
              OR zif_abapgit_git_definitions=>c_chmod-executable
              OR zif_abapgit_git_definitions=>c_chmod-symbolic_link.
              READ TABLE lt_seen_blobs WITH TABLE KEY table_line = <ls_node>-sha1 TRANSPORTING NO FIELDS.
              IF sy-subrc <> 0.
                INSERT <ls_node>-sha1 INTO TABLE lt_seen_blobs.
                APPEND <ls_node>-sha1 TO lt_blob_sha1s.
              ENDIF.
            WHEN zif_abapgit_git_definitions=>c_chmod-submodule.
              CONTINUE.
            WHEN OTHERS.
              zcx_abapgit_ortec_git=>raise( |Tree { <ls_tree_object>-sha1 } contains unknown chmod { <ls_node>-chmod }| ).
          ENDCASE.
        ENDLOOP.
      ENDLOOP.

      lt_current_trees = lt_next_trees.
    ENDWHILE.

    IF lt_blob_sha1s IS NOT INITIAL.
      lt_blob_objects = get_objects( iv_repo_key   = iv_repo_key
                                     it_sha1s      = lt_blob_sha1s
                                     iv_bulk_fetch = abap_true ).
      LOOP AT lt_blob_objects ASSIGNING <ls_blob_object>.
        IF <ls_blob_object>-type <> zif_abapgit_git_definitions=>c_type-blob.
          zcx_abapgit_ortec_git=>raise( |Object { <ls_blob_object>-sha1 } is not a blob| ).
        ENDIF.
        READ TABLE lt_seen_objects WITH TABLE KEY table_line = <ls_blob_object>-sha1 TRANSPORTING NO FIELDS.
        IF sy-subrc <> 0.
          APPEND <ls_blob_object> TO rt_objects.
          INSERT <ls_blob_object>-sha1 INTO TABLE lt_seen_objects.
        ENDIF.
      ENDLOOP.
    ENDIF.
  ENDMETHOD.


  METHOD exists.
    " TODO: variable is assigned but never used (ABAP cleaner)
    DATA lv_dummy TYPE c LENGTH 40.

    SELECT SINGLE obj_sha1 FROM zaog_obj_store
      INTO lv_dummy
      WHERE repo_key = iv_repo_key
        AND obj_sha1 = iv_sha1
        AND status   = 'R'.
    rv_exists = boolc( sy-subrc = 0 ).
  ENDMETHOD.


  METHOD get_known_commits.
    SELECT obj_sha1 FROM zaog_obj_store
      INTO TABLE rt_commits
      WHERE repo_key = iv_repo_key
        AND obj_type = 'commit'
        AND status   = 'R'.
  ENDMETHOD.


  METHOD get_all_objects.
    DATA ls_obj TYPE zif_abapgit_definitions=>ty_object.

    FIELD-SYMBOLS <ls_row> LIKE LINE OF mt_cache.

    " STRATEGY 3: Populate cache (preload small repos < 55K, handle large separately)
    populate_cache( iv_repo_key ).

    " STRATEGY 2: Return all cached objects for repo
    LOOP AT mt_cache ASSIGNING <ls_row>
         WHERE repo_key = iv_repo_key AND status = 'R'.
      CLEAR ls_obj.
      ls_obj-sha1 = <ls_row>-obj_sha1.
      ls_obj-type = <ls_row>-obj_type.
      ls_obj-data = <ls_row>-obj_data.
      APPEND ls_obj TO rt_objects.
    ENDLOOP.
  ENDMETHOD.


  METHOD clear_repo.
    DELETE FROM zaog_obj_store WHERE repo_key = iv_repo_key.
    invalidate_cache( ).
  ENDMETHOD.


  METHOD get_commit_parents.
    DATA lv_data   TYPE xstring.
    DATA lv_text   TYPE string.
    DATA lv_line   TYPE string.
    DATA lv_sha1   TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lt_lines  TYPE TABLE OF string.

    SELECT SINGLE obj_data FROM zaog_obj_store
      INTO lv_data
      WHERE repo_key = iv_repo_key
        AND obj_sha1 = iv_sha1
        AND obj_type = 'commit'
        AND status   = 'R'.
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    " Decode raw bytes to string (UTF-8 / ASCII commit header)
    TRY.
        lv_text = cl_abap_codepage=>convert_from( source   = lv_data
                                                  codepage = '4110' ). " UTF-8
      CATCH cx_parameter_invalid_range cx_sy_conversion_codepage.
        RETURN. " Commit data not valid UTF-8 — cannot parse parents
    ENDTRY.
    SPLIT lv_text AT cl_abap_char_utilities=>newline INTO TABLE lt_lines.
    LOOP AT lt_lines INTO lv_line.
      IF lv_line IS INITIAL.
        EXIT. " End of commit header
      ENDIF.
      IF strlen( lv_line ) >= 47 AND lv_line(7) = 'parent '.
        lv_sha1 = lv_line+7(40).
        APPEND lv_sha1 TO rt_parents.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.


  METHOD invalidate_cache.
    CLEAR: mt_cache,
           mv_cache_repo_key,
           mv_full_cache_repo_key.
  ENDMETHOD.


  METHOD get_timestamp.
    GET TIME STAMP FIELD rv_ts.
  ENDMETHOD.


  METHOD is_cache_valid.
    IF mv_full_cache_repo_key = iv_repo_key AND mv_full_cache_repo_key IS NOT INITIAL.
      rv_valid = abap_true.
    ENDIF.
  ENDMETHOD.


  METHOD populate_cache.
    DATA lt_rows         TYPE STANDARD TABLE OF zaog_obj_store.
    DATA ls_entry        TYPE ty_cache_entry.

    FIELD-SYMBOLS <ls_row> LIKE LINE OF lt_rows.

    " Already cached for this repo? Return.
    IF is_cache_valid( iv_repo_key ) = abap_true.
      RETURN.
    ENDIF.

    SELECT * FROM zaog_obj_store
      INTO TABLE lt_rows
      WHERE repo_key = iv_repo_key AND status = 'R'
      ORDER BY obj_sha1.

    CLEAR mt_cache.
    mv_cache_repo_key = iv_repo_key.
    mv_full_cache_repo_key = iv_repo_key.

    LOOP AT lt_rows ASSIGNING <ls_row>.
      MOVE-CORRESPONDING <ls_row> TO ls_entry.
      INSERT ls_entry INTO TABLE mt_cache.
    ENDLOOP.
  ENDMETHOD.


  METHOD read_object_rows.
    DATA lr_sha1s TYPE RANGE OF zaog_obj_store-obj_sha1.
    FIELD-SYMBOLS <ls_sha1> LIKE LINE OF it_sha1s.

    IF it_sha1s IS INITIAL.
      RETURN.
    ENDIF.

    LOOP AT it_sha1s ASSIGNING <ls_sha1>.
      APPEND VALUE #( sign   = 'I'
                      option = 'EQ'
                      low    = <ls_sha1>-sha1 ) TO lr_sha1s.
    ENDLOOP.

    SELECT * FROM zaog_obj_store
      INTO TABLE rt_rows
      WHERE repo_key = iv_repo_key
        AND obj_sha1 IN lr_sha1s
        AND status   = 'R'.
  ENDMETHOD.
ENDCLASS.
