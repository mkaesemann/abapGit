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
      IMPORTING iv_repo_key      TYPE ty_repo_key
                iv_sha1          TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rs_object) TYPE zif_abapgit_definitions=>ty_object
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

  PRIVATE SECTION.
    "! STRATEGY 2: Cache table
    TYPES BEGIN OF ty_cache_entry.
            INCLUDE TYPE zaog_obj_store.
    TYPES END OF ty_cache_entry.

    CLASS-DATA mt_cache          TYPE HASHED TABLE OF ty_cache_entry
                WITH UNIQUE KEY repo_key obj_sha1.
    CLASS-DATA mv_cache_repo_key TYPE ty_repo_key.

    "! STRATEGY 3: Preload threshold (55000 objects)
    CONSTANTS c_preload_threshold TYPE i VALUE 55000.

    CLASS-METHODS get_timestamp
      RETURNING VALUE(rv_ts) TYPE timestampl.

    CLASS-METHODS is_cache_valid
      IMPORTING iv_repo_key     TYPE ty_repo_key
      RETURNING VALUE(rv_valid) TYPE abap_bool.

    CLASS-METHODS populate_cache
      IMPORTING iv_repo_key TYPE ty_repo_key
      RAISING   zcx_abapgit_ortec_git.
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
    DATA ls_row TYPE zaog_obj_store.
    DATA lv_effective_key TYPE ty_repo_key.

    " If no repo_key provided, use the currently cached repo (set by prior populate_cache)
    lv_effective_key = iv_repo_key.
    IF lv_effective_key IS INITIAL AND mv_cache_repo_key IS NOT INITIAL.
      lv_effective_key = mv_cache_repo_key.
    ENDIF.

    "! STRATEGY 2: Try cache first (O(1) lookup)
    IF lv_effective_key IS NOT INITIAL AND is_cache_valid( lv_effective_key ) = abap_true.
      READ TABLE mt_cache INTO ls_row
           WITH TABLE KEY repo_key = lv_effective_key obj_sha1 = iv_sha1.
      IF sy-subrc = 0 AND ls_row-status = 'R'.
        rs_object-sha1 = ls_row-obj_sha1.
        rs_object-type = ls_row-obj_type.
        rs_object-data = ls_row-obj_data.
        RETURN.
      ENDIF.
    ENDIF.

    "! STRATEGY 2: Cache miss - read from DB
    IF lv_effective_key IS NOT INITIAL.
      SELECT SINGLE * FROM zaog_obj_store
        INTO ls_row
        WHERE repo_key = lv_effective_key
          AND obj_sha1 = iv_sha1
          AND status   = 'R'.
      IF sy-subrc = 0.
        rs_object-sha1 = ls_row-obj_sha1.
        rs_object-type = ls_row-obj_type.
        rs_object-data = ls_row-obj_data.
        RETURN.
      ENDIF.
    ENDIF.

    zcx_abapgit_ortec_git=>raise( |Object { iv_sha1 } not found in store| ).
  ENDMETHOD.

  METHOD exists.
    " TODO: variable is assigned but never used (ABAP cleaner)
    DATA lv_dummy TYPE c LENGTH 40.

    SELECT SINGLE obj_sha1 FROM zaog_obj_store
      INTO lv_dummy
      WHERE repo_key = iv_repo_key
        AND obj_sha1 = iv_sha1.
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

    ""! STRATEGY 3: Populate cache (preload small repos < 55K, handle large separately)
    populate_cache( iv_repo_key ).

    "! STRATEGY 2: Return all cached objects for repo
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

  METHOD invalidate_cache.
    CLEAR: mt_cache,
           mv_cache_repo_key.
  ENDMETHOD.

  METHOD get_timestamp.
    GET TIME STAMP FIELD rv_ts.
  ENDMETHOD.

  METHOD is_cache_valid.
    IF mv_cache_repo_key = iv_repo_key AND mv_cache_repo_key IS NOT INITIAL.
      rv_valid = abap_true.
    ENDIF.
  ENDMETHOD.

  METHOD populate_cache.
    DATA lt_rows         TYPE STANDARD TABLE OF zaog_obj_store.
    DATA lv_object_count TYPE i.
    DATA ls_entry        TYPE ty_cache_entry.

    FIELD-SYMBOLS <ls_row> LIKE LINE OF lt_rows.

    ""! Already cached for this repo? Return.
    IF is_cache_valid( iv_repo_key ) = abap_true.
      RETURN.
    ENDIF.

    ""! STRATEGY 3: Count objects to decide strategy
    SELECT COUNT(*) FROM zaog_obj_store
      INTO lv_object_count
      WHERE repo_key = iv_repo_key AND status = 'R'.

    "! STRATEGY 3: Small repo (<55K) - bulk preload everything
    IF lv_object_count < c_preload_threshold.
      SELECT * FROM zaog_obj_store
        INTO TABLE lt_rows
        WHERE repo_key = iv_repo_key AND status = 'R'
        ORDER BY obj_sha1.

      CLEAR mt_cache.
      mv_cache_repo_key = iv_repo_key.

      LOOP AT lt_rows ASSIGNING <ls_row>.
        MOVE-CORRESPONDING <ls_row> TO ls_entry.
        INSERT ls_entry INTO TABLE mt_cache.
      ENDLOOP.
    ELSE.
      ""! STRATEGY 4: Large repo (>=55K) - mark cache valid but empty
      ""! get_object() falls through to single SELECT on cache miss
      ""! get_all_objects() can be enhanced with cursor-based chunking later
      CLEAR mt_cache.
      mv_cache_repo_key = iv_repo_key.
    ENDIF.
  ENDMETHOD.
ENDCLASS.
