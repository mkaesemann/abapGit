"! <p class="shorttext synchronized">ORTEC Git Object Store</p>
"! Public API for storing, retrieving, and checking Git objects
"! in the persistent object store (ZAOG_OBJ_STORE).
CLASS zcl_abapgit_ortec_obj_store DEFINITION
  PUBLIC
  FINAL
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

  PROTECTED SECTION.
  PRIVATE SECTION.
    CLASS-METHODS get_timestamp
      RETURNING VALUE(rv_ts) TYPE timestampl.
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
  ENDMETHOD.

  METHOD store_objects.
    DATA lt_rows TYPE STANDARD TABLE OF zaog_obj_store.
    DATA ls_row  TYPE zaog_obj_store.
    DATA lv_ts   TYPE timestampl.
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
  ENDMETHOD.

  METHOD get_object.
    DATA ls_row TYPE zaog_obj_store.
    SELECT SINGLE * FROM zaog_obj_store INTO ls_row
      WHERE repo_key = iv_repo_key
        AND obj_sha1 = iv_sha1
        AND status   = 'R'.
    IF sy-subrc <> 0.
      zcx_abapgit_ortec_git=>raise( |Object { iv_sha1 } not found in store| ).
    ENDIF.
    rs_object-sha1 = ls_row-obj_sha1.
    rs_object-type = ls_row-obj_type.
    rs_object-data = ls_row-obj_data.
  ENDMETHOD.

  METHOD exists.
    DATA lv_dummy TYPE c LENGTH 40.
    SELECT SINGLE obj_sha1 FROM zaog_obj_store INTO lv_dummy
      WHERE repo_key = iv_repo_key
        AND obj_sha1 = iv_sha1.
    rv_exists = boolc( sy-subrc = 0 ).
  ENDMETHOD.

  METHOD get_known_commits.
    SELECT obj_sha1 FROM zaog_obj_store INTO TABLE rt_commits
      WHERE repo_key = iv_repo_key
        AND obj_type = 'commit'
        AND status   = 'R'.
  ENDMETHOD.

  METHOD get_all_objects.
    DATA lt_rows TYPE STANDARD TABLE OF zaog_obj_store.
    DATA ls_obj  TYPE zif_abapgit_definitions=>ty_object.
    FIELD-SYMBOLS <ls_row> LIKE LINE OF lt_rows.
    SELECT * FROM zaog_obj_store INTO TABLE lt_rows
      WHERE repo_key = iv_repo_key
        AND status   = 'R'.
    LOOP AT lt_rows ASSIGNING <ls_row>.
      CLEAR ls_obj.
      ls_obj-sha1 = <ls_row>-obj_sha1.
      ls_obj-type = <ls_row>-obj_type.
      ls_obj-data = <ls_row>-obj_data.
      APPEND ls_obj TO rt_objects.
    ENDLOOP.
  ENDMETHOD.

  METHOD clear_repo.
    DELETE FROM zaog_obj_store WHERE repo_key = iv_repo_key.
  ENDMETHOD.

  METHOD get_timestamp.
    GET TIME STAMP FIELD rv_ts.
  ENDMETHOD.

ENDCLASS.
