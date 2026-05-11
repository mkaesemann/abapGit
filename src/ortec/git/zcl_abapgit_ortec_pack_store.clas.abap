"! <p class="shorttext synchronized">ORTEC Git Pack Metadata Store</p>
"! Manages pack metadata in ZAOG_PACK_META and optional raw packfile
"! storage in ZAOG_RAW_PACK for resume support.
CLASS zcl_abapgit_ortec_pack_store DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.

    TYPES ty_repo_key TYPE c LENGTH 12.
    TYPES ty_pack_id  TYPE c LENGTH 32.

    CLASS-METHODS register_pack
      IMPORTING iv_repo_key   TYPE ty_repo_key
                iv_pack_sha1  TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_obj_count  TYPE i
                iv_total_size TYPE i
      RETURNING VALUE(rv_pack_id) TYPE ty_pack_id
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS update_progress
      IMPORTING iv_repo_key    TYPE ty_repo_key
                iv_pack_id     TYPE ty_pack_id
                iv_obj_decoded TYPE i
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS mark_complete
      IMPORTING iv_repo_key TYPE ty_repo_key
                iv_pack_id  TYPE ty_pack_id
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS store_raw_pack
      IMPORTING iv_repo_key TYPE ty_repo_key
                iv_pack_id  TYPE ty_pack_id
                iv_raw_data TYPE xstring
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS get_raw_pack
      IMPORTING iv_repo_key   TYPE ty_repo_key
                iv_pack_id    TYPE ty_pack_id
      RETURNING VALUE(rv_raw) TYPE xstring
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS cleanup_repo
      IMPORTING iv_repo_key TYPE ty_repo_key.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.


CLASS zcl_abapgit_ortec_pack_store IMPLEMENTATION.

  METHOD register_pack.
    DATA ls_meta TYPE zaog_pack_meta.
    DATA lv_ts   TYPE timestampl.
    GET TIME STAMP FIELD lv_ts.

    TRY.
        rv_pack_id = cl_system_uuid=>create_uuid_c32_static( ).
      CATCH cx_uuid_error.
        zcx_abapgit_ortec_git=>raise( |UUID generation failed| ).
    ENDTRY.

    ls_meta-repo_key    = iv_repo_key.
    ls_meta-pack_id     = rv_pack_id.
    ls_meta-pack_sha1   = iv_pack_sha1.
    ls_meta-obj_count   = iv_obj_count.
    ls_meta-obj_decoded = 0.
    ls_meta-total_size  = iv_total_size.
    ls_meta-status      = 'P'. " partial
    ls_meta-received_at = lv_ts.

    INSERT zaog_pack_meta FROM ls_meta.
    IF sy-subrc <> 0.
      zcx_abapgit_ortec_git=>raise( |Failed to register pack| ).
    ENDIF.
  ENDMETHOD.

  METHOD update_progress.
    UPDATE zaog_pack_meta
      SET obj_decoded = iv_obj_decoded
      WHERE repo_key = iv_repo_key
        AND pack_id  = iv_pack_id.
  ENDMETHOD.

  METHOD mark_complete.
    UPDATE zaog_pack_meta
      SET status = 'C'
      WHERE repo_key = iv_repo_key
        AND pack_id  = iv_pack_id.
  ENDMETHOD.

  METHOD store_raw_pack.
    DATA ls_raw TYPE zaog_raw_pack.
    ls_raw-repo_key = iv_repo_key.
    ls_raw-pack_id  = iv_pack_id.
    ls_raw-raw_data = iv_raw_data.
    MODIFY zaog_raw_pack FROM ls_raw.
  ENDMETHOD.

  METHOD get_raw_pack.
    SELECT SINGLE raw_data FROM zaog_raw_pack INTO rv_raw
      WHERE repo_key = iv_repo_key
        AND pack_id  = iv_pack_id.
    IF sy-subrc <> 0.
      zcx_abapgit_ortec_git=>raise( |Raw pack not found| ).
    ENDIF.
  ENDMETHOD.

  METHOD cleanup_repo.
    DELETE FROM zaog_pack_meta WHERE repo_key = iv_repo_key.
    DELETE FROM zaog_raw_pack  WHERE repo_key = iv_repo_key.
  ENDMETHOD.

ENDCLASS.
