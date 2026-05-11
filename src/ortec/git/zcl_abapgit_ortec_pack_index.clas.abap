"! <p class="shorttext synchronized">ORTEC Git Pack Index Manager</p>
"! Persists and reads pack index entries in ZAOG_PACK_IDX.
CLASS zcl_abapgit_ortec_pack_index DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.

    TYPES ty_repo_key TYPE c LENGTH 12.
    TYPES ty_pack_id  TYPE c LENGTH 32.

    TYPES: BEGIN OF ty_index_entry,
             obj_index   TYPE i,
             obj_sha1    TYPE zif_abapgit_git_definitions=>ty_sha1,
             obj_type    TYPE zif_abapgit_git_definitions=>ty_type,
             pack_offset TYPE i,
             comp_len    TYPE i,
             uncomp_len  TYPE i,
             delta_base  TYPE zif_abapgit_git_definitions=>ty_sha1,
             adler32     TYPE zif_abapgit_git_definitions=>ty_adler32,
             dec_status  TYPE c LENGTH 1,
           END OF ty_index_entry.
    TYPES ty_index_entries TYPE STANDARD TABLE OF ty_index_entry WITH DEFAULT KEY.

    CLASS-METHODS store_entries
      IMPORTING iv_repo_key TYPE ty_repo_key
                iv_pack_id  TYPE ty_pack_id
                it_entries  TYPE ty_index_entries
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS get_pending
      IMPORTING iv_repo_key       TYPE ty_repo_key
                iv_pack_id        TYPE ty_pack_id
      RETURNING VALUE(rt_entries) TYPE ty_index_entries.

    CLASS-METHODS mark_decoded
      IMPORTING iv_repo_key  TYPE ty_repo_key
                iv_pack_id   TYPE ty_pack_id
                iv_obj_index TYPE i
                iv_obj_sha1  TYPE zif_abapgit_git_definitions=>ty_sha1.

    CLASS-METHODS cleanup_repo
      IMPORTING iv_repo_key TYPE ty_repo_key.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.


CLASS zcl_abapgit_ortec_pack_index IMPLEMENTATION.

  METHOD store_entries.
    DATA lt_rows TYPE STANDARD TABLE OF zaog_pack_idx.
    DATA ls_row  TYPE zaog_pack_idx.
    FIELD-SYMBOLS <ls_entry> LIKE LINE OF it_entries.
    LOOP AT it_entries ASSIGNING <ls_entry>.
      CLEAR ls_row.
      ls_row-repo_key   = iv_repo_key.
      ls_row-pack_id    = iv_pack_id.
      ls_row-obj_index  = <ls_entry>-obj_index.
      ls_row-obj_sha1   = <ls_entry>-obj_sha1.
      ls_row-obj_type   = <ls_entry>-obj_type.
      ls_row-pack_offset = <ls_entry>-pack_offset.
      ls_row-comp_len   = <ls_entry>-comp_len.
      ls_row-uncomp_len = <ls_entry>-uncomp_len.
      ls_row-delta_base = <ls_entry>-delta_base.
      ls_row-adler32    = <ls_entry>-adler32.
      ls_row-dec_status = <ls_entry>-dec_status.
      APPEND ls_row TO lt_rows.
    ENDLOOP.
    IF lt_rows IS NOT INITIAL.
      MODIFY zaog_pack_idx FROM TABLE lt_rows.
    ENDIF.
  ENDMETHOD.

  METHOD get_pending.
    DATA lt_rows TYPE STANDARD TABLE OF zaog_pack_idx.
    DATA ls_entry TYPE ty_index_entry.
    FIELD-SYMBOLS <ls_row> LIKE LINE OF lt_rows.
    SELECT * FROM zaog_pack_idx INTO TABLE lt_rows
      WHERE repo_key   = iv_repo_key
        AND pack_id    = iv_pack_id
        AND dec_status = 'P'
      ORDER BY obj_index.
    LOOP AT lt_rows ASSIGNING <ls_row>.
      CLEAR ls_entry.
      ls_entry-obj_index   = <ls_row>-obj_index.
      ls_entry-obj_sha1    = <ls_row>-obj_sha1.
      ls_entry-obj_type    = <ls_row>-obj_type.
      ls_entry-pack_offset = <ls_row>-pack_offset.
      ls_entry-comp_len    = <ls_row>-comp_len.
      ls_entry-uncomp_len  = <ls_row>-uncomp_len.
      ls_entry-delta_base  = <ls_row>-delta_base.
      ls_entry-adler32     = <ls_row>-adler32.
      ls_entry-dec_status  = <ls_row>-dec_status.
      APPEND ls_entry TO rt_entries.
    ENDLOOP.
  ENDMETHOD.

  METHOD mark_decoded.
    UPDATE zaog_pack_idx
      SET dec_status = 'D'
          obj_sha1   = iv_obj_sha1
      WHERE repo_key  = iv_repo_key
        AND pack_id   = iv_pack_id
        AND obj_index = iv_obj_index.
  ENDMETHOD.

  METHOD cleanup_repo.
    DELETE FROM zaog_pack_idx WHERE repo_key = iv_repo_key.
  ENDMETHOD.

ENDCLASS.
