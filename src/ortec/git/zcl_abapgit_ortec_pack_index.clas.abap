"! <p class="shorttext synchronized">ORTEC Git Pack Index Manager</p>
"! Persists and reads pack index entries in ZAOG_PACK_IDX.
CLASS zcl_abapgit_ortec_pack_index DEFINITION
  PUBLIC FINAL
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
    TYPES tty_index_entries     TYPE STANDARD TABLE OF ty_index_entry WITH DEFAULT KEY.

    TYPES ty_index_entries_upd  TYPE ty_index_entry WITH INDICATORS _control.
    TYPES tty_index_entries_upd TYPE STANDARD TABLE OF ty_index_entries_upd WITH DEFAULT KEY.

    CLASS-METHODS store_entries
      IMPORTING iv_repo_key TYPE ty_repo_key
                iv_pack_id  TYPE ty_pack_id
                it_entries  TYPE tty_index_entries.

    CLASS-METHODS update_entries
      IMPORTING iv_repo_key TYPE ty_repo_key
                iv_pack_id  TYPE ty_pack_id
                it_entries  TYPE tty_index_entries_upd.

    CLASS-METHODS get_pending
      IMPORTING iv_repo_key       TYPE ty_repo_key
                iv_pack_id        TYPE ty_pack_id
      RETURNING VALUE(rt_entries) TYPE tty_index_entries.

    CLASS-METHODS mark_decoded
      IMPORTING iv_repo_key  TYPE ty_repo_key
                iv_pack_id   TYPE ty_pack_id
                iv_obj_index TYPE i
                iv_obj_sha1  TYPE zif_abapgit_git_definitions=>ty_sha1.

    CLASS-METHODS cleanup_repo
      IMPORTING iv_repo_key TYPE ty_repo_key.
ENDCLASS.


CLASS zcl_abapgit_ortec_pack_index IMPLEMENTATION.
  METHOD store_entries.
    DATA lt_rows TYPE STANDARD TABLE OF zaog_pack_idx.

    lt_rows = CORRESPONDING #( it_entries MAPPING
                repo_key = DEFAULT iv_repo_key
                pack_id  = DEFAULT iv_pack_id ).

    IF lt_rows IS NOT INITIAL.
      MODIFY zaog_pack_idx FROM TABLE lt_rows.
    ENDIF.
  ENDMETHOD.

  METHOD get_pending.
    DATA lt_rows TYPE STANDARD TABLE OF zaog_pack_idx.

    SELECT * FROM zaog_pack_idx
      INTO TABLE lt_rows
      WHERE repo_key   = iv_repo_key
        AND pack_id    = iv_pack_id
        AND dec_status = 'P'
      ORDER BY obj_index.
    rt_entries = CORRESPONDING #( BASE ( rt_entries ) lt_rows ).
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

  METHOD update_entries.

    TYPES ty_pack_idx TYPE zaog_pack_idx WITH INDICATORS _control.

    DATA lt_idx TYPE STANDARD TABLE OF ty_pack_idx WITH DEFAULT KEY.

    lt_idx = CORRESPONDING #( it_entries MAPPING
                                repo_key = DEFAULT iv_repo_key
                                pack_id  = DEFAULT iv_pack_id ).

    IF lt_idx IS NOT INITIAL.
      UPDATE zaog_pack_idx FROM TABLE @lt_idx INDICATORS SET STRUCTURE _control.
    ENDIF.

  ENDMETHOD.
ENDCLASS.
