"! <p class="shorttext synchronized">ORTEC Incremental Pack Decoder with Persistence</p>
"! Wraps standard abapGit pack decode but persists each object
"! to ZAOG_OBJ_STORE during decode. Stores raw packfile and tracks
"! progress in ZAOG_FETCH_SESS for crash resume.
CLASS zcl_abapgit_ortec_pack_dec DEFINITION
  PUBLIC FINAL CREATE PUBLIC.
  PUBLIC SECTION.
    TYPES ty_repo_key   TYPE c LENGTH 12.
    TYPES ty_session_id TYPE c LENGTH 32.
    TYPES ty_pack_id    TYPE c LENGTH 32.
    CLASS-METHODS decode_and_persist
      IMPORTING iv_data            TYPE xstring
                iv_repo_key        TYPE ty_repo_key
                iv_commit_interval TYPE i DEFAULT 50
      RETURNING VALUE(rt_objects)  TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_exception.
    CLASS-METHODS resume_decode
      IMPORTING iv_repo_key       TYPE ty_repo_key
      RETURNING VALUE(rt_objects) TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_exception.
  PROTECTED SECTION.
  PRIVATE SECTION.
    CLASS-METHODS create_session
      IMPORTING iv_repo_key TYPE ty_repo_key iv_obj_total TYPE i iv_pack_id TYPE ty_pack_id
      RETURNING VALUE(rv_session_id) TYPE ty_session_id.
    CLASS-METHODS update_session_progress
      IMPORTING iv_session_id TYPE ty_session_id iv_obj_done TYPE i.
    CLASS-METHODS fail_session
      IMPORTING iv_session_id TYPE ty_session_id iv_obj_done TYPE i.
    CLASS-METHODS complete_session
      IMPORTING iv_session_id TYPE ty_session_id.
    CLASS-METHODS find_active_session
      IMPORTING iv_repo_key TYPE ty_repo_key
      EXPORTING ev_session_id TYPE ty_session_id ev_pack_id TYPE ty_pack_id ev_obj_done TYPE i.
    CLASS-METHODS persist_objects
      IMPORTING iv_repo_key TYPE ty_repo_key iv_pack_id TYPE ty_pack_id
                iv_session_id TYPE ty_session_id iv_skip_count TYPE i DEFAULT 0
                iv_commit_interval TYPE i DEFAULT 50
                it_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    CLASS-METHODS complete_pack
      IMPORTING iv_repo_key TYPE ty_repo_key iv_pack_id TYPE ty_pack_id iv_count TYPE i.
ENDCLASS.

CLASS zcl_abapgit_ortec_pack_dec IMPLEMENTATION.

  METHOD decode_and_persist.
    DATA lv_pack_id TYPE ty_pack_id.
    DATA lv_session_id TYPE ty_session_id.
    DATA lv_ts TYPE timestampl.
    DATA ls_raw TYPE zaog_raw_pack.
    DATA ls_meta TYPE zaog_pack_meta.

    rt_objects = zcl_abapgit_git_pack=>decode( iv_data ).

    TRY.
        lv_pack_id = cl_system_uuid=>create_uuid_c32_static( ).
      CATCH cx_uuid_error.
        RETURN.
    ENDTRY.

    ls_raw-repo_key = iv_repo_key.
    ls_raw-pack_id  = lv_pack_id.
    ls_raw-raw_data = iv_data.
    MODIFY zaog_raw_pack FROM ls_raw.

    GET TIME STAMP FIELD lv_ts.
    ls_meta-repo_key    = iv_repo_key.
    ls_meta-pack_id     = lv_pack_id.
    ls_meta-obj_count   = lines( rt_objects ).
    ls_meta-obj_decoded = 0.
    ls_meta-total_size  = xstrlen( iv_data ).
    ls_meta-status      = 'P'.
    ls_meta-raw_stored  = abap_true.
    ls_meta-received_at = lv_ts.
    INSERT zaog_pack_meta FROM ls_meta.

    lv_session_id = create_session( iv_repo_key = iv_repo_key iv_obj_total = lines( rt_objects ) iv_pack_id = lv_pack_id ).
    COMMIT WORK.

    persist_objects( iv_repo_key = iv_repo_key iv_pack_id = lv_pack_id iv_session_id = lv_session_id
                     iv_skip_count = 0 iv_commit_interval = iv_commit_interval it_objects = rt_objects ).

    complete_pack( iv_repo_key = iv_repo_key iv_pack_id = lv_pack_id iv_count = lines( rt_objects ) ).
    complete_session( lv_session_id ).
    DELETE FROM zaog_raw_pack WHERE repo_key = iv_repo_key AND pack_id = lv_pack_id.
    COMMIT WORK.
  ENDMETHOD.

  METHOD resume_decode.
    DATA lv_session_id TYPE ty_session_id.
    DATA lv_pack_id TYPE ty_pack_id.
    DATA lv_obj_done TYPE i.
    DATA lv_raw TYPE xstring.

    find_active_session( EXPORTING iv_repo_key = iv_repo_key
      IMPORTING ev_session_id = lv_session_id ev_pack_id = lv_pack_id ev_obj_done = lv_obj_done ).
    IF lv_session_id IS INITIAL.
      RETURN.
    ENDIF.

    SELECT SINGLE raw_data FROM zaog_raw_pack INTO lv_raw
      WHERE repo_key = iv_repo_key AND pack_id = lv_pack_id.
    IF sy-subrc <> 0 OR lv_raw IS INITIAL.
      fail_session( iv_session_id = lv_session_id iv_obj_done = lv_obj_done ).
      COMMIT WORK.
      RETURN.
    ENDIF.

    rt_objects = zcl_abapgit_git_pack=>decode( lv_raw ).

    persist_objects( iv_repo_key = iv_repo_key iv_pack_id = lv_pack_id iv_session_id = lv_session_id
                     iv_skip_count = lv_obj_done it_objects = rt_objects ).

    complete_pack( iv_repo_key = iv_repo_key iv_pack_id = lv_pack_id iv_count = lines( rt_objects ) ).
    complete_session( lv_session_id ).
    DELETE FROM zaog_raw_pack WHERE repo_key = iv_repo_key AND pack_id = lv_pack_id.
    COMMIT WORK.
  ENDMETHOD.

  METHOD persist_objects.
    DATA ls_row TYPE zaog_obj_store.
    DATA lv_count TYPE i.
    FIELD-SYMBOLS <ls_obj> LIKE LINE OF it_objects.
    LOOP AT it_objects ASSIGNING <ls_obj>.
      lv_count = lv_count + 1.
      IF lv_count <= iv_skip_count.
        CONTINUE.
      ENDIF.
      CLEAR ls_row.
      ls_row-repo_key = iv_repo_key.
      ls_row-obj_sha1 = <ls_obj>-sha1.
      ls_row-obj_type = <ls_obj>-type.
      ls_row-obj_data = <ls_obj>-data.
      ls_row-obj_size = xstrlen( <ls_obj>-data ).
      ls_row-pack_id  = iv_pack_id.
      GET TIME STAMP FIELD ls_row-created_at.
      ls_row-status   = 'R'.
      MODIFY zaog_obj_store FROM ls_row.
      IF lv_count MOD iv_commit_interval = 0.
        update_session_progress( iv_session_id = iv_session_id iv_obj_done = lv_count ).
        COMMIT WORK.
      ENDIF.
    ENDLOOP.
    update_session_progress( iv_session_id = iv_session_id iv_obj_done = lv_count ).
    COMMIT WORK.
  ENDMETHOD.

  METHOD complete_pack.
    DATA ls_meta TYPE zaog_pack_meta.
    ls_meta-status      = 'C'.
    ls_meta-obj_decoded = iv_count.
    UPDATE zaog_pack_meta SET status = ls_meta-status obj_decoded = ls_meta-obj_decoded
      WHERE repo_key = iv_repo_key AND pack_id = iv_pack_id.
  ENDMETHOD.

  METHOD create_session.
    DATA ls_sess TYPE zaog_fetch_sess.
    DATA lv_ts TYPE timestampl.
    GET TIME STAMP FIELD lv_ts.
    TRY.
        rv_session_id = cl_system_uuid=>create_uuid_c32_static( ).
      CATCH cx_uuid_error.
        RETURN.
    ENDTRY.
    ls_sess-session_id = rv_session_id.
    ls_sess-repo_key   = iv_repo_key.
    ls_sess-pack_id    = iv_pack_id.
    ls_sess-phase      = 'D'.
    ls_sess-obj_done   = 0.
    ls_sess-obj_total  = iv_obj_total.
    ls_sess-status     = 'A'.
    ls_sess-created_at = lv_ts.
    ls_sess-updated_at = lv_ts.
    ls_sess-changed_by = sy-uname.
    INSERT zaog_fetch_sess FROM ls_sess.
  ENDMETHOD.

  METHOD update_session_progress.
    DATA ls_upd TYPE zaog_fetch_sess.
    DATA lv_ts TYPE timestampl.
    GET TIME STAMP FIELD lv_ts.
    ls_upd-obj_done   = iv_obj_done.
    ls_upd-updated_at = lv_ts.
    UPDATE zaog_fetch_sess SET obj_done = ls_upd-obj_done updated_at = ls_upd-updated_at
      WHERE session_id = iv_session_id.
  ENDMETHOD.

  METHOD fail_session.
    DATA ls_upd TYPE zaog_fetch_sess.
    DATA lv_ts TYPE timestampl.
    GET TIME STAMP FIELD lv_ts.
    ls_upd-obj_done   = iv_obj_done.
    ls_upd-status     = 'F'.
    ls_upd-updated_at = lv_ts.
    UPDATE zaog_fetch_sess SET obj_done = ls_upd-obj_done status = ls_upd-status updated_at = ls_upd-updated_at
      WHERE session_id = iv_session_id.
  ENDMETHOD.

  METHOD complete_session.
    DATA ls_upd TYPE zaog_fetch_sess.
    DATA lv_ts TYPE timestampl.
    GET TIME STAMP FIELD lv_ts.
    ls_upd-phase      = 'C'.
    ls_upd-status     = 'C'.
    ls_upd-updated_at = lv_ts.
    UPDATE zaog_fetch_sess SET phase = ls_upd-phase status = ls_upd-status updated_at = ls_upd-updated_at
      WHERE session_id = iv_session_id.
  ENDMETHOD.

  METHOD find_active_session.
    DATA ls_sess TYPE zaog_fetch_sess.
    CLEAR: ev_session_id, ev_pack_id, ev_obj_done.
    SELECT SINGLE * FROM zaog_fetch_sess INTO ls_sess
      WHERE repo_key = iv_repo_key AND status = 'A'.
    IF sy-subrc = 0.
      ev_session_id = ls_sess-session_id.
      ev_pack_id    = ls_sess-pack_id.
      ev_obj_done   = ls_sess-obj_done.
    ENDIF.
  ENDMETHOD.

ENDCLASS.
