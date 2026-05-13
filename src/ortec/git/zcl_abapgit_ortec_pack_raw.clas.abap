"! <p class="shorttext synchronized">ORTEC Raw Pack Storage Access</p>
"! Encapsulates all ZAOG_RAW_PACK read/write/delete operations in one class.
"! This keeps decode orchestration code independent from direct table access.
CLASS zcl_abapgit_ortec_pack_raw DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    TYPES ty_repo_key TYPE c LENGTH 12.
    TYPES ty_pack_id  TYPE c LENGTH 32.
    TYPES ty_session_id TYPE c LENGTH 32.

    TYPES: BEGIN OF ty_session_info,
             session_id   TYPE ty_session_id,
             repo_key     TYPE ty_repo_key,
             branch_name  TYPE string,
             pack_id      TYPE ty_pack_id,
             phase        TYPE c LENGTH 1,
             curr_offset  TYPE i,
             obj_done     TYPE i,
             obj_total    TYPE i,
             status       TYPE c LENGTH 1,
             deepen_level TYPE i,
           END OF ty_session_info.

    "! Stores raw pack bytes for a repository/pack pair.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_pack_id |
    "! Pack identifier
    "! @parameter iv_raw_data |
    "! Raw packfile payload
    "! @raising zcx_abapgit_exception |
    "! On persistence error
    CLASS-METHODS store
      IMPORTING iv_repo_key TYPE ty_repo_key
                iv_pack_id  TYPE ty_pack_id
                iv_raw_data TYPE xstring
      RAISING   zcx_abapgit_exception.

    "! Loads raw pack bytes for a repository/pack pair.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_pack_id |
    "! Pack identifier
    "! @parameter rv_raw_data |
    "! Raw packfile payload
    "! @raising zcx_abapgit_exception |
    "! If raw data is missing
    CLASS-METHODS load
      IMPORTING iv_repo_key        TYPE ty_repo_key
                iv_pack_id         TYPE ty_pack_id
      RETURNING VALUE(rv_raw_data) TYPE xstring
      RAISING   zcx_abapgit_exception.

    "! Deletes one raw pack payload.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_pack_id |
    "! Pack identifier
    CLASS-METHODS delete
      IMPORTING iv_repo_key TYPE ty_repo_key
                iv_pack_id  TYPE ty_pack_id.

    "! Deletes all raw pack payloads of a repository.
    "! @parameter iv_repo_key |
    "! Repository key
    CLASS-METHODS delete_repo
      IMPORTING iv_repo_key TYPE ty_repo_key.

    "! Creates a fetch session for resumable decode.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_pack_id |
    "! Pack identifier
    "! @parameter iv_obj_total |
    "! Number of objects expected in the pack
    "! @parameter iv_branch_name |
    "! Branch name associated with the fetch request
    "! @parameter iv_deepen_level |
    "! Deepen level associated with the fetch request
    "! @parameter rv_session_id |
    "! Created session identifier or INITIAL on UUID failure
    CLASS-METHODS create_session
      IMPORTING iv_repo_key          TYPE ty_repo_key
                iv_pack_id           TYPE ty_pack_id
                iv_obj_total         TYPE i
                iv_branch_name       TYPE string OPTIONAL
                iv_deepen_level      TYPE i DEFAULT 1
      RETURNING VALUE(rv_session_id) TYPE ty_session_id.

    "! Updates decode progress of an active session.
    "! @parameter iv_session_id |
    "! Session identifier
    "! @parameter iv_obj_done |
    "! Number of decoded objects persisted so far
    "! @parameter iv_curr_offset |
    "! Optional byte offset checkpoint in the raw pack
    CLASS-METHODS update_session_progress
      IMPORTING iv_session_id  TYPE ty_session_id
                iv_obj_done    TYPE i
                iv_curr_offset TYPE i OPTIONAL.

    "! Marks a session as failed.
    "! @parameter iv_session_id |
    "! Session identifier
    "! @parameter iv_obj_done |
    "! Last decoded object count
    CLASS-METHODS fail_session
      IMPORTING iv_session_id TYPE ty_session_id
                iv_obj_done   TYPE i.

    "! Marks a session as completed.
    "! @parameter iv_session_id |
    "! Session identifier
    CLASS-METHODS complete_session
      IMPORTING iv_session_id TYPE ty_session_id.

    "! Reads the currently active session of a repository.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter rs_session |
    "! Active session information or INITIAL if none exists
    CLASS-METHODS find_active_session
      IMPORTING iv_repo_key        TYPE ty_repo_key
      RETURNING VALUE(rs_session)  TYPE ty_session_info.

    "! Reads one session by its identifier.
    "! @parameter iv_session_id |
    "! Session identifier
    "! @parameter rs_session |
    "! Session information or INITIAL if none exists
    CLASS-METHODS get_session
      IMPORTING iv_session_id      TYPE ty_session_id
      RETURNING VALUE(rs_session)  TYPE ty_session_info.

    "! Cleans up an abandoned partial decode session and its temporary data.
    "! @parameter is_session |
    "! Session information returned by FIND_ACTIVE_SESSION/GET_SESSION
    "! @parameter iv_reason |
    "! Optional reason persisted to the failed session
    CLASS-METHODS cleanup_partial_session
      IMPORTING is_session TYPE ty_session_info
                iv_reason  TYPE string OPTIONAL.

    "! Acquire repo-scoped mutex row in ZAOG_FETCH_SESS.
    "! Retries with exponential backoff and jitter.
    "! @parameter iv_repo_key |
    "! Repository key.
    "! @parameter iv_max_attempts |
    "! Maximum lock attempts.
    "! @parameter iv_base_wait_ms |
    "! Base wait in milliseconds.
    "! @parameter rv_lock_id |
    "! Mutex identifier used for release.
    "! @raising zcx_abapgit_exception |
    "! If lock cannot be acquired.
    CLASS-METHODS acquire_repo_lock
      IMPORTING iv_repo_key      TYPE ty_repo_key
                iv_max_attempts  TYPE i DEFAULT 7
                iv_base_wait_ms  TYPE i DEFAULT 50
      RETURNING VALUE(rv_lock_id) TYPE ty_session_id
      RAISING   zcx_abapgit_exception.

    "! Release previously acquired repo-scoped mutex.
    "! @parameter iv_lock_id |
    "! Mutex identifier.
    CLASS-METHODS release_repo_lock
      IMPORTING iv_lock_id TYPE ty_session_id.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.


CLASS zcl_abapgit_ortec_pack_raw IMPLEMENTATION.

  METHOD store.
    DATA ls_raw TYPE zaog_raw_pack.

    ls_raw-repo_key = iv_repo_key.
    ls_raw-pack_id  = iv_pack_id.
    ls_raw-raw_data = iv_raw_data.
    MODIFY zaog_raw_pack FROM ls_raw.

    IF sy-subrc <> 0.
      zcx_abapgit_exception=>raise( |Failed to store raw pack| ).
    ENDIF.
  ENDMETHOD.


  METHOD load.
    SELECT SINGLE raw_data
      FROM zaog_raw_pack
      INTO rv_raw_data
      WHERE repo_key = iv_repo_key
        AND pack_id  = iv_pack_id.

    IF sy-subrc <> 0 OR rv_raw_data IS INITIAL.
      zcx_abapgit_exception=>raise( |Raw pack not found| ).
    ENDIF.
  ENDMETHOD.


  METHOD delete.
    DELETE FROM zaog_raw_pack
      WHERE repo_key = iv_repo_key
        AND pack_id  = iv_pack_id.
  ENDMETHOD.


  METHOD delete_repo.
    DELETE FROM zaog_raw_pack WHERE repo_key = iv_repo_key.
  ENDMETHOD.


  METHOD create_session.
    DATA lv_ts TYPE timestampl.
    DATA ls_sess TYPE zaog_fetch_sess.

    GET TIME STAMP FIELD lv_ts.

    TRY.
        rv_session_id = cl_system_uuid=>create_uuid_c32_static( ).
      CATCH cx_uuid_error.
        RETURN.
    ENDTRY.

    ls_sess-session_id = rv_session_id.
    ls_sess-repo_key = iv_repo_key.
    ls_sess-branch_name = iv_branch_name.
    ls_sess-pack_id = iv_pack_id.
    ls_sess-phase = 'D'.
    ls_sess-obj_done = 0.
    ls_sess-obj_total = iv_obj_total.
    ls_sess-status = 'A'.
    ls_sess-created_at = lv_ts.
    ls_sess-updated_at = lv_ts.
    ls_sess-changed_by = sy-uname.
    IF iv_deepen_level IS INITIAL.
      ls_sess-error_text = 'DEEPEN=1'.
    ELSE.
      ls_sess-error_text = |DEEPEN={ iv_deepen_level }|.
    ENDIF.

    INSERT zaog_fetch_sess FROM ls_sess.
  ENDMETHOD.


  METHOD update_session_progress.
    DATA lv_ts TYPE timestampl.

    GET TIME STAMP FIELD lv_ts.
    IF iv_curr_offset IS SUPPLIED.
      UPDATE zaog_fetch_sess
        SET obj_done = iv_obj_done curr_offset = iv_curr_offset updated_at = lv_ts
        WHERE session_id = iv_session_id.
    ELSE.
      UPDATE zaog_fetch_sess
        SET obj_done = iv_obj_done updated_at = lv_ts
        WHERE session_id = iv_session_id.
    ENDIF.
  ENDMETHOD.


  METHOD fail_session.
    DATA lv_ts TYPE timestampl.

    GET TIME STAMP FIELD lv_ts.
    UPDATE zaog_fetch_sess
      SET obj_done = iv_obj_done status = 'F' updated_at = lv_ts
      WHERE session_id = iv_session_id.
  ENDMETHOD.


  METHOD complete_session.
    DATA lv_ts TYPE timestampl.

    GET TIME STAMP FIELD lv_ts.
    UPDATE zaog_fetch_sess
      SET phase = 'C' status = 'C' updated_at = lv_ts
      WHERE session_id = iv_session_id.
  ENDMETHOD.


  METHOD find_active_session.
    DATA ls_sess TYPE zaog_fetch_sess.
    DATA lv_deepen_txt TYPE string.

    SELECT SINGLE * FROM zaog_fetch_sess
      INTO ls_sess
      WHERE repo_key = iv_repo_key
        AND status = 'A'.

    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    rs_session-session_id = ls_sess-session_id.
    rs_session-repo_key = ls_sess-repo_key.
    rs_session-branch_name = ls_sess-branch_name.
    rs_session-pack_id = ls_sess-pack_id.
    rs_session-phase = ls_sess-phase.
    rs_session-curr_offset = ls_sess-curr_offset.
    rs_session-obj_done = ls_sess-obj_done.
    rs_session-obj_total = ls_sess-obj_total.
    rs_session-status = ls_sess-status.
    rs_session-deepen_level = 1.
    IF ls_sess-error_text IS NOT INITIAL.
      lv_deepen_txt = ls_sess-error_text.
      REPLACE FIRST OCCURRENCE OF 'DEEPEN=' IN lv_deepen_txt WITH ''.
      TRY.
          rs_session-deepen_level = lv_deepen_txt.
        CATCH cx_root.
          rs_session-deepen_level = 1.
      ENDTRY.
    ENDIF.
  ENDMETHOD.


  METHOD get_session.
    DATA ls_sess TYPE zaog_fetch_sess.
    DATA lv_deepen_txt TYPE string.

    SELECT SINGLE * FROM zaog_fetch_sess
      INTO ls_sess
      WHERE session_id = iv_session_id.

    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    rs_session-session_id = ls_sess-session_id.
    rs_session-repo_key = ls_sess-repo_key.
    rs_session-branch_name = ls_sess-branch_name.
    rs_session-pack_id = ls_sess-pack_id.
    rs_session-phase = ls_sess-phase.
    rs_session-curr_offset = ls_sess-curr_offset.
    rs_session-obj_done = ls_sess-obj_done.
    rs_session-obj_total = ls_sess-obj_total.
    rs_session-status = ls_sess-status.
    rs_session-deepen_level = 1.
    IF ls_sess-error_text IS NOT INITIAL.
      lv_deepen_txt = ls_sess-error_text.
      REPLACE FIRST OCCURRENCE OF 'DEEPEN=' IN lv_deepen_txt WITH ''.
      TRY.
          rs_session-deepen_level = lv_deepen_txt.
        CATCH cx_root.
          rs_session-deepen_level = 1.
      ENDTRY.
    ENDIF.
  ENDMETHOD.


  METHOD cleanup_partial_session.
    DATA lv_ts TYPE timestampl.

    IF is_session-session_id IS INITIAL
       OR is_session-repo_key IS INITIAL
       OR is_session-pack_id IS INITIAL.
      RETURN.
    ENDIF.

    " Remove decode artifacts that are only useful for resume.
    DELETE FROM zaog_pack_idx
      WHERE repo_key = is_session-repo_key
        AND pack_id  = is_session-pack_id.

    DELETE FROM zaog_obj_store
      WHERE repo_key = is_session-repo_key
        AND pack_id  = is_session-pack_id
        AND status   = 'P'.

    DELETE FROM zaog_raw_pack
      WHERE repo_key = is_session-repo_key
        AND pack_id  = is_session-pack_id.

    UPDATE zaog_pack_meta
      SET status = 'F' obj_decoded = is_session-obj_done
      WHERE repo_key = is_session-repo_key
        AND pack_id  = is_session-pack_id
        AND status   = 'P'.

    GET TIME STAMP FIELD lv_ts.
    IF iv_reason IS INITIAL.
      UPDATE zaog_fetch_sess
        SET phase = 'F' status = 'F' updated_at = lv_ts
        WHERE session_id = is_session-session_id.
    ELSE.
      UPDATE zaog_fetch_sess
        SET phase = 'F' status = 'F' error_text = iv_reason updated_at = lv_ts
        WHERE session_id = is_session-session_id.
    ENDIF.
  ENDMETHOD.


  METHOD acquire_repo_lock.
    DATA ls_lock TYPE zaog_fetch_sess.
    DATA lv_ts TYPE timestampl.
    DATA lv_wait_s TYPE f.
    DATA lv_jitter_ms TYPE i.

    rv_lock_id = |LOCK_{ iv_repo_key }|.

    IF iv_max_attempts <= 0.
      zcx_abapgit_exception=>raise( 'Repo lock: invalid max attempts' ).
    ENDIF.

    DO iv_max_attempts TIMES.
      GET TIME STAMP FIELD lv_ts.

      CLEAR ls_lock.
      ls_lock-session_id = rv_lock_id.
      ls_lock-repo_key   = iv_repo_key.
      ls_lock-phase      = 'L'.
      ls_lock-status     = 'L'.
      ls_lock-error_text = 'MUTEX'.
      ls_lock-created_at = lv_ts.
      ls_lock-updated_at = lv_ts.
      ls_lock-changed_by = sy-uname.

      INSERT zaog_fetch_sess FROM ls_lock.
      IF sy-subrc = 0.
        RETURN.
      ENDIF.

      lv_jitter_ms = ( sy-index * 31 + strlen( iv_repo_key ) * 17 ) MOD 41.
      lv_wait_s = ( iv_base_wait_ms * ( 2 ** ( sy-index - 1 ) ) + lv_jitter_ms ) / 1000.
      IF lv_wait_s > 2.
        lv_wait_s = 2.
      ENDIF.
      WAIT UP TO lv_wait_s SECONDS.
    ENDDO.

    zcx_abapgit_exception=>raise( |Repo lock timeout for key { iv_repo_key }| ).
  ENDMETHOD.


  METHOD release_repo_lock.
    IF iv_lock_id IS INITIAL.
      RETURN.
    ENDIF.

    DELETE FROM zaog_fetch_sess
      WHERE session_id = iv_lock_id
        AND status     = 'L'.
  ENDMETHOD.

ENDCLASS.
