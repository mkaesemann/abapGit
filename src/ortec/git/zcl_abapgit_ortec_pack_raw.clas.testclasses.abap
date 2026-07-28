CLASS zcl_abapgit_ortec_pack_raw DEFINITION LOCAL FRIENDS ltcl_pack_raw.

CLASS ltcl_pack_raw DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_RAW'.
    METHODS setup.
    METHODS teardown.
    "! D2B1 attempt-id correlation (target_design §9).
    METHODS attempt_id_on_fetch_sess FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_pack_raw IMPLEMENTATION.
  METHOD setup.
    DELETE FROM zaog_fetch_sess WHERE repo_key = mc_repo.
    COMMIT WORK.
  ENDMETHOD.

  METHOD teardown.
    ROLLBACK WORK.
    DELETE FROM zaog_fetch_sess WHERE repo_key = mc_repo.
    COMMIT WORK.
  ENDMETHOD.

  METHOD attempt_id_on_fetch_sess.
    " D2B1 (target_design §9): create_session's optional iv_attempt_id must
    " be persisted onto the new ZAOG_FETCH_SESS.ATTEMPT_ID column so a
    " resumable-decode session can be correlated with the owning
    " ZAOG_COMMIT_HIST attempt for diagnostics/cleanup.
    DATA lv_session_id TYPE zcl_abapgit_ortec_pack_raw=>ty_session_id.
    DATA lv_attempt    TYPE zcl_abapgit_ortec_mat_state=>ty_attempt_id.
    DATA lv_actual     TYPE zaog_fetch_sess-attempt_id.

    lv_attempt = 'ATTEMPT_D2B1_FETCH_SESS_0001'.

    lv_session_id = zcl_abapgit_ortec_pack_raw=>create_session(
      iv_repo_key   = mc_repo
      iv_pack_id    = 'ZAOGT_RAW_PACK_TEST'
      iv_obj_total  = 1
      iv_attempt_id = lv_attempt ).
    COMMIT WORK.

    cl_abap_unit_assert=>assert_not_initial( act = lv_session_id
      msg = 'create_session must return a non-initial session id' ).

    SELECT SINGLE attempt_id FROM zaog_fetch_sess INTO lv_actual
      WHERE session_id = lv_session_id.
    cl_abap_unit_assert=>assert_equals( act = lv_actual exp = lv_attempt
      msg = 'A supplied iv_attempt_id must be persisted onto the resulting ZAOG_FETCH_SESS row' ).
  ENDMETHOD.
ENDCLASS.
