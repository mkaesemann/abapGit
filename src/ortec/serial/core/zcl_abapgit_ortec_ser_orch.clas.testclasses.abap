CLASS zcl_abapgit_ortec_ser_orch DEFINITION LOCAL FRIENDS ltcl_ser_orch.

CLASS ltcl_ser_orch DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  PRIVATE SECTION.
    METHODS setup.

    METHODS build_run_id
      RETURNING VALUE(rv_run_id) TYPE sysuuid_x16.

    METHODS build_result
      IMPORTING iv_obj_type     TYPE trobjtype
                iv_obj_name     TYPE sobj_name
                iv_rc           TYPE i DEFAULT 0
      RETURNING VALUE(rs_result) TYPE zaog_ser_batch_result.

    METHODS build_tadir
      IMPORTING iv_obj_type    TYPE trobjtype
                iv_obj_name    TYPE sobj_name
      RETURNING VALUE(rs_tadir) TYPE zif_abapgit_definitions=>ty_tadir.

    METHODS key_sets_equal_match FOR TESTING.
    METHODS key_sets_equal_count_mismatch FOR TESTING.
    METHODS key_sets_content_mismatch FOR TESTING.

    METHODS breaker_stays_closed_below_min FOR TESTING.
    METHODS breaker_trips_on_high_failure FOR TESTING.
    METHODS breaker_ignores_other_runs FOR TESTING.

    METHODS purge_blocked_while_awaiting FOR TESTING.
    METHODS purge_keeps_abandoned_rows FOR TESTING.
    METHODS purge_removes_terminal_rows FOR TESTING.

    METHODS release_budget_floors_at_zero FOR TESTING.

    METHODS no_parallel_parity FOR TESTING.

    METHODS next_task_name_is_unique FOR TESTING.
    METHODS breaker_gates_before_dispatch FOR TESTING.
    METHODS merge_fails_without_context FOR TESTING.
    METHODS merge_fails_on_bad_payload FOR TESTING.
    METHODS merge_succeeds_with_payload FOR TESTING.

ENDCLASS.


CLASS ltcl_ser_orch IMPLEMENTATION.

  METHOD setup.
    " isolate every test from any other test's static state - see
    " git-workflow-safety / abap-coding conventions for this codebase's
    " established pattern of clearing CLASS-DATA between tests.
    CLEAR zcl_abapgit_ortec_ser_orch=>mt_dispatch.
    CLEAR zcl_abapgit_ortec_ser_orch=>mt_resolved.
    CLEAR zcl_abapgit_ortec_ser_orch=>mt_task_outcomes.
    CLEAR zcl_abapgit_ortec_ser_orch=>mt_broken_runs.
    CLEAR zcl_abapgit_ortec_ser_orch=>mt_run_context.
  ENDMETHOD.

  METHOD build_run_id.
    TRY.
        rv_run_id = cl_system_uuid=>create_uuid_x16_static( ).
      CATCH cx_uuid_error INTO DATA(lx_uuid).
        cl_abap_unit_assert=>fail( msg = lx_uuid->get_text( ) ).
    ENDTRY.
  ENDMETHOD.

  METHOD build_result.
    rs_result-obj_type = iv_obj_type.
    rs_result-obj_name = iv_obj_name.
    rs_result-rc       = iv_rc.
  ENDMETHOD.

  METHOD build_tadir.
    rs_tadir-object   = iv_obj_type.
    rs_tadir-obj_name = iv_obj_name.
  ENDMETHOD.

  METHOD key_sets_equal_match.
    DATA(lt_result) = VALUE zaog_ser_batch_result_tt(
      ( build_result( iv_obj_type = 'CLAS' iv_obj_name = 'A' ) )
      ( build_result( iv_obj_type = 'CLAS' iv_obj_name = 'B' ) ) ).
    DATA(lt_keys) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'A' ) )
      ( build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'B' ) ) ).

    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_orch=>object_key_sets_equal( it_result = lt_result it_object_keys = lt_keys ) ).
  ENDMETHOD.

  METHOD key_sets_equal_count_mismatch.
    DATA(lt_result) = VALUE zaog_ser_batch_result_tt(
      ( build_result( iv_obj_type = 'CLAS' iv_obj_name = 'A' ) ) ).
    DATA(lt_keys) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'A' ) )
      ( build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'B' ) ) ).

    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_orch=>object_key_sets_equal( it_result = lt_result it_object_keys = lt_keys ) ).
  ENDMETHOD.

  METHOD key_sets_content_mismatch.
    DATA(lt_result) = VALUE zaog_ser_batch_result_tt(
      ( build_result( iv_obj_type = 'CLAS' iv_obj_name = 'A' ) )
      ( build_result( iv_obj_type = 'CLAS' iv_obj_name = 'C' ) ) ).
    DATA(lt_keys) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'A' ) )
      ( build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'B' ) ) ).

    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_orch=>object_key_sets_equal( it_result = lt_result it_object_keys = lt_keys ) ).
  ENDMETHOD.

  METHOD breaker_stays_closed_below_min.
    DATA(lv_run) = build_run_id( ).

    " c_breaker_min_sample = 5: only 4 confirmed failures - must not trip
    DO 4 TIMES.
      zcl_abapgit_ortec_ser_orch=>record_task_outcome( iv_run_id = lv_run iv_success = abap_false ).
    ENDDO.

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_broken_runs
      WITH TABLE KEY table_line = lv_run
      TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( exp = 4 act = sy-subrc ).
  ENDMETHOD.

  METHOD breaker_trips_on_high_failure.
    DATA(lv_run) = build_run_id( ).

    " 5 confirmed outcomes, 4 failures = 80% >= c_breaker_failure_ratio (70%)
    DO 4 TIMES.
      zcl_abapgit_ortec_ser_orch=>record_task_outcome( iv_run_id = lv_run iv_success = abap_false ).
    ENDDO.
    zcl_abapgit_ortec_ser_orch=>record_task_outcome( iv_run_id = lv_run iv_success = abap_true ).

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_broken_runs
      WITH TABLE KEY table_line = lv_run
      TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( exp = 0 act = sy-subrc ).
  ENDMETHOD.

  METHOD breaker_ignores_other_runs.
    DATA(lv_run_a) = build_run_id( ).
    DATA(lv_run_b) = build_run_id( ).

    " run A: 5 confirmed failures - trips
    DO 5 TIMES.
      zcl_abapgit_ortec_ser_orch=>record_task_outcome( iv_run_id = lv_run_a iv_success = abap_false ).
    ENDDO.
    " run B: 5 confirmed successes - must stay healthy regardless of A
    DO 5 TIMES.
      zcl_abapgit_ortec_ser_orch=>record_task_outcome( iv_run_id = lv_run_b iv_success = abap_true ).
    ENDDO.

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_broken_runs
      WITH TABLE KEY table_line = lv_run_a
      TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( exp = 0 act = sy-subrc ).

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_broken_runs
      WITH TABLE KEY table_line = lv_run_b
      TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( exp = 4 act = sy-subrc ).
  ENDMETHOD.

  METHOD purge_blocked_while_awaiting.
    DATA(lv_run) = build_run_id( ).
    INSERT VALUE #( task_name = 'T1' run_id = lv_run
                     state = zcl_abapgit_ortec_ser_orch=>c_state_awaiting )
      INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_dispatch.
    INSERT VALUE #( run_id = lv_run ) INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    zcl_abapgit_ortec_ser_orch=>purge_run_state( lv_run ).

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context
      WITH TABLE KEY run_id = lv_run
      TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( exp = 0 act = sy-subrc ).
  ENDMETHOD.

  METHOD purge_keeps_abandoned_rows.
    DATA(lv_run) = build_run_id( ).
    INSERT VALUE #( task_name = 'T1' run_id = lv_run
                     state = zcl_abapgit_ortec_ser_orch=>c_state_abandoned )
      INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_dispatch.
    INSERT VALUE #( run_id = lv_run ) INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    zcl_abapgit_ortec_ser_orch=>purge_run_state( lv_run ).

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_dispatch
      WITH TABLE KEY task_name = 'T1'
      TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( exp = 0 act = sy-subrc ).

    " the run's OTHER state (context, resolved, outcomes) is still purged
    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context
      WITH TABLE KEY run_id = lv_run
      TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( exp = 4 act = sy-subrc ).
  ENDMETHOD.

  METHOD purge_removes_terminal_rows.
    DATA(lv_run) = build_run_id( ).
    INSERT VALUE #( task_name = 'T1' run_id = lv_run
                     state = zcl_abapgit_ortec_ser_orch=>c_state_received )
      INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_dispatch.
    INSERT VALUE #( run_id = lv_run ) INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    zcl_abapgit_ortec_ser_orch=>purge_run_state( lv_run ).

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_dispatch
      WITH TABLE KEY task_name = 'T1'
      TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( exp = 4 act = sy-subrc ).
  ENDMETHOD.

  METHOD release_budget_floors_at_zero.
    DATA(lv_run) = build_run_id( ).
    INSERT VALUE #( run_id = lv_run in_flight = 0 ) INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    zcl_abapgit_ortec_ser_orch=>release_in_flight_budget( lv_run ).

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context INTO DATA(ls_ctx) WITH TABLE KEY run_id = lv_run.
    cl_abap_unit_assert=>assert_equals( act = ls_ctx-in_flight exp = 0 ).
  ENDMETHOD.

  METHOD no_parallel_parity.
    " Pins ZCL_ABAPGIT_ORTEC_SER_ORCH's local IS_STANDARD_NO_PARALLEL_TYPE
    " copy against ZCL_ABAPGIT_SERIALIZE=>IS_NO_PARALLEL's ACTUAL, current,
    " PRIVATE source (read directly, not called - that method stays
    " private, see this test's own class-level doc and the method's own
    " ABAP Doc). If the standard method's denylist ever changes, this
    " test's own hardcoded expectations must be reviewed and updated to
    " match, keeping the two in permanent, visible sync instead of silent
    " drift. Covers every currently-denylisted type plus representative
    " allowed types.
    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_ser_orch=>is_standard_no_parallel_type( 'ECTC' ) ).
    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_ser_orch=>is_standard_no_parallel_type( 'ECTD' ) ).

    cl_abap_unit_assert=>assert_false(
      act = zcl_abapgit_ortec_ser_orch=>is_standard_no_parallel_type( 'CLAS' ) ).
    cl_abap_unit_assert=>assert_false(
      act = zcl_abapgit_ortec_ser_orch=>is_standard_no_parallel_type( 'INTF' ) ).
    cl_abap_unit_assert=>assert_false(
      act = zcl_abapgit_ortec_ser_orch=>is_standard_no_parallel_type( 'DDLS' ) ).
    cl_abap_unit_assert=>assert_false(
      act = zcl_abapgit_ortec_ser_orch=>is_standard_no_parallel_type( 'WAPA' ) ).
  ENDMETHOD.

  METHOD next_task_name_is_unique.
    " AR-1-003 regression test (independent adversarial audit) - the
    " earlier truncated-RUN_ID-hex scheme could produce identical task
    " names for two different runs; the session-wide monotonic counter
    " cannot.
    DATA(lv_name_1) = zcl_abapgit_ortec_ser_orch=>next_task_name( ).
    DATA(lv_name_2) = zcl_abapgit_ortec_ser_orch=>next_task_name( ).

    cl_abap_unit_assert=>assert_differs( act = lv_name_2 exp = lv_name_1 ).
  ENDMETHOD.

  METHOD breaker_gates_before_dispatch.
    " AR-1-002 regression test (independent adversarial audit) - a
    " tripped breaker must stop BEFORE_DISPATCH from ever reaching
    " DISPATCH_BATCH (no new MT_DISPATCH row). No run context is
    " inserted, so the fallback it routes to returns immediately without
    " touching any real object - a zero-risk, deterministic check of the
    " gate itself.
    DATA(lv_run) = build_run_id( ).
    INSERT lv_run INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_broken_runs.

    DATA(lt_keys) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'ZZZ' ) ) ).

    zcl_abapgit_ortec_ser_orch=>before_dispatch(
      iv_run_id      = lv_run
      it_object_keys = lt_keys
      iv_attempt     = 1
      iv_batch_id    = 'B1' ).

    cl_abap_unit_assert=>assert_initial( zcl_abapgit_ortec_ser_orch=>mt_dispatch ).
  ENDMETHOD.

  METHOD merge_fails_without_context.
    " AR-1-004 regression test (independent adversarial audit).
    DATA(lv_run) = build_run_id( ).

    DATA(lv_merged) = zcl_abapgit_ortec_ser_orch=>merge_into_mt_files(
      iv_run_id = lv_run
      is_tadir  = build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'A' )
      is_result = build_result( iv_obj_type = 'CLAS' iv_obj_name = 'A' ) ).

    cl_abap_unit_assert=>assert_false( lv_merged ).
  ENDMETHOD.

  METHOD merge_fails_on_bad_payload.
    " AR-1-004 regression test (independent adversarial audit) - a
    " corrupted/incompatible FILES_XSTRING must not be silently treated
    " as a successful merge.
    DATA(lv_run) = build_run_id( ).
    INSERT VALUE #( run_id = lv_run ) INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    DATA(ls_result) = build_result( iv_obj_type = 'CLAS' iv_obj_name = 'A' ).
    ls_result-files_xstring = '0102030405'.

    DATA(lv_merged) = zcl_abapgit_ortec_ser_orch=>merge_into_mt_files(
      iv_run_id = lv_run
      is_tadir  = build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'A' )
      is_result = ls_result ).

    cl_abap_unit_assert=>assert_false( lv_merged ).
  ENDMETHOD.

  METHOD merge_succeeds_with_payload.
    DATA(lv_run) = build_run_id( ).
    INSERT VALUE #( run_id = lv_run ) INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    DATA ls_serialization TYPE zif_abapgit_objects=>ty_serialization.
    ls_serialization-item-obj_type = 'CLAS'.
    ls_serialization-item-obj_name = 'ZCL_TEST'.
    APPEND INITIAL LINE TO ls_serialization-files ASSIGNING FIELD-SYMBOL(<ls_file>).
    <ls_file>-filename = 'zcl_test.clas.abap'.

    DATA lv_buffer TYPE xstring.
    EXPORT data = ls_serialization TO DATA BUFFER lv_buffer.

    DATA(ls_result) = build_result( iv_obj_type = 'CLAS' iv_obj_name = 'ZCL_TEST' ).
    ls_result-files_xstring = lv_buffer.

    DATA(lv_merged) = zcl_abapgit_ortec_ser_orch=>merge_into_mt_files(
      iv_run_id = lv_run
      is_tadir  = build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'ZCL_TEST' )
      is_result = ls_result ).

    cl_abap_unit_assert=>assert_true( lv_merged ).

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context INTO DATA(ls_ctx) WITH TABLE KEY run_id = lv_run.
    cl_abap_unit_assert=>assert_equals( act = lines( ls_ctx-files ) exp = 1 ).
  ENDMETHOD.

ENDCLASS.
