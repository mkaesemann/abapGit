CLASS zcl_abapgit_ortec_ser_orch DEFINITION LOCAL FRIENDS ltcl_ser_orch.

CLASS ltcl_ser_orch DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  PRIVATE SECTION.
    METHODS setup.

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
    DATA(lv_run) = cl_system_uuid=>create_uuid_x16_static( ).

    " c_breaker_min_sample = 5: only 4 confirmed failures - must not trip
    DO 4 TIMES.
      zcl_abapgit_ortec_ser_orch=>record_task_outcome( iv_run_id = lv_run iv_success = abap_false ).
    ENDDO.

    cl_abap_unit_assert=>assert_false(
      line_exists( zcl_abapgit_ortec_ser_orch=>mt_broken_runs[ table_line = lv_run ] ) ).
  ENDMETHOD.

  METHOD breaker_trips_on_high_failure.
    DATA(lv_run) = cl_system_uuid=>create_uuid_x16_static( ).

    " 5 confirmed outcomes, 4 failures = 80% >= c_breaker_failure_ratio (70%)
    DO 4 TIMES.
      zcl_abapgit_ortec_ser_orch=>record_task_outcome( iv_run_id = lv_run iv_success = abap_false ).
    ENDDO.
    zcl_abapgit_ortec_ser_orch=>record_task_outcome( iv_run_id = lv_run iv_success = abap_true ).

    cl_abap_unit_assert=>assert_true(
      line_exists( zcl_abapgit_ortec_ser_orch=>mt_broken_runs[ table_line = lv_run ] ) ).
  ENDMETHOD.

  METHOD breaker_ignores_other_runs.
    DATA(lv_run_a) = cl_system_uuid=>create_uuid_x16_static( ).
    DATA(lv_run_b) = cl_system_uuid=>create_uuid_x16_static( ).

    " run A: 5 confirmed failures - trips
    DO 5 TIMES.
      zcl_abapgit_ortec_ser_orch=>record_task_outcome( iv_run_id = lv_run_a iv_success = abap_false ).
    ENDDO.
    " run B: 5 confirmed successes - must stay healthy regardless of A
    DO 5 TIMES.
      zcl_abapgit_ortec_ser_orch=>record_task_outcome( iv_run_id = lv_run_b iv_success = abap_true ).
    ENDDO.

    cl_abap_unit_assert=>assert_true(
      line_exists( zcl_abapgit_ortec_ser_orch=>mt_broken_runs[ table_line = lv_run_a ] ) ).
    cl_abap_unit_assert=>assert_false(
      line_exists( zcl_abapgit_ortec_ser_orch=>mt_broken_runs[ table_line = lv_run_b ] ) ).
  ENDMETHOD.

  METHOD purge_blocked_while_awaiting.
    DATA(lv_run) = cl_system_uuid=>create_uuid_x16_static( ).
    INSERT VALUE #( task_name = 'T1' run_id = lv_run
                     state = zcl_abapgit_ortec_ser_orch=>c_state_awaiting )
      INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_dispatch.
    INSERT VALUE #( run_id = lv_run ) INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    zcl_abapgit_ortec_ser_orch=>purge_run_state( lv_run ).

    cl_abap_unit_assert=>assert_true(
      line_exists( zcl_abapgit_ortec_ser_orch=>mt_run_context[ run_id = lv_run ] ) ).
  ENDMETHOD.

  METHOD purge_keeps_abandoned_rows.
    DATA(lv_run) = cl_system_uuid=>create_uuid_x16_static( ).
    INSERT VALUE #( task_name = 'T1' run_id = lv_run
                     state = zcl_abapgit_ortec_ser_orch=>c_state_abandoned )
      INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_dispatch.
    INSERT VALUE #( run_id = lv_run ) INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    zcl_abapgit_ortec_ser_orch=>purge_run_state( lv_run ).

    cl_abap_unit_assert=>assert_true(
      line_exists( zcl_abapgit_ortec_ser_orch=>mt_dispatch[ task_name = 'T1' ] ) ).
    " the run's OTHER state (context, resolved, outcomes) is still purged
    cl_abap_unit_assert=>assert_false(
      line_exists( zcl_abapgit_ortec_ser_orch=>mt_run_context[ run_id = lv_run ] ) ).
  ENDMETHOD.

  METHOD purge_removes_terminal_rows.
    DATA(lv_run) = cl_system_uuid=>create_uuid_x16_static( ).
    INSERT VALUE #( task_name = 'T1' run_id = lv_run
                     state = zcl_abapgit_ortec_ser_orch=>c_state_received )
      INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_dispatch.
    INSERT VALUE #( run_id = lv_run ) INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    zcl_abapgit_ortec_ser_orch=>purge_run_state( lv_run ).

    cl_abap_unit_assert=>assert_false(
      line_exists( zcl_abapgit_ortec_ser_orch=>mt_dispatch[ task_name = 'T1' ] ) ).
  ENDMETHOD.

  METHOD release_budget_floors_at_zero.
    DATA(lv_run) = cl_system_uuid=>create_uuid_x16_static( ).
    INSERT VALUE #( run_id = lv_run in_flight = 0 ) INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    zcl_abapgit_ortec_ser_orch=>release_in_flight_budget( lv_run ).

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context INTO DATA(ls_ctx) WITH TABLE KEY run_id = lv_run.
    cl_abap_unit_assert=>assert_equals( act = ls_ctx-in_flight exp = 0 ).
  ENDMETHOD.

ENDCLASS.
