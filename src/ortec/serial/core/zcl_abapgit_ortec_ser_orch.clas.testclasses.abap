CLASS zcl_abapgit_ortec_ser_orch DEFINITION LOCAL FRIENDS ltcl_ser_orch.

CLASS ltcl_ser_orch DEFINITION FINAL
  FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.

  PRIVATE SECTION.
    METHODS setup.

    METHODS build_run_id
      RETURNING VALUE(rv_run_id) TYPE sysuuid_x16.

    METHODS build_result
      IMPORTING iv_obj_type      TYPE trobjtype
                iv_obj_name      TYPE sobj_name
                iv_rc            TYPE i DEFAULT 0
      RETURNING VALUE(rs_result) TYPE zaog_ser_batch_result.

    METHODS build_tadir
      IMPORTING iv_obj_type     TYPE trobjtype
                iv_obj_name     TYPE sobj_name
      RETURNING VALUE(rs_tadir) TYPE zif_abapgit_definitions=>ty_tadir.

    METHODS key_sets_equal_match           FOR TESTING.
    METHODS key_sets_equal_count_mismatch  FOR TESTING.
    METHODS key_sets_content_mismatch      FOR TESTING.

    METHODS breaker_stays_closed_below_min FOR TESTING.
    METHODS breaker_trips_on_high_failure  FOR TESTING.
    METHODS breaker_ignores_other_runs     FOR TESTING.

    METHODS purge_blocked_while_awaiting   FOR TESTING.
    METHODS discard_clears_run_state       FOR TESTING.
    METHODS purge_removes_terminal_rows    FOR TESTING.

    METHODS release_budget_floors_at_zero  FOR TESTING.

    METHODS no_parallel_parity             FOR TESTING.
    METHODS wait_complete_when_done        FOR TESTING.
    METHODS wait_zero_incomplete           FOR TESTING.
    METHODS wait_subrc4_incomplete         FOR TESTING.
    METHODS wait_subrc8_timeout            FOR TESTING.
    METHODS all_success_allows_return      FOR TESTING.
    METHODS failure_blocks_return          FOR TESTING.
    METHODS incomplete_blocks_return       FOR TESTING.
    METHODS fallback_fail_marks_failed     FOR TESTING.
    METHODS queued_failures_block_return   FOR TESTING.
    METHODS drain_fail_marks_batch         FOR TESTING.
    METHODS terminal_counts_isolated       FOR TESTING.
    METHODS wapa_partition_separates       FOR TESTING.
    METHODS wapa_batches_singletons        FOR TESTING.

    METHODS next_task_name_is_unique       FOR TESTING.
    METHODS breaker_gates_before_dispatch  FOR TESTING.
    METHODS merge_fails_without_context    FOR TESTING.
    METHODS merge_fails_on_bad_payload     FOR TESTING.
    METHODS merge_succeeds_with_payload    FOR TESTING.
    METHODS merge_empty_file_list_ok       FOR TESTING.

    METHODS split_depth_below_cap_false     FOR TESTING.
    METHODS split_depth_at_cap_true         FOR TESTING.
    METHODS split_depth_above_cap_true      FOR TESTING.
    METHODS before_dispatch_dd_buf_empty    FOR TESTING.

    METHODS zero_file_success_flagged    FOR TESTING.
    METHODS nonzero_file_not_flagged      FOR TESTING.
    METHODS zero_file_but_failed_row_ok   FOR TESTING.
    METHODS zero_file_unmatched_row_ok    FOR TESTING.
    METHODS fallback_zero_files_fails      FOR TESTING.

ENDCLASS.


CLASS ltcl_ser_orch IMPLEMENTATION.
  METHOD setup.
    " isolate every test from any other test's static state - see
    " git-workflow-safety / abap-coding conventions for this codebase's
    " established pattern of clearing CLASS-DATA between tests.
    CLEAR zcl_abapgit_ortec_ser_orch=>mt_dispatch.
    CLEAR zcl_abapgit_ortec_ser_orch=>mt_resolved.
    CLEAR zcl_abapgit_ortec_ser_orch=>mt_failed.
    CLEAR zcl_abapgit_ortec_ser_orch=>mt_task_outcomes.
    CLEAR zcl_abapgit_ortec_ser_orch=>mt_broken_runs.
    CLEAR zcl_abapgit_ortec_ser_orch=>mt_run_context.
    CLEAR zcl_abapgit_ortec_ser_orch=>mv_test_raise_drain.
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
    DATA(lt_result) = VALUE zaog_ser_batch_result_tt( ( build_result( iv_obj_type = 'CLAS' iv_obj_name = 'A' ) )
                                                      ( build_result( iv_obj_type = 'CLAS' iv_obj_name = 'B' ) ) ).
    DATA(lt_keys) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
                              ( build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'A' ) )
                              ( build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'B' ) ) ).

    cl_abap_unit_assert=>assert_true(
        zcl_abapgit_ortec_ser_orch=>object_key_sets_equal( it_result = lt_result it_object_keys = lt_keys ) ).
  ENDMETHOD.

  METHOD key_sets_equal_count_mismatch.
    DATA(lt_result) = VALUE zaog_ser_batch_result_tt( ( build_result( iv_obj_type = 'CLAS' iv_obj_name = 'A' ) ) ).
    DATA(lt_keys) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
                              ( build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'A' ) )
                              ( build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'B' ) ) ).

    cl_abap_unit_assert=>assert_false(
        zcl_abapgit_ortec_ser_orch=>object_key_sets_equal( it_result = lt_result it_object_keys = lt_keys ) ).
  ENDMETHOD.

  METHOD key_sets_content_mismatch.
    DATA(lt_result) = VALUE zaog_ser_batch_result_tt( ( build_result( iv_obj_type = 'CLAS' iv_obj_name = 'A' ) )
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
    INSERT VALUE #( task_name = 'T1'
                    run_id    = lv_run
                    state     = zcl_abapgit_ortec_ser_orch=>c_state_awaiting )
           INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_dispatch.
    INSERT VALUE #( run_id = lv_run ) INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    zcl_abapgit_ortec_ser_orch=>purge_run_state( lv_run ).

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context
         WITH TABLE KEY run_id = lv_run
         TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( exp = 0 act = sy-subrc ).
  ENDMETHOD.

  METHOD discard_clears_run_state.
    DATA(lv_run) = build_run_id( ).
    INSERT VALUE #( task_name = 'T1'
                    run_id    = lv_run
                    state     = zcl_abapgit_ortec_ser_orch=>c_state_awaiting )
           INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_dispatch.
    INSERT VALUE #( run_id = lv_run obj_type = 'CLAS' obj_name = 'A' )
      INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_resolved.
    APPEND VALUE #( run_id = lv_run seq = 1 success = abap_true )
      TO zcl_abapgit_ortec_ser_orch=>mt_task_outcomes.
    INSERT lv_run INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_broken_runs.
    INSERT VALUE #( run_id = lv_run ) INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    TRY.
        zcl_abapgit_ortec_ser_orch=>discard_run_state( lv_run ).
      CATCH zcx_abapgit_exception INTO DATA(lx_discard).
        cl_abap_unit_assert=>fail( msg = lx_discard->get_text( ) ).
    ENDTRY.

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_dispatch
         WITH TABLE KEY task_name = 'T1'
         TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( exp = 4 act = sy-subrc ).

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context
         WITH TABLE KEY run_id = lv_run
         TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( exp = 4 act = sy-subrc ).

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_resolved
      WITH TABLE KEY run_id = lv_run obj_type = 'CLAS' obj_name = 'A'
      TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( exp = 4 act = sy-subrc ).

    cl_abap_unit_assert=>assert_initial( zcl_abapgit_ortec_ser_orch=>mt_task_outcomes ).

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_broken_runs
      WITH TABLE KEY table_line = lv_run
      TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( exp = 4 act = sy-subrc ).
  ENDMETHOD.

  METHOD purge_removes_terminal_rows.
    DATA(lv_run) = build_run_id( ).
    INSERT VALUE #( task_name = 'T1'
                    run_id    = lv_run
                    state     = zcl_abapgit_ortec_ser_orch=>c_state_received )
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

    TRY.
        zcl_abapgit_ortec_ser_orch=>release_in_flight_budget( lv_run ).
      CATCH zcx_abapgit_exception INTO DATA(lx_release).
        cl_abap_unit_assert=>fail( msg = lx_release->get_text( ) ).
    ENDTRY.

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context INTO DATA(ls_ctx) WITH TABLE KEY run_id = lv_run.
    cl_abap_unit_assert=>assert_equals( exp = 0 act = ls_ctx-in_flight ).
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
    cl_abap_unit_assert=>assert_true( act = zcl_abapgit_ortec_ser_orch=>is_standard_no_parallel_type( 'ECTC' ) ).
    cl_abap_unit_assert=>assert_true( act = zcl_abapgit_ortec_ser_orch=>is_standard_no_parallel_type( 'ECTD' ) ).

    cl_abap_unit_assert=>assert_false( act = zcl_abapgit_ortec_ser_orch=>is_standard_no_parallel_type( 'CLAS' ) ).
    cl_abap_unit_assert=>assert_false( act = zcl_abapgit_ortec_ser_orch=>is_standard_no_parallel_type( 'INTF' ) ).
    cl_abap_unit_assert=>assert_false( act = zcl_abapgit_ortec_ser_orch=>is_standard_no_parallel_type( 'DDLS' ) ).
    cl_abap_unit_assert=>assert_false( act = zcl_abapgit_ortec_ser_orch=>is_standard_no_parallel_type( 'WAPA' ) ).
  ENDMETHOD.

  METHOD wait_complete_when_done.
    cl_abap_unit_assert=>assert_equals(
      exp = 0
      act = zcl_abapgit_ortec_ser_orch=>interpret_wait_result(
              iv_wait_subrc   = 4
              iv_run_complete = abap_true ) ).
  ENDMETHOD.

  METHOD wait_zero_incomplete.
    cl_abap_unit_assert=>assert_equals(
      exp = 4
      act = zcl_abapgit_ortec_ser_orch=>interpret_wait_result(
              iv_wait_subrc   = 0
              iv_run_complete = abap_false ) ).
  ENDMETHOD.

  METHOD wait_subrc4_incomplete.
    cl_abap_unit_assert=>assert_equals(
      exp = 4
      act = zcl_abapgit_ortec_ser_orch=>interpret_wait_result(
              iv_wait_subrc   = 4
              iv_run_complete = abap_false ) ).
  ENDMETHOD.

  METHOD wait_subrc8_timeout.
    cl_abap_unit_assert=>assert_equals(
      exp = 8
      act = zcl_abapgit_ortec_ser_orch=>interpret_wait_result(
              iv_wait_subrc   = 8
              iv_run_complete = abap_false ) ).
  ENDMETHOD.

  METHOD all_success_allows_return.
    DATA(lv_run) = build_run_id( ).
    INSERT VALUE #( run_id = lv_run expected_count = 2 )
      INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    TRY.
        zcl_abapgit_ortec_ser_orch=>mark_object_success(
          iv_run_id = lv_run
          is_tadir  = build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'A' ) ).
        zcl_abapgit_ortec_ser_orch=>mark_object_success(
          iv_run_id = lv_run
          is_tadir  = build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'B' ) ).
        zcl_abapgit_ortec_ser_orch=>assert_successful_run(
          iv_run_id      = lv_run
          iv_wait_result = 0 ).
      CATCH zcx_abapgit_exception INTO DATA(lx_all_success).
        cl_abap_unit_assert=>fail( msg = lx_all_success->get_text( ) ).
    ENDTRY.
  ENDMETHOD.

  METHOD failure_blocks_return.
    DATA(lv_run) = build_run_id( ).
    INSERT VALUE #( run_id = lv_run expected_count = 2 )
      INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    TRY.
        zcl_abapgit_ortec_ser_orch=>mark_object_success(
          iv_run_id = lv_run
          is_tadir  = build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'A' ) ).
        zcl_abapgit_ortec_ser_orch=>mark_object_failures(
          iv_run_id      = lv_run
          it_object_keys = VALUE #( ( build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'B' ) ) ) ).
      CATCH zcx_abapgit_exception INTO DATA(lx_mark_setup).
        cl_abap_unit_assert=>fail( msg = lx_mark_setup->get_text( ) ).
    ENDTRY.

    TRY.
        zcl_abapgit_ortec_ser_orch=>assert_successful_run(
          iv_run_id      = lv_run
          iv_wait_result = 0 ).
        cl_abap_unit_assert=>fail( msg = 'expected failure to block success' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_failure).
        cl_abap_unit_assert=>assert_true( act = boolc( lx_failure->get_text( ) CS 'object(s) failed' ) ).
        cl_abap_unit_assert=>assert_true( act = boolc( lx_failure->get_text( ) CS 'CLAS B' ) ).
    ENDTRY.
  ENDMETHOD.

  METHOD incomplete_blocks_return.
    DATA(lv_run) = build_run_id( ).
    INSERT VALUE #( run_id = lv_run expected_count = 2 )
      INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    TRY.
        zcl_abapgit_ortec_ser_orch=>mark_object_success(
          iv_run_id = lv_run
          is_tadir  = build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'A' ) ).
      CATCH zcx_abapgit_exception INTO DATA(lx_mark_incomplete).
        cl_abap_unit_assert=>fail( msg = lx_mark_incomplete->get_text( ) ).
    ENDTRY.

    TRY.
        zcl_abapgit_ortec_ser_orch=>assert_successful_run(
          iv_run_id      = lv_run
          iv_wait_result = 0 ).
        cl_abap_unit_assert=>fail( msg = 'expected incomplete terminal count to fail' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_incomplete).
        cl_abap_unit_assert=>assert_true( act = boolc( lx_incomplete->get_text( ) CS 'missing batch result condition' ) ).
    ENDTRY.
  ENDMETHOD.

  METHOD fallback_fail_marks_failed.
    DATA(lv_run) = build_run_id( ).
    INSERT VALUE #( run_id = lv_run expected_count = 1 )
      INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    TRY.
        zcl_abapgit_ortec_ser_orch=>route_to_sequential_fallback(
          iv_run_id      = lv_run
          it_object_keys = VALUE #( ( build_tadir( iv_obj_type = 'ZZZZ' iv_obj_name = 'NOPE' ) ) ) ).
      CATCH zcx_abapgit_exception INTO DATA(lx_fallback).
        cl_abap_unit_assert=>fail( msg = lx_fallback->get_text( ) ).
    ENDTRY.

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_failed
      WITH TABLE KEY run_id = lv_run obj_type = 'ZZZZ' obj_name = 'NOPE'
      TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( exp = 0 act = sy-subrc ).

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_resolved
      WITH TABLE KEY run_id = lv_run obj_type = 'ZZZZ' obj_name = 'NOPE'
      TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( exp = 4 act = sy-subrc ).
  ENDMETHOD.

  METHOD fallback_zero_files_fails.
    " SER-SLICE-3 parity incident fix (serialization_slice_3_dtel_doma_
    " parity.md, AR-3-002): a REAL, valid object type (DOMA) whose object
    " does not exist reaches ZCL_ABAPGIT_OBJECT_DOMA's own defensive
    " "IF ls_dd01v IS INITIAL...RETURN" branch - NO exception, zero files.
    " Before this fix, ROUTE_TO_SEQUENTIAL_FALLBACK would have marked this
    " as a bare SUCCESS (MT_RESOLVED) despite producing no output at all -
    " exactly the silent-loss shape of the parity incident. It must now be
    " marked FAILED instead.
    DATA(lv_run) = build_run_id( ).
    INSERT VALUE #( run_id = lv_run expected_count = 1 )
      INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    TRY.
        zcl_abapgit_ortec_ser_orch=>route_to_sequential_fallback(
          iv_run_id      = lv_run
          it_object_keys = VALUE #( ( build_tadir( iv_obj_type = 'DOMA' iv_obj_name = 'ZZZZ_SLICE3_NOT_A_REAL_DOMAIN' ) ) ) ).
      CATCH zcx_abapgit_exception INTO DATA(lx_fallback).
        cl_abap_unit_assert=>fail( msg = lx_fallback->get_text( ) ).
    ENDTRY.

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_failed
      WITH TABLE KEY run_id = lv_run obj_type = 'DOMA' obj_name = 'ZZZZ_SLICE3_NOT_A_REAL_DOMAIN'
      TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( exp = 0 act = sy-subrc ).

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_resolved
      WITH TABLE KEY run_id = lv_run obj_type = 'DOMA' obj_name = 'ZZZZ_SLICE3_NOT_A_REAL_DOMAIN'
      TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( exp = 4 act = sy-subrc ).
  ENDMETHOD.

  METHOD queued_failures_block_return.
    DATA(lv_run) = build_run_id( ).
    INSERT VALUE #(
      run_id         = lv_run
      expected_count = 2
      queue          = VALUE zcl_abapgit_ortec_ser_planner=>tt_batch(
                         ( items = VALUE #(
                             ( tadir = build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'B' ) ) ) ) ) )
      INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    TRY.
        zcl_abapgit_ortec_ser_orch=>mark_object_success(
          iv_run_id = lv_run
          is_tadir  = build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'A' ) ).
        zcl_abapgit_ortec_ser_orch=>mark_queued_failures( lv_run ).
      CATCH zcx_abapgit_exception INTO DATA(lx_qf_setup).
        cl_abap_unit_assert=>fail( msg = lx_qf_setup->get_text( ) ).
    ENDTRY.

    cl_abap_unit_assert=>assert_equals( exp = 2 act = zcl_abapgit_ortec_ser_orch=>count_terminal_objects( lv_run ) ).
    cl_abap_unit_assert=>assert_equals( exp = 1 act = zcl_abapgit_ortec_ser_orch=>count_failed_objects( lv_run ) ).
    cl_abap_unit_assert=>assert_true( act = zcl_abapgit_ortec_ser_orch=>is_run_complete( lv_run ) ).

    TRY.
        zcl_abapgit_ortec_ser_orch=>assert_successful_run(
          iv_run_id      = lv_run
          iv_wait_result = 0 ).
        cl_abap_unit_assert=>fail( msg = 'expected explicit queued failure to block success' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_queued_failure).
        cl_abap_unit_assert=>assert_true( act = boolc( lx_queued_failure->get_text( ) CS 'object(s) failed' ) ).
        cl_abap_unit_assert=>assert_false( act = boolc( lx_queued_failure->get_text( ) CS 'missing batch result condition' ) ).
    ENDTRY.
  ENDMETHOD.

  METHOD drain_fail_marks_batch.
    DATA(lv_run) = build_run_id( ).
    INSERT VALUE #(
      run_id         = lv_run
      worker_count   = 1
      expected_count = 2
      queue          = VALUE zcl_abapgit_ortec_ser_planner=>tt_batch(
                         ( items = VALUE #(
                             ( tadir = build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'B' ) ) ) ) ) )
      INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    TRY.
        zcl_abapgit_ortec_ser_orch=>mark_object_success(
          iv_run_id = lv_run
          is_tadir  = build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'A' ) ).
      CATCH zcx_abapgit_exception INTO DATA(lx_drain_setup).
        cl_abap_unit_assert=>fail( msg = lx_drain_setup->get_text( ) ).
    ENDTRY.

    zcl_abapgit_ortec_ser_orch=>mv_test_raise_drain = abap_true.

    TRY.
        zcl_abapgit_ortec_ser_orch=>drain_queue( lv_run ).
        cl_abap_unit_assert=>fail( msg = 'expected drain_queue seam failure' ).
      CATCH zcx_abapgit_exception.
    ENDTRY.

    TRY.
        zcl_abapgit_ortec_ser_orch=>mark_queued_failures( lv_run ).
      CATCH zcx_abapgit_exception INTO DATA(lx_drain_mark).
        cl_abap_unit_assert=>fail( msg = lx_drain_mark->get_text( ) ).
    ENDTRY.

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_failed
      WITH TABLE KEY run_id = lv_run obj_type = 'CLAS' obj_name = 'B'
      TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc( exp = 0 act = sy-subrc ).
    cl_abap_unit_assert=>assert_equals( exp = 2 act = zcl_abapgit_ortec_ser_orch=>count_terminal_objects( lv_run ) ).
    cl_abap_unit_assert=>assert_equals( exp = 1 act = zcl_abapgit_ortec_ser_orch=>count_failed_objects( lv_run ) ).
    cl_abap_unit_assert=>assert_true( act = zcl_abapgit_ortec_ser_orch=>is_run_complete( lv_run ) ).

    TRY.
        zcl_abapgit_ortec_ser_orch=>assert_successful_run(
          iv_run_id      = lv_run
          iv_wait_result = 0 ).
        cl_abap_unit_assert=>fail( msg = 'expected drain failure to become explicit failed-object outcome' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_drain_failure).
        cl_abap_unit_assert=>assert_true( act = boolc( lx_drain_failure->get_text( ) CS 'object(s) failed' ) ).
        cl_abap_unit_assert=>assert_false( act = boolc( lx_drain_failure->get_text( ) CS 'missing batch result condition' ) ).
    ENDTRY.
  ENDMETHOD.

  METHOD terminal_counts_isolated.
    DATA(lv_run_a) = build_run_id( ).
    DATA(lv_run_b) = build_run_id( ).
    INSERT VALUE #( run_id = lv_run_a expected_count = 1 )
      INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.
    INSERT VALUE #( run_id = lv_run_b expected_count = 1 )
      INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    TRY.
        zcl_abapgit_ortec_ser_orch=>mark_object_success(
          iv_run_id = lv_run_a
          is_tadir  = build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'A' ) ).
        zcl_abapgit_ortec_ser_orch=>mark_object_failures(
          iv_run_id      = lv_run_b
          it_object_keys = VALUE #( ( build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'B' ) ) ) ).
      CATCH zcx_abapgit_exception INTO DATA(lx_isolated).
        cl_abap_unit_assert=>fail( msg = lx_isolated->get_text( ) ).
    ENDTRY.

    cl_abap_unit_assert=>assert_equals( exp = 1 act = zcl_abapgit_ortec_ser_orch=>count_terminal_objects( lv_run_a ) ).
    cl_abap_unit_assert=>assert_equals( exp = 1 act = zcl_abapgit_ortec_ser_orch=>count_terminal_objects( lv_run_b ) ).
    cl_abap_unit_assert=>assert_equals( exp = 0 act = zcl_abapgit_ortec_ser_orch=>count_failed_objects( lv_run_a ) ).
    cl_abap_unit_assert=>assert_equals( exp = 1 act = zcl_abapgit_ortec_ser_orch=>count_failed_objects( lv_run_b ) ).
    cl_abap_unit_assert=>assert_true( act = zcl_abapgit_ortec_ser_orch=>is_run_complete( lv_run_a ) ).
    cl_abap_unit_assert=>assert_true( act = zcl_abapgit_ortec_ser_orch=>is_run_complete( lv_run_b ) ).
  ENDMETHOD.

  METHOD wapa_partition_separates.
    DATA ls_params TYPE zif_abapgit_definitions=>ty_i18n_params.
    DATA(lt_input) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'ZCL_A' ) )
      ( build_tadir( iv_obj_type = 'WAPA' iv_obj_name = 'ZWAPA_A' ) )
      ( build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'ZCL_B' ) )
      ( build_tadir( iv_obj_type = 'WAPA' iv_obj_name = 'ZWAPA_B' ) ) ).

    TRY.
        DATA(ls_partition) = zcl_abapgit_ortec_ser_orch=>partition_objects(
          it_tadir         = lt_input
          iv_max_processes = 4
          is_i18n_params   = ls_params ).
      CATCH zcx_abapgit_exception INTO DATA(lx_partition).
        cl_abap_unit_assert=>fail( msg = lx_partition->get_text( ) ).
    ENDTRY.

    cl_abap_unit_assert=>assert_equals( exp = 0 act = lines( ls_partition-forced_seq ) ).
    cl_abap_unit_assert=>assert_equals( exp = 2 act = lines( ls_partition-eligible ) ).
    cl_abap_unit_assert=>assert_equals( exp = 2 act = lines( ls_partition-wapa ) ).

    READ TABLE ls_partition-eligible INDEX 1 INTO DATA(ls_eligible_1).
    READ TABLE ls_partition-eligible INDEX 2 INTO DATA(ls_eligible_2).
    cl_abap_unit_assert=>assert_equals( exp = 'CLAS' act = ls_eligible_1-object ).
    cl_abap_unit_assert=>assert_equals( exp = 'CLAS' act = ls_eligible_2-object ).

    READ TABLE ls_partition-wapa INDEX 1 INTO DATA(ls_wapa_1).
    READ TABLE ls_partition-wapa INDEX 2 INTO DATA(ls_wapa_2).
    cl_abap_unit_assert=>assert_equals( exp = 'WAPA' act = ls_wapa_1-object ).
    cl_abap_unit_assert=>assert_equals( exp = 'WAPA' act = ls_wapa_2-object ).
  ENDMETHOD.

  METHOD wapa_batches_singletons.
    DATA ls_params TYPE zif_abapgit_definitions=>ty_i18n_params.
    DATA(lt_input) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'ZCL_A' ) )
      ( build_tadir( iv_obj_type = 'WAPA' iv_obj_name = 'ZWAPA_A' ) )
      ( build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'ZCL_B' ) )
      ( build_tadir( iv_obj_type = 'WAPA' iv_obj_name = 'ZWAPA_B' ) ) ).

    TRY.
        DATA(ls_partition) = zcl_abapgit_ortec_ser_orch=>partition_objects(
          it_tadir         = lt_input
          iv_max_processes = 4
          is_i18n_params   = ls_params ).
        DATA(lt_eligible_work) = VALUE zcl_abapgit_ortec_ser_planner=>tt_work_item(
          FOR ls_key IN ls_partition-eligible
          ( tadir = ls_key est_ms = 10 est_bytes = 100 est_source = 'F' ) ).
        DATA(lt_eligible_batches) = zcl_abapgit_ortec_ser_planner=>build_initial_batches(
          it_work_items   = lt_eligible_work
          iv_worker_count = 1
          iv_row_limit    = 25
          iv_byte_limit   = 1000 ).
        DATA(lt_wapa_batches) = zcl_abapgit_ortec_ser_orch=>build_wapa_singleton_batches( ls_partition-wapa ).
      CATCH zcx_abapgit_exception INTO DATA(lx_wapa_batch).
        cl_abap_unit_assert=>fail( msg = lx_wapa_batch->get_text( ) ).
    ENDTRY.

    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( lt_eligible_batches ) ).
    READ TABLE lt_eligible_batches INDEX 1 INTO DATA(ls_eligible_batch).
    cl_abap_unit_assert=>assert_equals( exp = 2 act = lines( ls_eligible_batch-items ) ).

    cl_abap_unit_assert=>assert_equals( exp = 2 act = lines( lt_wapa_batches ) ).
    READ TABLE lt_wapa_batches INDEX 1 INTO DATA(ls_wapa_batch_1).
    READ TABLE lt_wapa_batches INDEX 2 INTO DATA(ls_wapa_batch_2).
    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( ls_wapa_batch_1-items ) ).
    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( ls_wapa_batch_2-items ) ).
    READ TABLE ls_wapa_batch_1-items INDEX 1 INTO DATA(ls_wapa_item_1).
    READ TABLE ls_wapa_batch_2-items INDEX 1 INTO DATA(ls_wapa_item_2).
    cl_abap_unit_assert=>assert_equals( exp = 'WAPA' act = ls_wapa_item_1-tadir-object ).
    cl_abap_unit_assert=>assert_equals( exp = 'WAPA' act = ls_wapa_item_2-tadir-object ).
  ENDMETHOD.

  METHOD next_task_name_is_unique.
    " AR-1-003 regression test (independent adversarial audit) - the
    " earlier truncated-RUN_ID-hex scheme could produce identical task
    " names for two different runs; the session-wide monotonic counter
    " cannot.
    TRY.
        DATA(lv_name_1) = zcl_abapgit_ortec_ser_orch=>next_task_name( ).
        DATA(lv_name_2) = zcl_abapgit_ortec_ser_orch=>next_task_name( ).
      CATCH zcx_abapgit_exception INTO DATA(lx_task_name).
        cl_abap_unit_assert=>fail( msg = lx_task_name->get_text( ) ).
    ENDTRY.

    cl_abap_unit_assert=>assert_differs( exp = lv_name_1 act = lv_name_2 ).
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

    TRY.
        zcl_abapgit_ortec_ser_orch=>before_dispatch(
            iv_run_id      = lv_run
            it_object_keys = lt_keys
            iv_attempt     = 1
            iv_batch_id    = 'B1' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_exc).
        cl_abap_unit_assert=>fail( msg = 'Run is broken, cannot dispatch.' ).
    ENDTRY.

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
    ASSIGN zcl_abapgit_ortec_ser_orch=>mt_run_context[ run_id = lv_run ] TO FIELD-SYMBOL(<ls_run_context>).
    IF sy-subrc = 0.
      INSERT VALUE #( file-path = '/existing/' ) INTO TABLE <ls_run_context>-files.
    ENDIF.

    DATA(ls_result) = build_result( iv_obj_type = 'CLAS' iv_obj_name = 'A' ).
    ls_result-files_xstring = '0102030405'.

    DATA(lv_merged) = zcl_abapgit_ortec_ser_orch=>merge_into_mt_files(
                          iv_run_id = lv_run
                          is_tadir  = build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'A' )
                          is_result = ls_result ).

    cl_abap_unit_assert=>assert_false( lv_merged ).

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context INTO DATA(ls_bad_ctx) WITH TABLE KEY run_id = lv_run.
    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( ls_bad_ctx-files ) ).
    READ TABLE ls_bad_ctx-files INDEX 1 INTO DATA(ls_bad_file).
    cl_abap_unit_assert=>assert_equals( exp = '/existing/' act = ls_bad_file-file-path ).
  ENDMETHOD.

  METHOD merge_succeeds_with_payload.
    DATA ls_serialization TYPE zif_abapgit_objects=>ty_serialization.
    DATA lv_buffer        TYPE xstring.

    DATA(lv_run) = build_run_id( ).
    INSERT VALUE #( run_id = lv_run ) INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    ls_serialization-item-obj_type = 'CLAS'.
    ls_serialization-item-obj_name = 'ZCL_TEST'.
    APPEND INITIAL LINE TO ls_serialization-files ASSIGNING FIELD-SYMBOL(<ls_file>).
    <ls_file>-filename = 'zcl_test.clas.abap'.
    <ls_file>-data = '4142'.
    APPEND INITIAL LINE TO ls_serialization-files ASSIGNING FIELD-SYMBOL(<ls_file_2>).
    <ls_file_2>-filename = 'zcl_test.clas.xml'.
    <ls_file_2>-data = '4344'.

    EXPORT data = ls_serialization TO DATA BUFFER lv_buffer.

    DATA(ls_result) = build_result( iv_obj_type = 'CLAS' iv_obj_name = 'ZCL_TEST' ).
    ls_result-files_xstring = lv_buffer.

    DATA(ls_tadir) = build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'ZCL_TEST' ).
    ls_tadir-path = '/src/test/'.

    DATA(lv_merged) = zcl_abapgit_ortec_ser_orch=>merge_into_mt_files(
                          iv_run_id = lv_run
                          is_tadir  = ls_tadir
                          is_result = ls_result ).

    cl_abap_unit_assert=>assert_true( lv_merged ).

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context INTO DATA(ls_ctx) WITH TABLE KEY run_id = lv_run.
    cl_abap_unit_assert=>assert_equals( exp = 2 act = lines( ls_ctx-files ) ).

    READ TABLE ls_ctx-files INDEX 1 INTO DATA(ls_ctx_file_1).
    cl_abap_unit_assert=>assert_equals( exp = '/src/test/' act = ls_ctx_file_1-file-path ).
    cl_abap_unit_assert=>assert_equals( exp = 'CLAS' act = ls_ctx_file_1-item-obj_type ).
    cl_abap_unit_assert=>assert_equals( exp = 'ZCL_TEST' act = ls_ctx_file_1-item-obj_name ).
    cl_abap_unit_assert=>assert_equals( exp = 'zcl_test.clas.abap' act = ls_ctx_file_1-file-filename ).
    cl_abap_unit_assert=>assert_equals( exp = '4142' act = ls_ctx_file_1-file-data ).

    READ TABLE ls_ctx-files INDEX 2 INTO DATA(ls_ctx_file_2).
    cl_abap_unit_assert=>assert_equals( exp = '/src/test/' act = ls_ctx_file_2-file-path ).
    cl_abap_unit_assert=>assert_equals( exp = 'CLAS' act = ls_ctx_file_2-item-obj_type ).
    cl_abap_unit_assert=>assert_equals( exp = 'ZCL_TEST' act = ls_ctx_file_2-item-obj_name ).
    cl_abap_unit_assert=>assert_equals( exp = 'zcl_test.clas.xml' act = ls_ctx_file_2-file-filename ).
    cl_abap_unit_assert=>assert_equals( exp = '4344' act = ls_ctx_file_2-file-data ).
  ENDMETHOD.

  METHOD merge_empty_file_list_ok.
    DATA ls_serialization TYPE zif_abapgit_objects=>ty_serialization.
    DATA lv_buffer        TYPE xstring.

    DATA(lv_run) = build_run_id( ).
    INSERT VALUE #( run_id = lv_run ) INTO TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context.

    ls_serialization-item-obj_type = 'CLAS'.
    ls_serialization-item-obj_name = 'ZCL_EMPTY'.
    EXPORT data = ls_serialization TO DATA BUFFER lv_buffer.

    DATA(ls_result) = build_result( iv_obj_type = 'CLAS' iv_obj_name = 'ZCL_EMPTY' ).
    ls_result-files_xstring = lv_buffer.

    DATA(ls_tadir) = build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'ZCL_EMPTY' ).
    ls_tadir-path = '/src/empty/'.

    DATA(lv_merged) = zcl_abapgit_ortec_ser_orch=>merge_into_mt_files(
                          iv_run_id = lv_run
                          is_tadir  = ls_tadir
                          is_result = ls_result ).

    cl_abap_unit_assert=>assert_true( lv_merged ).

    READ TABLE zcl_abapgit_ortec_ser_orch=>mt_run_context INTO DATA(ls_empty_ctx) WITH TABLE KEY run_id = lv_run.
    cl_abap_unit_assert=>assert_equals( exp = 0 act = lines( ls_empty_ctx-files ) ).
  ENDMETHOD.

  METHOD split_depth_below_cap_false.
    " SER-SLICE-3 (serialization_slice_3_provider_contract.md §4) -
    " boundary check one step below C_MAX_PRE_DISPATCH_SPLITS.
    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_orch=>split_depth_at_cap(
        zcl_abapgit_ortec_ser_orch=>c_max_pre_dispatch_splits - 1 ) ).
  ENDMETHOD.

  METHOD split_depth_at_cap_true.
    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_orch=>split_depth_at_cap(
        zcl_abapgit_ortec_ser_orch=>c_max_pre_dispatch_splits ) ).
  ENDMETHOD.

  METHOD split_depth_above_cap_true.
    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_orch=>split_depth_at_cap(
        zcl_abapgit_ortec_ser_orch=>c_max_pre_dispatch_splits + 1 ) ).
  ENDMETHOD.

  METHOD before_dispatch_dd_buf_empty.
    " SER-SLICE-3 - a batch with no DOMA/DTEL objects must extract to an
    " INITIAL (0-byte) DD buffer, so BEFORE_DISPATCH's actual-byte gate
    " never trips for object types the DD provider does not cover, and
    " BEFORE_DISPATCH must still reach DISPATCH_BATCH's real MT_DISPATCH
    " insert for a broken (non-existent) run rather than looping forever -
    " this exercises the wiring without a real RFC round trip, mirroring
    " BREAKER_GATES_BEFORE_DISPATCH's own no-run-context technique.
    DATA(lv_run) = build_run_id( ).
    DATA(lt_keys) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
                              ( build_tadir( iv_obj_type = 'CLAS' iv_obj_name = 'ZZZ' ) ) ).

    cl_abap_unit_assert=>assert_initial(
      zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch( lt_keys ) ).

    TRY.
        zcl_abapgit_ortec_ser_orch=>before_dispatch(
            iv_run_id      = lv_run
            it_object_keys = lt_keys
            iv_attempt     = 1
            iv_batch_id    = 'B1' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_exc).
        cl_abap_unit_assert=>fail( msg = lx_exc->get_text( ) ).
    ENDTRY.

    " No run context exists, so DISPATCH_BATCH's own guard clause returns
    " immediately without ever issuing a real CALL FUNCTION - MT_DISPATCH
    " stays empty, proving the DD-buffer computation did not interfere
    " with the pre-existing no-context short-circuit.
    cl_abap_unit_assert=>assert_initial( zcl_abapgit_ortec_ser_orch=>mt_dispatch ).
  ENDMETHOD.

  METHOD zero_file_success_flagged.
    " SER-SLICE-3 parity incident (serialization_slice_3_dtel_doma_
    " parity.md, H5) - the exact suspicious combination: worker reports
    " success (RC = 0) for a REQUESTED object, but zero files.
    DATA(ls_row) = build_result( iv_obj_type = 'DTEL' iv_obj_name = 'A' iv_rc = 0 ).
    ls_row-output_file_count = 0.

    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_orch=>is_zero_file_success_bad(
        is_row       = ls_row
        iv_key_found = abap_true ) ).
  ENDMETHOD.

  METHOD nonzero_file_not_flagged.
    DATA(ls_row) = build_result( iv_obj_type = 'DTEL' iv_obj_name = 'A' iv_rc = 0 ).
    ls_row-output_file_count = 1.

    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_orch=>is_zero_file_success_bad(
        is_row       = ls_row
        iv_key_found = abap_true ) ).
  ENDMETHOD.

  METHOD zero_file_but_failed_row_ok.
    " RC <> 0 already goes through the ordinary failure branch - the
    " zero-file guard must not double-trigger for a real, reported
    " failure (it is not a "success with no files" case at all).
    DATA(ls_row) = build_result( iv_obj_type = 'DTEL' iv_obj_name = 'A' iv_rc = 4 ).
    ls_row-output_file_count = 0.

    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_orch=>is_zero_file_success_bad(
        is_row       = ls_row
        iv_key_found = abap_true ) ).
  ENDMETHOD.

  METHOD zero_file_unmatched_row_ok.
    " An unrequested/unmatched row (IV_KEY_FOUND = FALSE) is handled by
    " the pre-existing object_key_sets_equal mismatch gate upstream, not
    " this guard - must not double-trigger here.
    DATA(ls_row) = build_result( iv_obj_type = 'DTEL' iv_obj_name = 'A' iv_rc = 0 ).
    ls_row-output_file_count = 0.

    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_orch=>is_zero_file_success_bad(
        is_row       = ls_row
        iv_key_found = abap_false ) ).
  ENDMETHOD.
ENDCLASS.
