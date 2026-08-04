CLASS ltcl_ser_planner DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  PRIVATE SECTION.
    METHODS build_item
      IMPORTING iv_name      TYPE sobj_name
                iv_ms        TYPE i
                iv_bytes     TYPE i DEFAULT 1000
      RETURNING VALUE(rs_item) TYPE zcl_abapgit_ortec_ser_planner=>ty_work_item.

    METHODS empty_input_yields_none FOR TESTING.
    METHODS fewer_items_than_workers FOR TESTING.
    METHODS lpt_balances_across_workers FOR TESTING.
    METHODS row_limit_forces_new_batch FOR TESTING.
    METHODS byte_limit_forces_new_batch FOR TESTING.
    METHODS tie_break_uses_original_order FOR TESTING.

    METHODS refill_zero_when_nothing_left FOR TESTING.
    METHODS refill_applies_shrink_factor FOR TESTING.
    METHODS refill_capped_by_row_limit FOR TESTING.
    METHODS refill_never_below_one FOR TESTING.

ENDCLASS.


CLASS ltcl_ser_planner IMPLEMENTATION.

  METHOD build_item.
    rs_item-tadir-object   = 'CLAS'.
    rs_item-tadir-obj_name = iv_name.
    rs_item-est_ms         = iv_ms.
    rs_item-est_bytes      = iv_bytes.
    rs_item-est_source     = zcl_abapgit_ortec_ser_cost=>c_source_family.
  ENDMETHOD.

  METHOD empty_input_yields_none.
    DATA(lt_batches) = zcl_abapgit_ortec_ser_planner=>build_initial_batches(
      it_work_items   = VALUE #( )
      iv_worker_count = 3
      iv_row_limit    = 100
      iv_byte_limit   = 1000000 ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_batches ) exp = 0 ).
  ENDMETHOD.

  METHOD fewer_items_than_workers.
    DATA(lt_items) = VALUE zcl_abapgit_ortec_ser_planner=>tt_work_item(
      ( build_item( iv_name = 'A' iv_ms = 10 ) )
      ( build_item( iv_name = 'B' iv_ms = 20 ) ) ).

    DATA(lt_batches) = zcl_abapgit_ortec_ser_planner=>build_initial_batches(
      it_work_items   = lt_items
      iv_worker_count = 3
      iv_row_limit    = 100
      iv_byte_limit   = 1000000 ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_batches ) exp = 2 ).
    LOOP AT lt_batches INTO DATA(ls_batch).
      cl_abap_unit_assert=>assert_equals( act = lines( ls_batch-items ) exp = 1 ).
    ENDLOOP.
  ENDMETHOD.

  METHOD lpt_balances_across_workers.
    DATA(lt_items) = VALUE zcl_abapgit_ortec_ser_planner=>tt_work_item(
      ( build_item( iv_name = 'A' iv_ms = 5 ) )
      ( build_item( iv_name = 'B' iv_ms = 5 ) )
      ( build_item( iv_name = 'C' iv_ms = 4 ) )
      ( build_item( iv_name = 'D' iv_ms = 4 ) )
      ( build_item( iv_name = 'E' iv_ms = 3 ) )
      ( build_item( iv_name = 'F' iv_ms = 3 ) ) ).

    DATA(lt_batches) = zcl_abapgit_ortec_ser_planner=>build_initial_batches(
      it_work_items   = lt_items
      iv_worker_count = 3
      iv_row_limit    = 100
      iv_byte_limit   = 1000000 ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_batches ) exp = 3 ).
    LOOP AT lt_batches INTO DATA(ls_batch).
      cl_abap_unit_assert=>assert_equals( act = ls_batch-total_est_ms exp = 8 ).
      cl_abap_unit_assert=>assert_equals( act = lines( ls_batch-items ) exp = 2 ).
    ENDLOOP.
  ENDMETHOD.

  METHOD row_limit_forces_new_batch.
    DATA(lt_items) = VALUE zcl_abapgit_ortec_ser_planner=>tt_work_item(
      ( build_item( iv_name = 'A' iv_ms = 10 ) )
      ( build_item( iv_name = 'B' iv_ms = 10 ) )
      ( build_item( iv_name = 'C' iv_ms = 10 ) ) ).

    DATA(lt_batches) = zcl_abapgit_ortec_ser_planner=>build_initial_batches(
      it_work_items   = lt_items
      iv_worker_count = 1
      iv_row_limit    = 1
      iv_byte_limit   = 1000000 ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_batches ) exp = 3 ).
    LOOP AT lt_batches INTO DATA(ls_batch).
      cl_abap_unit_assert=>assert_equals( act = lines( ls_batch-items ) exp = 1 ).
    ENDLOOP.
  ENDMETHOD.

  METHOD byte_limit_forces_new_batch.
    DATA(lt_items) = VALUE zcl_abapgit_ortec_ser_planner=>tt_work_item(
      ( build_item( iv_name = 'A' iv_ms = 10 iv_bytes = 600 ) )
      ( build_item( iv_name = 'B' iv_ms = 10 iv_bytes = 600 ) ) ).

    DATA(lt_batches) = zcl_abapgit_ortec_ser_planner=>build_initial_batches(
      it_work_items   = lt_items
      iv_worker_count = 1
      iv_row_limit    = 100
      iv_byte_limit   = 1000 ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_batches ) exp = 2 ).
    LOOP AT lt_batches INTO DATA(ls_batch).
      cl_abap_unit_assert=>assert_equals( act = lines( ls_batch-items ) exp = 1 ).
    ENDLOOP.
  ENDMETHOD.

  METHOD tie_break_uses_original_order.
    " all equal est_ms - with a single worker, items must be appended in
    " original TADIR order (deterministic, not hash/sort-order dependent)
    DATA(lt_items) = VALUE zcl_abapgit_ortec_ser_planner=>tt_work_item(
      ( build_item( iv_name = 'FIRST'  iv_ms = 7 ) )
      ( build_item( iv_name = 'SECOND' iv_ms = 7 ) )
      ( build_item( iv_name = 'THIRD'  iv_ms = 7 ) ) ).

    DATA(lt_batches) = zcl_abapgit_ortec_ser_planner=>build_initial_batches(
      it_work_items   = lt_items
      iv_worker_count = 1
      iv_row_limit    = 100
      iv_byte_limit   = 1000000 ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_batches ) exp = 1 ).
    READ TABLE lt_batches INDEX 1 INTO DATA(ls_batch).
    cl_abap_unit_assert=>assert_equals( act = lines( ls_batch-items ) exp = 3 ).
    cl_abap_unit_assert=>assert_equals( act = ls_batch-items[ 1 ]-tadir-obj_name exp = 'FIRST' ).
    cl_abap_unit_assert=>assert_equals( act = ls_batch-items[ 2 ]-tadir-obj_name exp = 'SECOND' ).
    cl_abap_unit_assert=>assert_equals( act = ls_batch-items[ 3 ]-tadir-obj_name exp = 'THIRD' ).
  ENDMETHOD.

  METHOD refill_zero_when_nothing_left.
    DATA(lv_size) = zcl_abapgit_ortec_ser_planner=>compute_refill_size(
      iv_remaining_items = 0
      iv_worker_count    = 3
      iv_row_limit        = 100 ).

    cl_abap_unit_assert=>assert_equals( act = lv_size exp = 0 ).
  ENDMETHOD.

  METHOD refill_applies_shrink_factor.
    " c_shrink_factor = 2, denom = worker_count * 2 = 4, ceil(10/4) = 3
    DATA(lv_size) = zcl_abapgit_ortec_ser_planner=>compute_refill_size(
      iv_remaining_items = 10
      iv_worker_count    = 2
      iv_row_limit        = 100 ).

    cl_abap_unit_assert=>assert_equals( act = lv_size exp = 3 ).
  ENDMETHOD.

  METHOD refill_capped_by_row_limit.
    " denom = 1 * 2 = 2, ceil(100/2) = 50, capped to row_limit = 10
    DATA(lv_size) = zcl_abapgit_ortec_ser_planner=>compute_refill_size(
      iv_remaining_items = 100
      iv_worker_count    = 1
      iv_row_limit        = 10 ).

    cl_abap_unit_assert=>assert_equals( act = lv_size exp = 10 ).
  ENDMETHOD.

  METHOD refill_never_below_one.
    " denom = 5 * 2 = 10, ceil(1/10) = 1
    DATA(lv_size) = zcl_abapgit_ortec_ser_planner=>compute_refill_size(
      iv_remaining_items = 1
      iv_worker_count    = 5
      iv_row_limit        = 100 ).

    cl_abap_unit_assert=>assert_equals( act = lv_size exp = 1 ).
  ENDMETHOD.

ENDCLASS.
