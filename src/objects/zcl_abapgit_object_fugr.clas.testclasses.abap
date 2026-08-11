CLASS zcl_abapgit_object_fugr DEFINITION LOCAL FRIENDS ltcl_changed_by.

CLASS ltcl_changed_by DEFINITION FINAL FOR TESTING
  DURATION SHORT
  RISK LEVEL HARMLESS.

  PRIVATE SECTION.

    METHODS:
      " needs_function_lookup( iv_extra ) - the exact guard added by SER-FINAL
      extra_initial_skips_lookup FOR TESTING RAISING cx_static_check,
      extra_filled_needs_lookup FOR TESTING RAISING cx_static_check,
      extra_namespaced_needs_lookup FOR TESTING RAISING cx_static_check,

      " most_recent_user( it_stamps ) - CHANGED_BY's own tie-break rule,
      " unchanged by SER-FINAL, extracted here so the final result
      " determination is independently regression-tested.
      no_stamps_returns_unknown FOR TESTING RAISING cx_static_check,
      single_stamp_wins FOR TESTING RAISING cx_static_check,
      latest_date_wins FOR TESTING RAISING cx_static_check,
      same_date_latest_time_wins FOR TESTING RAISING cx_static_check,
      unsorted_input_still_correct FOR TESTING RAISING cx_static_check.

ENDCLASS.


CLASS ltcl_changed_by IMPLEMENTATION.


  METHOD extra_initial_skips_lookup.

    cl_abap_unit_assert=>assert_equals(
      msg = |a whole-object CHANGED_BY request (empty IV_EXTRA) must not | &&
            |trigger the FUNCTIONS( ) lookup - function names are never empty|
      exp = abap_false
      act = zcl_abapgit_object_fugr=>needs_function_lookup( '' ) ).

  ENDMETHOD.


  METHOD extra_filled_needs_lookup.

    cl_abap_unit_assert=>assert_equals(
      msg = |a per-function/per-include CHANGED_BY request must still | &&
            |trigger the FUNCTIONS( ) lookup|
      exp = abap_true
      act = zcl_abapgit_object_fugr=>needs_function_lookup( 'ZFOO' ) ).

  ENDMETHOD.


  METHOD extra_namespaced_needs_lookup.

    " the guard is a pure emptiness check - it must not special-case or
    " break on namespaced identifiers
    cl_abap_unit_assert=>assert_equals(
      msg = |namespaced IV_EXTRA must behave exactly like a plain name|
      exp = abap_true
      act = zcl_abapgit_object_fugr=>needs_function_lookup( '/NAMESPACE/ZFOO' ) ).

  ENDMETHOD.


  METHOD no_stamps_returns_unknown.

    DATA lt_stamps TYPE zcl_abapgit_object_fugr=>ty_changed_by_stamp_tt.

    cl_abap_unit_assert=>assert_equals(
      exp = zcl_abapgit_objects_super=>c_user_unknown
      act = zcl_abapgit_object_fugr=>most_recent_user( lt_stamps ) ).

  ENDMETHOD.


  METHOD single_stamp_wins.

    DATA lt_stamps TYPE zcl_abapgit_object_fugr=>ty_changed_by_stamp_tt.

    APPEND VALUE #( user = 'JDOE' date = '20260101' time = '120000' ) TO lt_stamps.

    cl_abap_unit_assert=>assert_equals(
      exp = 'JDOE'
      act = zcl_abapgit_object_fugr=>most_recent_user( lt_stamps ) ).

  ENDMETHOD.


  METHOD latest_date_wins.

    DATA lt_stamps TYPE zcl_abapgit_object_fugr=>ty_changed_by_stamp_tt.

    APPEND VALUE #( user = 'OLDER'  date = '20260101' time = '120000' ) TO lt_stamps.
    APPEND VALUE #( user = 'NEWEST' date = '20260215' time = '080000' ) TO lt_stamps.
    APPEND VALUE #( user = 'MIDDLE' date = '20260201' time = '235959' ) TO lt_stamps.

    cl_abap_unit_assert=>assert_equals(
      exp = 'NEWEST'
      act = zcl_abapgit_object_fugr=>most_recent_user( lt_stamps ) ).

  ENDMETHOD.


  METHOD same_date_latest_time_wins.

    DATA lt_stamps TYPE zcl_abapgit_object_fugr=>ty_changed_by_stamp_tt.

    APPEND VALUE #( user = 'MORNING'   date = '20260101' time = '080000' ) TO lt_stamps.
    APPEND VALUE #( user = 'EVENING'   date = '20260101' time = '190000' ) TO lt_stamps.
    APPEND VALUE #( user = 'AFTERNOON' date = '20260101' time = '133000' ) TO lt_stamps.

    cl_abap_unit_assert=>assert_equals(
      exp = 'EVENING'
      act = zcl_abapgit_object_fugr=>most_recent_user( lt_stamps ) ).

  ENDMETHOD.


  METHOD unsorted_input_still_correct.

    " REPOSRC/REPOTEXT/EUDB rows are appended in arbitrary DB order -
    " the tie-break must not rely on caller-side pre-sorting
    DATA lt_stamps TYPE zcl_abapgit_object_fugr=>ty_changed_by_stamp_tt.

    APPEND VALUE #( user = 'MIDDLE' date = '20260201' time = '235959' ) TO lt_stamps.
    APPEND VALUE #( user = 'NEWEST' date = '20260215' time = '080000' ) TO lt_stamps.
    APPEND VALUE #( user = 'OLDER'  date = '20260101' time = '120000' ) TO lt_stamps.

    cl_abap_unit_assert=>assert_equals(
      exp = 'NEWEST'
      act = zcl_abapgit_object_fugr=>most_recent_user( lt_stamps ) ).

  ENDMETHOD.

ENDCLASS.
