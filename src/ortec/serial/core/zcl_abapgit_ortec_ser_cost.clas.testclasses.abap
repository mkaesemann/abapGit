CLASS ltcl_ser_cost DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  PRIVATE SECTION.
    METHODS default_family_for_clas FOR TESTING.
    METHODS default_family_for_intf FOR TESTING.
    METHODS default_family_for_ddic FOR TESTING.
    METHODS default_family_for_generic FOR TESTING.
    METHODS exact_sample_wins_over_default FOR TESTING.
    METHODS update_creates_first_sample FOR TESTING.
    METHODS update_blends_existing_sample FOR TESTING.
    METHODS update_only_touches_own_type FOR TESTING.

ENDCLASS.


CLASS ltcl_ser_cost IMPLEMENTATION.

  METHOD default_family_for_clas.
    DATA(ls_estimate) = zcl_abapgit_ortec_ser_cost=>get_estimate(
      iv_obj_type = 'CLAS'
      it_ewma     = VALUE #( ) ).

    cl_abap_unit_assert=>assert_equals( act = ls_estimate-est_ms exp = zcl_abapgit_ortec_ser_cost=>c_default_ms_oo ).
    cl_abap_unit_assert=>assert_equals( act = ls_estimate-est_bytes exp = zcl_abapgit_ortec_ser_cost=>c_default_bytes_oo ).
    cl_abap_unit_assert=>assert_equals( act = ls_estimate-est_source exp = zcl_abapgit_ortec_ser_cost=>c_source_family ).
  ENDMETHOD.

  METHOD default_family_for_intf.
    DATA(ls_estimate) = zcl_abapgit_ortec_ser_cost=>get_estimate(
      iv_obj_type = 'INTF'
      it_ewma     = VALUE #( ) ).

    cl_abap_unit_assert=>assert_equals( act = ls_estimate-est_ms exp = zcl_abapgit_ortec_ser_cost=>c_default_ms_oo ).
    cl_abap_unit_assert=>assert_equals( act = ls_estimate-est_bytes exp = zcl_abapgit_ortec_ser_cost=>c_default_bytes_oo ).
  ENDMETHOD.

  METHOD default_family_for_ddic.
    DATA(ls_dtel) = zcl_abapgit_ortec_ser_cost=>get_estimate( iv_obj_type = 'DTEL' it_ewma = VALUE #( ) ).
    DATA(ls_doma) = zcl_abapgit_ortec_ser_cost=>get_estimate( iv_obj_type = 'DOMA' it_ewma = VALUE #( ) ).

    cl_abap_unit_assert=>assert_equals( act = ls_dtel-est_ms exp = zcl_abapgit_ortec_ser_cost=>c_default_ms_ddic ).
    cl_abap_unit_assert=>assert_equals( act = ls_dtel-est_bytes exp = zcl_abapgit_ortec_ser_cost=>c_default_bytes_ddic ).
    cl_abap_unit_assert=>assert_equals( act = ls_doma-est_ms exp = zcl_abapgit_ortec_ser_cost=>c_default_ms_ddic ).
    cl_abap_unit_assert=>assert_equals( act = ls_doma-est_bytes exp = zcl_abapgit_ortec_ser_cost=>c_default_bytes_ddic ).
  ENDMETHOD.

  METHOD default_family_for_generic.
    DATA(ls_estimate) = zcl_abapgit_ortec_ser_cost=>get_estimate(
      iv_obj_type = 'WAPA'
      it_ewma     = VALUE #( ) ).

    cl_abap_unit_assert=>assert_equals( act = ls_estimate-est_ms exp = zcl_abapgit_ortec_ser_cost=>c_default_ms_generic ).
    cl_abap_unit_assert=>assert_equals( act = ls_estimate-est_bytes exp = zcl_abapgit_ortec_ser_cost=>c_default_bytes_generic ).
    cl_abap_unit_assert=>assert_equals( act = ls_estimate-est_source exp = zcl_abapgit_ortec_ser_cost=>c_source_family ).
  ENDMETHOD.

  METHOD exact_sample_wins_over_default.
    DATA(lt_ewma) = VALUE zcl_abapgit_ortec_ser_cost=>ty_ewma_tt(
      ( obj_type = 'CLAS' est_ms = 999 est_bytes = 12345 ) ).

    DATA(ls_estimate) = zcl_abapgit_ortec_ser_cost=>get_estimate(
      iv_obj_type = 'CLAS'
      it_ewma     = lt_ewma ).

    cl_abap_unit_assert=>assert_equals( act = ls_estimate-est_ms exp = 999 ).
    cl_abap_unit_assert=>assert_equals( act = ls_estimate-est_bytes exp = 12345 ).
    cl_abap_unit_assert=>assert_equals( act = ls_estimate-est_source exp = zcl_abapgit_ortec_ser_cost=>c_source_exact ).
  ENDMETHOD.

  METHOD update_creates_first_sample.
    DATA lt_ewma TYPE zcl_abapgit_ortec_ser_cost=>ty_ewma_tt.

    zcl_abapgit_ortec_ser_cost=>update_estimate(
      EXPORTING
        iv_obj_type     = 'CLAS'
        iv_actual_ms    = 42
        iv_actual_bytes = 4200
      CHANGING
        ct_ewma         = lt_ewma ).

    READ TABLE lt_ewma WITH TABLE KEY obj_type = 'CLAS' INTO DATA(ls_ewma).
    cl_abap_unit_assert=>assert_subrc( exp = 0 ).
    cl_abap_unit_assert=>assert_equals( act = ls_ewma-est_ms exp = 42 ).
    cl_abap_unit_assert=>assert_equals( act = ls_ewma-est_bytes exp = 4200 ).
  ENDMETHOD.

  METHOD update_blends_existing_sample.
    DATA lt_ewma TYPE zcl_abapgit_ortec_ser_cost=>ty_ewma_tt.
    lt_ewma = VALUE #( ( obj_type = 'CLAS' est_ms = 100 est_bytes = 10000 ) ).

    " alpha = 0.30: new = 0.3*actual + 0.7*old
    zcl_abapgit_ortec_ser_cost=>update_estimate(
      EXPORTING
        iv_obj_type     = 'CLAS'
        iv_actual_ms    = 200
        iv_actual_bytes = 20000
      CHANGING
        ct_ewma         = lt_ewma ).

    READ TABLE lt_ewma WITH TABLE KEY obj_type = 'CLAS' INTO DATA(ls_ewma).
    cl_abap_unit_assert=>assert_subrc( exp = 0 ).
    cl_abap_unit_assert=>assert_equals( act = ls_ewma-est_ms exp = 130 ).
    cl_abap_unit_assert=>assert_equals( act = ls_ewma-est_bytes exp = 13000 ).
  ENDMETHOD.

  METHOD update_only_touches_own_type.
    DATA lt_ewma TYPE zcl_abapgit_ortec_ser_cost=>ty_ewma_tt.
    lt_ewma = VALUE #( ( obj_type = 'INTF' est_ms = 77 est_bytes = 7700 ) ).

    zcl_abapgit_ortec_ser_cost=>update_estimate(
      EXPORTING
        iv_obj_type     = 'CLAS'
        iv_actual_ms    = 42
        iv_actual_bytes = 4200
      CHANGING
        ct_ewma         = lt_ewma ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_ewma ) exp = 2 ).
    READ TABLE lt_ewma WITH TABLE KEY obj_type = 'INTF' INTO DATA(ls_intf).
    cl_abap_unit_assert=>assert_subrc( exp = 0 ).
    cl_abap_unit_assert=>assert_equals( act = ls_intf-est_ms exp = 77 ).
  ENDMETHOD.

ENDCLASS.
