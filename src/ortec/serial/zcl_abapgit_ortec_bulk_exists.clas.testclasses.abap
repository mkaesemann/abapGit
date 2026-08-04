CLASS ltcl_bulk_exists DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  " IT8 finding (2026-08-04): CL_OSQL_TEST_ENVIRONMENT test-doubling DD01L/
  " DD04L/SEOCLASSDF short-dumps with RAISE_EXCEPTION/NOT_FOUND inside
  " CL_ABAP_STRUCTDESCR=>GET_DDIC_FIELD_LIST, reproducibly, on every run -
  " these are DDIC CATALOG tables (domains/data elements/class defs) the
  " ABAP runtime itself relies on to resolve types for the WHOLE session,
  " including to load the test class itself; they must never be doubled.
  " Fixtures below use REAL, stable objects (SAP Basis MANDT domain/data
  " element; existing abapGit CLAS/INTF) for exists/absent checks instead
  " of fabricated DDIC catalog rows. Only VSEOEXTEND/SPROXHDR (plain
  " application content tables, not DDIC-catalog/RTTI-critical) are
  " doubled, to pin the SADL/proxy EXCLUSION join against a real,
  " definitely-existing class/interface without needing to fake its
  " existence.

  PRIVATE SECTION.
    CONSTANTS c_sadl_refclsname TYPE vseoextend-refclsname VALUE 'CL_SADL_GTK_EXPOSURE_MPC'.

    CLASS-DATA gi_environment TYPE REF TO if_osql_test_environment.

    CLASS-METHODS class_setup.
    CLASS-METHODS class_teardown.

    METHODS setup.

    METHODS build_tadir
      IMPORTING iv_object      TYPE tadir-object
                iv_obj_name    TYPE tadir-obj_name
      RETURNING VALUE(rs_tadir) TYPE zif_abapgit_definitions=>ty_tadir.

    " T-1: existing CLAS/INTF/DTEL/DOMA objects are classified as existing.
    " Real, stable objects only - MANDT (SAP Basis domain/data element)
    " and existing abapGit CLAS/INTF - never fabricated DDIC catalog rows.
    METHODS t1_exists_doma FOR TESTING RAISING cx_static_check.
    METHODS t1_exists_dtel FOR TESTING RAISING cx_static_check.
    METHODS t1_exists_clas FOR TESTING RAISING cx_static_check.
    METHODS t1_exists_intf FOR TESTING RAISING cx_static_check.
    METHODS t1_order_preserved FOR TESTING RAISING cx_static_check.

    " T-2: a deleted/never-existed name is excluded for every prototype type.
    METHODS t2_absent_doma FOR TESTING RAISING cx_static_check.
    METHODS t2_absent_dtel FOR TESTING RAISING cx_static_check.
    METHODS t2_absent_clas FOR TESTING RAISING cx_static_check.
    METHODS t2_absent_intf FOR TESTING RAISING cx_static_check.

    " T-3: CHDO/SADL/proxy-generated exclusion (CLAS/INTF branches). The
    " base object is REAL (so the "exists" side of the check is genuine,
    " not fabricated); only the join table that TRIGGERS the exclusion
    " (VSEOEXTEND/SPROXHDR) is doubled - both are plain application
    " content tables, not DDIC catalog tables. A control test proves the
    " same real object is normally included when that join row is absent.
    METHODS t3_clas_sadl_generated_excl FOR TESTING RAISING cx_static_check.
    METHODS t3_clas_sadl_control_incl FOR TESTING RAISING cx_static_check.
    METHODS t3_intf_proxy_generated_excl FOR TESTING RAISING cx_static_check.
    METHODS t3_intf_proxy_control_incl FOR TESTING RAISING cx_static_check.

ENDCLASS.


CLASS ltcl_bulk_exists IMPLEMENTATION.

  METHOD class_setup.
    DATA lt_tables TYPE if_osql_test_environment=>ty_t_sobjnames.
    APPEND 'VSEOEXTEND' TO lt_tables.
    APPEND 'SPROXHDR' TO lt_tables.
    gi_environment = cl_osql_test_environment=>create( lt_tables ).
  ENDMETHOD.

  METHOD class_teardown.
    gi_environment->destroy( ).
  ENDMETHOD.

  METHOD setup.
    gi_environment->clear_doubles( ).
  ENDMETHOD.

  METHOD build_tadir.
    rs_tadir-object   = iv_object.
    rs_tadir-obj_name = iv_obj_name.
  ENDMETHOD.

  METHOD t1_exists_doma.
    " MANDT: universal SAP Basis domain, guaranteed present in any system.
    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( build_tadir( iv_object = 'DOMA' iv_obj_name = 'MANDT' ) ) ).

    DATA(lt_result) = zcl_abapgit_ortec_bulk_exists=>filter_existing( lt_tadir ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_result ) exp = 1 ).
    cl_abap_unit_assert=>assert_equals( act = lt_result[ 1 ]-obj_name exp = 'MANDT' ).
  ENDMETHOD.

  METHOD t1_exists_dtel.
    " MANDT: universal SAP Basis data element, guaranteed present in any system.
    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( build_tadir( iv_object = 'DTEL' iv_obj_name = 'MANDT' ) ) ).

    DATA(lt_result) = zcl_abapgit_ortec_bulk_exists=>filter_existing( lt_tadir ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_result ) exp = 1 ).
    cl_abap_unit_assert=>assert_equals( act = lt_result[ 1 ]-obj_name exp = 'MANDT' ).
  ENDMETHOD.

  METHOD t1_exists_clas.
    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( build_tadir( iv_object = 'CLAS' iv_obj_name = 'ZCL_ABAPGIT_ORTEC_BULK_EXISTS' ) ) ).

    DATA(lt_result) = zcl_abapgit_ortec_bulk_exists=>filter_existing( lt_tadir ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_result ) exp = 1 ).
    cl_abap_unit_assert=>assert_equals( act = lt_result[ 1 ]-obj_name exp = 'ZCL_ABAPGIT_ORTEC_BULK_EXISTS' ).
  ENDMETHOD.

  METHOD t1_exists_intf.
    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( build_tadir( iv_object = 'INTF' iv_obj_name = 'ZIF_ABAPGIT_DEFINITIONS' ) ) ).

    DATA(lt_result) = zcl_abapgit_ortec_bulk_exists=>filter_existing( lt_tadir ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_result ) exp = 1 ).
    cl_abap_unit_assert=>assert_equals( act = lt_result[ 1 ]-obj_name exp = 'ZIF_ABAPGIT_DEFINITIONS' ).
  ENDMETHOD.

  METHOD t1_order_preserved.
    " One representative package-like mix of all four prototype types in
    " one call, all real objects - the original TADIR input order must be
    " preserved in the filtered result (T-1).
    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( build_tadir( iv_object = 'CLAS' iv_obj_name = 'ZCL_ABAPGIT_ORTEC_BULK_EXISTS' ) )
      ( build_tadir( iv_object = 'DOMA' iv_obj_name = 'MANDT' ) )
      ( build_tadir( iv_object = 'INTF' iv_obj_name = 'ZIF_ABAPGIT_DEFINITIONS' ) )
      ( build_tadir( iv_object = 'DTEL' iv_obj_name = 'MANDT' ) ) ).

    DATA(lt_result) = zcl_abapgit_ortec_bulk_exists=>filter_existing( lt_tadir ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_result ) exp = 4 ).
    cl_abap_unit_assert=>assert_equals( act = lt_result[ 1 ]-obj_name exp = 'ZCL_ABAPGIT_ORTEC_BULK_EXISTS' ).
    cl_abap_unit_assert=>assert_equals( act = lt_result[ 2 ]-obj_name exp = 'MANDT' ).
    cl_abap_unit_assert=>assert_equals( act = lt_result[ 2 ]-object exp = 'DOMA' ).
    cl_abap_unit_assert=>assert_equals( act = lt_result[ 3 ]-obj_name exp = 'ZIF_ABAPGIT_DEFINITIONS' ).
    cl_abap_unit_assert=>assert_equals( act = lt_result[ 4 ]-obj_name exp = 'MANDT' ).
    cl_abap_unit_assert=>assert_equals( act = lt_result[ 4 ]-object exp = 'DTEL' ).
  ENDMETHOD.

  METHOD t2_absent_doma.
    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( build_tadir( iv_object = 'DOMA' iv_obj_name = 'ZZZZ_BEX_NOT_A_REAL_DOM' ) ) ).

    DATA(lt_result) = zcl_abapgit_ortec_bulk_exists=>filter_existing( lt_tadir ).

    cl_abap_unit_assert=>assert_initial( lt_result ).
  ENDMETHOD.

  METHOD t2_absent_dtel.
    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( build_tadir( iv_object = 'DTEL' iv_obj_name = 'ZZZZ_BEX_NOT_A_REAL_DTL' ) ) ).

    DATA(lt_result) = zcl_abapgit_ortec_bulk_exists=>filter_existing( lt_tadir ).

    cl_abap_unit_assert=>assert_initial( lt_result ).
  ENDMETHOD.

  METHOD t2_absent_clas.
    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( build_tadir( iv_object = 'CLAS' iv_obj_name = 'ZCL_BEX_NOT_A_REAL_CLASX' ) ) ).

    DATA(lt_result) = zcl_abapgit_ortec_bulk_exists=>filter_existing( lt_tadir ).

    cl_abap_unit_assert=>assert_initial( lt_result ).
  ENDMETHOD.

  METHOD t2_absent_intf.
    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( build_tadir( iv_object = 'INTF' iv_obj_name = 'ZIF_BEX_NOT_A_REAL_INTFX' ) ) ).

    DATA(lt_result) = zcl_abapgit_ortec_bulk_exists=>filter_existing( lt_tadir ).

    cl_abap_unit_assert=>assert_initial( lt_result ).
  ENDMETHOD.

  METHOD t3_clas_sadl_generated_excl.
    " ZCL_ABAPGIT_ORTEC_WAPA genuinely exists (real, undoubled SEOCLASSDF);
    " only the VSEOEXTEND join row is fabricated to trigger the SADL
    " exclusion - must be excluded despite "existing" (mirrors standard
    " CLAS~EXISTS).
    DATA lt_vseoextend TYPE STANDARD TABLE OF vseoextend WITH DEFAULT KEY.
    APPEND VALUE #( clsname = 'ZCL_ABAPGIT_ORTEC_WAPA' refclsname = c_sadl_refclsname ) TO lt_vseoextend.
    gi_environment->insert_test_data( lt_vseoextend ).

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( build_tadir( iv_object = 'CLAS' iv_obj_name = 'ZCL_ABAPGIT_ORTEC_WAPA' ) ) ).

    DATA(lt_result) = zcl_abapgit_ortec_bulk_exists=>filter_existing( lt_tadir ).

    cl_abap_unit_assert=>assert_initial( lt_result ).
  ENDMETHOD.

  METHOD t3_clas_sadl_control_incl.
    " Same real class, no VSEOEXTEND join row (setup cleared all doubles) -
    " proves the exclusion in t3_clas_sadl_generated_excl is genuinely
    " conditional on the join, not a false negative in the base lookup.
    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( build_tadir( iv_object = 'CLAS' iv_obj_name = 'ZCL_ABAPGIT_ORTEC_WAPA' ) ) ).

    DATA(lt_result) = zcl_abapgit_ortec_bulk_exists=>filter_existing( lt_tadir ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_result ) exp = 1 ).
    cl_abap_unit_assert=>assert_equals( act = lt_result[ 1 ]-obj_name exp = 'ZCL_ABAPGIT_ORTEC_WAPA' ).
  ENDMETHOD.

  METHOD t3_intf_proxy_generated_excl.
    " ZIF_ABAPGIT_TADIR genuinely exists (real, undoubled SEOCLASSDF); only
    " the SPROXHDR join row is fabricated to trigger the proxy exclusion -
    " must be excluded despite "existing" (mirrors standard INTF~EXISTS).
    DATA lt_sproxhdr TYPE STANDARD TABLE OF sproxhdr WITH DEFAULT KEY.
    APPEND VALUE #( object = 'INTF' obj_name = 'ZIF_ABAPGIT_TADIR' ) TO lt_sproxhdr.
    gi_environment->insert_test_data( lt_sproxhdr ).

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( build_tadir( iv_object = 'INTF' iv_obj_name = 'ZIF_ABAPGIT_TADIR' ) ) ).

    DATA(lt_result) = zcl_abapgit_ortec_bulk_exists=>filter_existing( lt_tadir ).

    cl_abap_unit_assert=>assert_initial( lt_result ).
  ENDMETHOD.

  METHOD t3_intf_proxy_control_incl.
    " Same real interface, no SPROXHDR join row (setup cleared all
    " doubles) - proves the exclusion above is genuinely conditional.
    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( build_tadir( iv_object = 'INTF' iv_obj_name = 'ZIF_ABAPGIT_TADIR' ) ) ).

    DATA(lt_result) = zcl_abapgit_ortec_bulk_exists=>filter_existing( lt_tadir ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_result ) exp = 1 ).
    cl_abap_unit_assert=>assert_equals( act = lt_result[ 1 ]-obj_name exp = 'ZIF_ABAPGIT_TADIR' ).
  ENDMETHOD.

ENDCLASS.
