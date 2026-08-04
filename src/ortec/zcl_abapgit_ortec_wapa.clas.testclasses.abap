CLASS ltcl_wapa DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  " SER-SLICE-0 scope note: only T-WAPA-1 (serialization_wapa_review.md)
  " is implemented here. T-WAPA-2..5 all require calling
  " ZCL_ABAPGIT_ORTEC_WAPA=>serialize(), which unconditionally dispatches
  " to the concrete, non-injectable standard classes
  " CL_O2_API_APPLICATION=>LOAD/CL_O2_API_PAGES=>GET_ALL_PAGES/
  " GET_MASTER_LANGUAGE (hardcoded static calls, no interface or seam in
  " ZCL_ABAPGIT_ORTEC_WAPA to substitute a test double), and page content
  " is read via `IMPORT ... FROM DATABASE O2PAGCON(TR)` - O2PAGCON is a
  " pool/cluster table (confirmed by this class's own source comment),
  " which CL_OSQL_TEST_ENVIRONMENT cannot double (it only intercepts Open
  " SQL SELECT/INSERT/UPDATE/DELETE/MODIFY, not IMPORT/EXPORT FROM/TO
  " DATABASE cluster access). Exercising T-WAPA-2..5 would therefore
  " require either real, repository-specific BSP application/page content
  " already present in the target system (explicitly forbidden - "no test
  " depends on repository-specific unstable data") or a production
  " architecture change to add an injection seam (explicitly forbidden in
  " this slice). Reported as BLOCKED_MISSING_PRODUCTION_SEAM, not
  " silently skipped - see serialization_slice_0_regression.md.
  "
  " EXISTS() itself has no such dependency: it is a single, self-contained
  " SELECT SINGLE against the transparent table O2APPL, with no call into
  " CL_O2_API_APPLICATION/CL_O2_API_PAGES at all - fully testable.

  PRIVATE SECTION.
    CLASS-DATA gi_environment TYPE REF TO if_osql_test_environment.

    CLASS-METHODS class_setup.
    CLASS-METHODS class_teardown.

    METHODS setup.

    METHODS t_wapa_1_active_only FOR TESTING RAISING cx_static_check.
    METHODS t_wapa_1_inactive_only FOR TESTING RAISING cx_static_check.
    METHODS t_wapa_1_neither FOR TESTING RAISING cx_static_check.

ENDCLASS.


CLASS ltcl_wapa IMPLEMENTATION.

  METHOD class_setup.
    DATA lt_tables TYPE if_osql_test_environment=>ty_t_sobjnames.
    APPEND 'O2APPL' TO lt_tables.
    gi_environment = cl_osql_test_environment=>create( lt_tables ).
  ENDMETHOD.

  METHOD class_teardown.
    gi_environment->destroy( ).
  ENDMETHOD.

  METHOD setup.
    gi_environment->clear_doubles( ).
  ENDMETHOD.

  METHOD t_wapa_1_active_only.
    DATA lt_o2appl TYPE STANDARD TABLE OF o2appl WITH DEFAULT KEY.
    APPEND VALUE #( applname = 'ZTST_BEX_WAPA1' version = 'A' ) TO lt_o2appl.
    gi_environment->insert_test_data( lt_o2appl ).

    DATA(lv_exists) = zcl_abapgit_ortec_wapa=>exists( 'ZTST_BEX_WAPA1' ).

    cl_abap_unit_assert=>assert_true( lv_exists ).
  ENDMETHOD.

  METHOD t_wapa_1_inactive_only.
    DATA lt_o2appl TYPE STANDARD TABLE OF o2appl WITH DEFAULT KEY.
    APPEND VALUE #( applname = 'ZTST_BEX_WAPA2' version = 'I' ) TO lt_o2appl.
    gi_environment->insert_test_data( lt_o2appl ).

    DATA(lv_exists) = zcl_abapgit_ortec_wapa=>exists( 'ZTST_BEX_WAPA2' ).

    cl_abap_unit_assert=>assert_true( lv_exists ).
  ENDMETHOD.

  METHOD t_wapa_1_neither.
    " No O2APPL row inserted at all for this name.
    DATA(lv_exists) = zcl_abapgit_ortec_wapa=>exists( 'ZTST_BEX_WAPA3' ).

    cl_abap_unit_assert=>assert_false( lv_exists ).
  ENDMETHOD.

ENDCLASS.
