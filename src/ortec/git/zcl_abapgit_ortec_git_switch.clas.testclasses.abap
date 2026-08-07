CLASS ltcl_serial_batch_switch DEFINITION FOR TESTING
  RISK LEVEL HARMLESS
  DURATION SHORT FINAL.

  PRIVATE SECTION.
    CONSTANTS:
      c_url_a TYPE string VALUE 'https://test-git-switch-batch-a.example.com/repo.git',
      c_url_b TYPE string VALUE 'https://test-git-switch-batch-b.example.com/repo.git'.

    METHODS:
      teardown,
      roundtrip_via_switch FOR TESTING RAISING cx_static_check,
      default_off_unconfigured_repo FOR TESTING RAISING cx_static_check,
      reads_per_repo_not_global FOR TESTING RAISING cx_static_check,
      no_url_falls_back_to_test_seam FOR TESTING RAISING cx_static_check.

ENDCLASS.


CLASS ltcl_serial_batch_switch IMPLEMENTATION.

  METHOD teardown.
    " Restore the test-seam CLASS-DATA to its documented default so this
    " class does not leak state into other test classes sharing the
    " no-URL fallback path.
    zcl_abapgit_ortec_git_switch=>set_serial_batch_active( abap_false ).
  ENDMETHOD.

  METHOD roundtrip_via_switch.

    zcl_abapgit_ortec_git_switch=>set_repo_use_serial_batch(
      iv_url     = c_url_a
      iv_enabled = abap_true ).

    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_git_switch=>get_repo_use_serial_batch( c_url_a )
      exp = abap_true ).

  ENDMETHOD.

  METHOD default_off_unconfigured_repo.

    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_git_switch=>get_repo_use_serial_batch(
                'https://never-configured-switch.example.com/repo.git' )
      exp = abap_false ).

  ENDMETHOD.

  METHOD reads_per_repo_not_global.

    zcl_abapgit_ortec_git_switch=>set_repo_use_serial_batch(
      iv_url     = c_url_a
      iv_enabled = abap_true ).
    zcl_abapgit_ortec_git_switch=>set_repo_use_serial_batch(
      iv_url     = c_url_b
      iv_enabled = abap_false ).

    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_git_switch=>is_serial_batch_active( c_url_a )
      exp = abap_true
      msg = 'Repository A opted in must route to the adaptive batch path' ).

    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_git_switch=>is_serial_batch_active( c_url_b )
      exp = abap_false
      msg = 'Repository B did not opt in and must not be affected by repository A' ).

  ENDMETHOD.

  METHOD no_url_falls_back_to_test_seam.

    " Regression: existing callers that construct a controlled state
    " without a repository URL must keep working exactly as before this
    " change (Phase 7 will remove this fallback's remaining relevance).
    zcl_abapgit_ortec_git_switch=>set_serial_batch_active( abap_true ).

    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_git_switch=>is_serial_batch_active( )
      exp = abap_true ).

    zcl_abapgit_ortec_git_switch=>set_serial_batch_active( abap_false ).

    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_git_switch=>is_serial_batch_active( )
      exp = abap_false ).

  ENDMETHOD.

ENDCLASS.


CLASS ltcl_serial_prefetch_switch DEFINITION FOR TESTING
  RISK LEVEL HARMLESS
  DURATION SHORT FINAL.

  PRIVATE SECTION.
    METHODS:
      teardown,
      prefetch_default_off        FOR TESTING RAISING cx_static_check,
      wapa_default_off            FOR TESTING RAISING cx_static_check,
      wapa_delegates_to_prefetch  FOR TESTING RAISING cx_static_check.

ENDCLASS.


CLASS ltcl_serial_prefetch_switch IMPLEMENTATION.

  METHOD teardown.
    " SER-SLICE-3 Phase 7: OFF is the documented default - restore it so
    " this class does not leak state into other test classes sharing the
    " same session-global CLASS-DATA.
    zcl_abapgit_ortec_git_switch=>set_serial_prefetch_active( abap_false ).
  ENDMETHOD.

  METHOD prefetch_default_off.

    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_git_switch=>is_serial_prefetch_active( )
      exp = abap_false
      msg = 'Finding F-1 fix: classic per-object path must default to NOT consulting ORTEC caches' ).

  ENDMETHOD.

  METHOD wapa_default_off.

    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_git_switch=>is_wapa_active( )
      exp = abap_false
      msg = 'Finding F-2 fix: WAPA replacement must default off, not unconditionally on' ).

  ENDMETHOD.

  METHOD wapa_delegates_to_prefetch.

    " ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE cannot run standalone in a unit
    " test (needs live RFC batch dispatch + DB-backed result polling), so
    " this directly exercises the PREPARE/CLEAR-adjacent
    " set_serial_prefetch_active pairing logic that ORCH's own run window
    " relies on - proving IS_WAPA_ACTIVE and IS_SERIAL_PREFETCH_ACTIVE
    " share the exact same underlying flag.
    zcl_abapgit_ortec_git_switch=>set_serial_prefetch_active( abap_true ).

    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_git_switch=>is_serial_prefetch_active( )
      exp = abap_true ).
    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_git_switch=>is_wapa_active( )
      exp = abap_true ).

    zcl_abapgit_ortec_git_switch=>set_serial_prefetch_active( abap_false ).

    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_git_switch=>is_serial_prefetch_active( )
      exp = abap_false ).
    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_git_switch=>is_wapa_active( )
      exp = abap_false ).

  ENDMETHOD.

ENDCLASS.
