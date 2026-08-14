CLASS ltcl_user DEFINITION
  FOR TESTING
  RISK LEVEL CRITICAL
  DURATION SHORT FINAL.

  PRIVATE SECTION.
    CONSTANTS:
      c_abap_user TYPE sy-uname VALUE 'ABAPGIT_TEST',
      c_git_user  TYPE string VALUE 'abapgit_tester',
      c_repo_url  TYPE string VALUE 'https://github.com/abapGit/abapGit'.

    DATA:
      mi_user TYPE REF TO zcl_abapgit_persistence_ortec.

    METHODS:
      set_get_settings   FOR TESTING RAISING zcx_abapgit_exception,
      teardown RAISING zcx_abapgit_exception.

ENDCLASS.

CLASS ltcl_user IMPLEMENTATION.

  METHOD set_get_settings.

    DATA: ls_settings TYPE zcl_abapgit_persistence_ortec=>ty_user_settings.

    ls_settings-use_user_branch = abap_true.

    mi_user = zcl_abapgit_persistence_ortec=>get_instance( c_abap_user ).
    mi_user->set_settings( ls_settings ).

    FREE mi_user.

    mi_user = zcl_abapgit_persistence_ortec=>get_instance( c_abap_user ).
    ls_settings = mi_user->get_settings( ).

    cl_abap_unit_assert=>assert_equals(
      act = ls_settings-use_user_branch
      exp = abap_true ).

  ENDMETHOD.

  METHOD teardown.
    " Delete test user settings
    zcl_abapgit_persistence_db=>get_instance( )->delete(
      iv_type  = zcl_abapgit_persistence_db=>c_type_user
      iv_value = c_abap_user ).
    CALL FUNCTION 'DB_COMMIT'.
  ENDMETHOD.
ENDCLASS.

CLASS ltcl_serial_batch_setting DEFINITION
  FOR TESTING
  RISK LEVEL HARMLESS
  DURATION SHORT FINAL.

  PRIVATE SECTION.
    CONSTANTS:
      c_abap_user TYPE sy-uname VALUE 'ABAPGIT_TEST',
      c_url_a       TYPE string VALUE 'https://test-serial-batch-a.example.com/repo.git',
      c_url_b       TYPE string VALUE 'https://test-serial-batch-b.example.com/repo.git',
      c_cache_url_a TYPE string VALUE 'https://test-cache-a.example.com/repo.git',
      c_cache_url_b TYPE string VALUE 'https://test-cache-b.example.com/repo.git'.

    METHODS:
      roundtrip_set_then_get FOR TESTING RAISING zcx_abapgit_exception,
      default_off_unknown_url FOR TESTING RAISING zcx_abapgit_exception,
      repo_a_on_repo_b_off_isolated FOR TESTING RAISING zcx_abapgit_exception,
      cache_roundtrip_set_get FOR TESTING RAISING zcx_abapgit_exception,
      cache_default_off FOR TESTING RAISING zcx_abapgit_exception,
      cache_repo_isolated FOR TESTING RAISING zcx_abapgit_exception,
      serial_stats_roundtrip FOR TESTING RAISING zcx_abapgit_exception,
      teardown RAISING zcx_abapgit_exception.

ENDCLASS.

CLASS ltcl_serial_batch_setting IMPLEMENTATION.

  METHOD roundtrip_set_then_get.

    DATA mi_user TYPE REF TO zcl_abapgit_persistence_ortec.

    mi_user = zcl_abapgit_persistence_ortec=>get_instance( c_abap_user ).
    mi_user->set_repo_use_serial_batch(
      iv_url              = c_url_a
      iv_use_serial_batch = abap_true ).

    FREE mi_user.
    mi_user = zcl_abapgit_persistence_ortec=>get_instance( c_abap_user ).

    cl_abap_unit_assert=>assert_equals(
      act = mi_user->get_repo_use_serial_batch( c_url_a )
      exp = abap_true ).

  ENDMETHOD.

  METHOD default_off_unknown_url.

    DATA mi_user TYPE REF TO zcl_abapgit_persistence_ortec.

    mi_user = zcl_abapgit_persistence_ortec=>get_instance( c_abap_user ).

    cl_abap_unit_assert=>assert_equals(
      act = mi_user->get_repo_use_serial_batch( 'https://never-configured.example.com/repo.git' )
      exp = abap_false ).

  ENDMETHOD.

  METHOD repo_a_on_repo_b_off_isolated.

    DATA mi_user TYPE REF TO zcl_abapgit_persistence_ortec.

    mi_user = zcl_abapgit_persistence_ortec=>get_instance( c_abap_user ).
    mi_user->set_repo_use_serial_batch(
      iv_url              = c_url_a
      iv_use_serial_batch = abap_true ).
    mi_user->set_repo_use_serial_batch(
      iv_url              = c_url_b
      iv_use_serial_batch = abap_false ).

    FREE mi_user.
    mi_user = zcl_abapgit_persistence_ortec=>get_instance( c_abap_user ).

    cl_abap_unit_assert=>assert_equals(
      act = mi_user->get_repo_use_serial_batch( c_url_a )
      exp = abap_true ).
    cl_abap_unit_assert=>assert_equals(
      act = mi_user->get_repo_use_serial_batch( c_url_b )
      exp = abap_false ).

  ENDMETHOD.

  METHOD cache_roundtrip_set_get.

    DATA mi_user TYPE REF TO zcl_abapgit_persistence_ortec.

    mi_user = zcl_abapgit_persistence_ortec=>get_instance( c_abap_user ).
    mi_user->set_repo_use_cache(
      iv_url       = c_cache_url_a
      iv_use_cache = abap_true ).

    FREE mi_user.
    mi_user = zcl_abapgit_persistence_ortec=>get_instance( c_abap_user ).

    cl_abap_unit_assert=>assert_equals(
      act = mi_user->get_repo_use_cache( c_cache_url_a )
      exp = abap_true ).

  ENDMETHOD.


  METHOD cache_default_off.

    DATA mi_user TYPE REF TO zcl_abapgit_persistence_ortec.

    mi_user = zcl_abapgit_persistence_ortec=>get_instance( c_abap_user ).

    cl_abap_unit_assert=>assert_equals(
      act = mi_user->get_repo_use_cache( 'https://never-configured-cache.example.com/repo.git' )
      exp = abap_false ).

  ENDMETHOD.


  METHOD cache_repo_isolated.

    DATA mi_user TYPE REF TO zcl_abapgit_persistence_ortec.

    mi_user = zcl_abapgit_persistence_ortec=>get_instance( c_abap_user ).
    mi_user->set_repo_use_cache(
      iv_url       = c_cache_url_a
      iv_use_cache = abap_true ).
    mi_user->set_repo_use_cache(
      iv_url       = c_cache_url_b
      iv_use_cache = abap_false ).

    FREE mi_user.
    mi_user = zcl_abapgit_persistence_ortec=>get_instance( c_abap_user ).

    cl_abap_unit_assert=>assert_equals(
      act = mi_user->get_repo_use_cache( c_cache_url_a )
      exp = abap_true ).
    cl_abap_unit_assert=>assert_equals(
      act = mi_user->get_repo_use_cache( c_cache_url_b )
      exp = abap_false ).

  ENDMETHOD.


  METHOD serial_stats_roundtrip.

    DATA mi_user TYPE REF TO zcl_abapgit_persistence_ortec.

    mi_user = zcl_abapgit_persistence_ortec=>get_instance( c_abap_user ).
    mi_user->set_repo_use_serial_stats(
      iv_url               = c_url_a
      iv_use_serial_stats  = abap_true ).
    mi_user->set_repo_use_serial_stats(
      iv_url               = c_url_b
      iv_use_serial_stats  = abap_false ).

    FREE mi_user.
    mi_user = zcl_abapgit_persistence_ortec=>get_instance( c_abap_user ).

    cl_abap_unit_assert=>assert_equals(
      act = mi_user->get_repo_use_serial_stats( c_url_a )
      exp = abap_true ).
    cl_abap_unit_assert=>assert_equals(
      act = mi_user->get_repo_use_serial_stats( c_url_b )
      exp = abap_false ).
    cl_abap_unit_assert=>assert_equals(
      act = mi_user->get_repo_use_serial_stats( 'https://never-configured-stats.example.com/repo.git' )
      exp = abap_false ).

  ENDMETHOD.

  METHOD teardown.
    " Delete test user settings (correct type, unlike the sibling
    " ltcl_user teardown above which deletes the wrong DB type).
    zcl_abapgit_persistence_db=>get_instance( )->delete(
      iv_type  = zcl_abapgit_persistence_ortec=>c_type_ortec
      iv_value = c_abap_user ).
    CALL FUNCTION 'DB_COMMIT'.
  ENDMETHOD.

ENDCLASS.
