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
