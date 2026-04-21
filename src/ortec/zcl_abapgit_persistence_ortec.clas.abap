CLASS zcl_abapgit_persistence_ortec DEFINITION
  PUBLIC
  CREATE PRIVATE.

  PUBLIC SECTION.
    TYPES:
      BEGIN OF ty_user_settings,
        use_user_branch TYPE abap_bool,
      END OF ty_user_settings.

    CONSTANTS c_type_ortec TYPE zif_abapgit_persistence=>ty_type VALUE 'ORTEC' ##NO_TEXT.

    CLASS-METHODS get_instance
      IMPORTING iv_user        TYPE sy-uname DEFAULT sy-uname
      RETURNING VALUE(ri_user) TYPE REF TO zcl_abapgit_persistence_ortec
      RAISING   zcx_abapgit_exception.

    METHODS constructor
      IMPORTING iv_user TYPE sy-uname DEFAULT sy-uname
      RAISING   zcx_abapgit_exception.

    METHODS get_repo_user_branch
      IMPORTING iv_url           TYPE zif_abapgit_persistence=>ty_repo-url
      RETURNING VALUE(rv_branch) TYPE string
      RAISING   zcx_abapgit_exception.

    METHODS set_repo_user_branch
      IMPORTING iv_url    TYPE zif_abapgit_persistence=>ty_repo-url
                iv_branch TYPE string
      RAISING   zcx_abapgit_exception.

    METHODS get_settings
      RETURNING VALUE(rs_user_settings) TYPE ty_user_settings
      RAISING   zcx_abapgit_exception.

    METHODS set_settings
      IMPORTING is_user_settings TYPE ty_user_settings
      RAISING   zcx_abapgit_exception.

    METHODS get_db_explanation
      IMPORTING is_data        TYPE zif_abapgit_persistence=>ty_content
      CHANGING  cv_descr       TYPE string
                cs_explanation TYPE any.

  PROTECTED SECTION.

  PRIVATE SECTION.
    TYPES:
      BEGIN OF ty_repo_config,
        url         TYPE zif_abapgit_persistence=>ty_repo-url,
        user_branch TYPE string,
      END OF ty_repo_config.
    TYPES ty_repo_configs TYPE STANDARD TABLE OF ty_repo_config WITH DEFAULT KEY.
    TYPES:
      BEGIN OF ty_user,
        repo_config TYPE ty_repo_configs,
        settings    TYPE ty_user_settings,
      END OF ty_user.

    DATA mv_user TYPE sy-uname.
    DATA ms_user TYPE ty_user.

    CLASS-DATA gi_current_user TYPE REF TO zcl_abapgit_persistence_ortec.

    METHODS from_xml
      IMPORTING iv_xml         TYPE string
      RETURNING VALUE(rs_user) TYPE ty_user
      RAISING   zcx_abapgit_exception.

    METHODS read
      RAISING zcx_abapgit_exception.

    METHODS read_repo_config
      IMPORTING iv_url                TYPE zif_abapgit_persistence=>ty_repo-url
      RETURNING VALUE(rs_repo_config) TYPE ty_repo_config
      RAISING   zcx_abapgit_exception.

    METHODS to_xml
      IMPORTING is_user       TYPE ty_user
      RETURNING VALUE(rv_xml) TYPE string.

    METHODS update
      RAISING zcx_abapgit_exception.

    METHODS update_repo_config
      IMPORTING iv_url         TYPE zif_abapgit_persistence=>ty_repo-url
                is_repo_config TYPE ty_repo_config
      RAISING   zcx_abapgit_exception.

ENDCLASS.



CLASS ZCL_ABAPGIT_PERSISTENCE_ORTEC IMPLEMENTATION.


  METHOD constructor.
    mv_user = iv_user.
    read( ).
  ENDMETHOD.


  METHOD from_xml.

    DATA lv_xml TYPE string.

    lv_xml = iv_xml.

    " fix downward compatibility
    REPLACE ALL OCCURRENCES OF '<_--28C_TYPE_USER_--29>' IN lv_xml WITH '<USER>'.
    REPLACE ALL OCCURRENCES OF '</_--28C_TYPE_USER_--29>' IN lv_xml WITH '</USER>'.

    CALL TRANSFORMATION id
         OPTIONS value_handling = 'accept_data_loss'
         SOURCE XML lv_xml
         RESULT user = rs_user.

  ENDMETHOD.


  METHOD get_instance.

    IF iv_user = sy-uname ##USER_OK.
      IF gi_current_user IS NOT BOUND.
        gi_current_user = NEW zcl_abapgit_persistence_ortec( ).
      ENDIF.
      ri_user = gi_current_user.
    ELSE.
      ri_user = NEW zcl_abapgit_persistence_ortec( iv_user = iv_user ).
    ENDIF.

  ENDMETHOD.


  METHOD read.

    DATA lv_xml TYPE string.

    TRY.
        lv_xml = zcl_abapgit_persistence_db=>get_instance( )->read(
                     iv_type  = c_type_ortec
                     iv_value = mv_user ).
      CATCH zcx_abapgit_not_found.
        RETURN.
    ENDTRY.

    ms_user = from_xml(
                  lv_xml ).

  ENDMETHOD.


  METHOD read_repo_config.
    DATA lv_url TYPE string.

    lv_url = to_lower(
                 iv_url ).
    READ TABLE ms_user-repo_config INTO rs_repo_config WITH KEY url = lv_url.
  ENDMETHOD.


  METHOD to_xml.
    CALL TRANSFORMATION id
         SOURCE user = is_user
         RESULT XML rv_xml.
  ENDMETHOD.


  METHOD update.

    DATA lv_xml TYPE string.

    lv_xml = to_xml(
                 ms_user ).

    zcl_abapgit_persistence_db=>get_instance( )->modify(
        iv_type  = c_type_ortec
        iv_value = mv_user
        iv_data  = lv_xml ).

    COMMIT WORK AND WAIT.

  ENDMETHOD.


  METHOD update_repo_config.

    DATA lv_key TYPE string.

    FIELD-SYMBOLS <ls_repo_config> TYPE ty_repo_config.

    lv_key = to_lower(
                 iv_url ).

    READ TABLE ms_user-repo_config ASSIGNING <ls_repo_config> WITH KEY url = lv_key.
    IF sy-subrc IS NOT INITIAL.
      APPEND INITIAL LINE TO ms_user-repo_config ASSIGNING <ls_repo_config>.
    ENDIF.
    <ls_repo_config>     = is_repo_config.
    <ls_repo_config>-url = lv_key.

    update( ).

  ENDMETHOD.


  METHOD get_repo_user_branch.

    rv_branch = read_repo_config(
                    iv_url )-user_branch.

  ENDMETHOD.


  METHOD set_repo_user_branch.

    DATA ls_repo_config TYPE ty_repo_config.

    ls_repo_config = read_repo_config(
                         iv_url ).
    ls_repo_config-user_branch = iv_branch.
    update_repo_config(
        iv_url         = iv_url
        is_repo_config = ls_repo_config ).

  ENDMETHOD.


  METHOD get_settings.

    rs_user_settings = ms_user-settings.

  ENDMETHOD.


  METHOD set_settings.

    ms_user-settings = is_user_settings.
    update( ).

  ENDMETHOD.


  METHOD get_db_explanation.

    cv_descr = 'ORTEC User Settings'.

    ASSIGN COMPONENT 'VALUE' OF STRUCTURE cs_explanation
           TO FIELD-SYMBOL(<value>).
    IF sy-subrc = 0.
       <value> = zcl_abapgit_env_factory=>get_user_record( )->get_name( is_data-value ).
    ENDIF.

  ENDMETHOD.
ENDCLASS.
