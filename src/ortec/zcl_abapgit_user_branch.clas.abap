CLASS zcl_abapgit_user_branch DEFINITION
  PUBLIC FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    CONSTANTS:
      BEGIN OF cs_info,
        BEGIN OF settings,
          name  TYPE string VALUE 'use_user_branch',
          label TYPE string VALUE 'Use User-specific Branch',
          hint  TYPE string VALUE 'Work against user-specific branch, only valid for the current user',
        END OF settings,
      END OF cs_info.

    CLASS-METHODS set_user_branch_in_repo_list
      CHANGING ct_overview TYPE ANY TABLE
      RAISING  zcx_abapgit_exception.

    CLASS-METHODS is_user_branch_active
      RETURNING VALUE(rv_active) TYPE abap_bool.

    CLASS-METHODS select_branch
      IMPORTING iv_url    TYPE string
                iv_branch TYPE string
      RAISING   zcx_abapgit_exception.

    CLASS-METHODS get_selected_branch
      IMPORTING is_repo_data    TYPE zif_abapgit_persistence=>ty_repo
      RETURNING VALUE(r_branch) TYPE string.

    CLASS-METHODS get_user_branch
      IMPORTING iv_url           TYPE string
      RETURNING VALUE(rv_branch) TYPE string
      RAISING   zcx_abapgit_exception.

    CLASS-METHODS get_use_user_branch
      RETURNING VALUE(r_use_user_branch) TYPE abap_bool
      RAISING   zcx_abapgit_exception.

    CLASS-METHODS set_use_user_branch
      IMPORTING i_use_user_branch TYPE csequence
      RAISING   zcx_abapgit_exception.

    CLASS-METHODS get_obj_check_warning_header
      RETURNING VALUE(ro_header) TYPE REF TO cl_salv_form_layout_grid
      RAISING   zcx_abapgit_exception.

  PROTECTED SECTION.
    CLASS-DATA ms_settings TYPE zcl_abapgit_persistence_ortec=>ty_user_settings.

ENDCLASS.



CLASS ZCL_ABAPGIT_USER_BRANCH IMPLEMENTATION.


  METHOD is_user_branch_active.

    rv_active = abap_false.

    TRY.
        DATA(ls_settings) = zcl_abapgit_persistence_ortec=>get_instance( )->get_settings( ).
        rv_active = ls_settings-use_user_branch.
      CATCH cx_root.
        rv_active = abap_false.
    ENDTRY.

  ENDMETHOD.


  METHOD set_user_branch_in_repo_list.

    DATA lo_repo TYPE REF TO zcl_abapgit_repo_online.

    IF NOT is_user_branch_active( ).
      RETURN.
    ENDIF.

    DATA(lo_persistance) = zcl_abapgit_persistence_ortec=>get_instance( ).

    LOOP AT ct_overview ASSIGNING FIELD-SYMBOL(<fs_repo>).

      ASSIGN COMPONENT 'URL' OF STRUCTURE <fs_repo> TO FIELD-SYMBOL(<url>).
      IF sy-subrc <> 0.
        " Missing Field in Structure
        zcx_abapgit_exception=>raise(
            'Missing Field URL in Repo Data' ).
      ENDIF.
      ASSIGN COMPONENT 'BRANCH' OF STRUCTURE <fs_repo> TO FIELD-SYMBOL(<branch>).
      IF sy-subrc <> 0.
        " Missing Field in Structure
        zcx_abapgit_exception=>raise(
            'Missing Field BRANCH in Repo Data' ).
      ENDIF.
      ASSIGN COMPONENT 'KEY' OF STRUCTURE <fs_repo> TO FIELD-SYMBOL(<key>).
      IF sy-subrc <> 0.
        " Missing Field in Structure
        zcx_abapgit_exception=>raise(
            'Missing Field KEY in Repo Data' ).
      ENDIF.

      DATA(user_branch) = lo_persistance->get_repo_user_branch(
                              <url> ).
      IF user_branch IS NOT INITIAL.
        lo_repo ?= zcl_abapgit_repo_srv=>get_instance( )->get(
                       <key> ).
        lo_repo->select_branch(
            user_branch ).
        <branch> = user_branch.
        CLEAR user_branch.
      ENDIF.
    ENDLOOP.

  ENDMETHOD.


  METHOD get_user_branch.

    IF NOT is_user_branch_active( ).
      RETURN.
    ENDIF.

    rv_branch = zcl_abapgit_persistence_ortec=>get_instance( )->get_repo_user_branch(
                    iv_url ).

  ENDMETHOD.


  METHOD select_branch.

    IF NOT is_user_branch_active( ).
      RETURN.
    ENDIF.

    zcl_abapgit_persistence_ortec=>get_instance( )->set_repo_user_branch(
        iv_url    = iv_url
        iv_branch = iv_branch ).

  ENDMETHOD.


  METHOD get_use_user_branch.

    DATA(ms_settings) = zcl_abapgit_persistence_ortec=>get_instance( )->get_settings( ).

    r_use_user_branch = SWITCH #( ms_settings-use_user_branch
                                  WHEN abap_true
                                  THEN abap_true
                                  ELSE abap_false ).

  ENDMETHOD.


  METHOD set_use_user_branch.

    ms_settings-use_user_branch = SWITCH #( i_use_user_branch
                                            WHEN abap_true
                                            THEN abap_true
                                            ELSE abap_false ).

    zcl_abapgit_persistence_ortec=>get_instance( )->set_settings(
        ms_settings ).

  ENDMETHOD.


  METHOD get_obj_check_warning_header.

    IF NOT is_user_branch_active( ).
      RETURN.
    ENDIF.

    ro_header = NEW cl_salv_form_layout_grid( ).

    " Base Warning should reflectthe Text in ZCL_ABAPGIT_SERVICES_REPO->POPUP_OBJECTS_OVERWRITE
    ro_header->create_text(
        row    = 1
        column = 1
        text   = |The following objects are different between local and remote repository.| ).

    ro_header->create_text(
        row    = 2
        column = 1
        text   = |Select the objects which should be brought in line with the remote version.| ).

    " Extended Warning
    ro_header->create_text(
        row    = 3
        column = 1
        text   = || ).
    ro_header->create_header_information(
        row    = 4
        column = 1
        text   = |WARNING: Overwriting objects will affect ALL USERS in the system and is not branch specific!| ).

  ENDMETHOD.


  METHOD get_selected_branch.

    IF is_user_branch_active( ).
      r_branch = get_user_branch(
                     is_repo_data-url ).
      IF r_branch IS INITIAL.
        r_branch = is_repo_data-branch_name.
      ENDIF.
      RETURN.
    ENDIF.

    r_branch = is_repo_data-branch_name.

  ENDMETHOD.
ENDCLASS.
