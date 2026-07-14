REPORT zabapgit_ortec_cache_admin.

SELECTION-SCREEN BEGIN OF BLOCK b1 WITH FRAME TITLE TEXT-001.
PARAMETERS p_repo TYPE zcl_abapgit_ortec_repo_state=>ty_repo_key.
SELECTION-SCREEN END OF BLOCK b1.

SELECTION-SCREEN BEGIN OF BLOCK b2 WITH FRAME TITLE TEXT-002.
PARAMETERS p_clear TYPE abap_bool AS CHECKBOX DEFAULT abap_false.
SELECTION-SCREEN END OF BLOCK b2.

AT SELECTION-SCREEN ON VALUE-REQUEST FOR p_repo.
  DATA lt_repo_values TYPE STANDARD TABLE OF zcl_abapgit_ortec_cache_admin=>ty_repo_f4 WITH EMPTY KEY.
  DATA lt_return      TYPE STANDARD TABLE OF ddshretval WITH EMPTY KEY.
  DATA lt_field_tab   TYPE STANDARD TABLE OF dfies.
  DATA ls_field       TYPE dfies.

  lt_repo_values = zcl_abapgit_ortec_cache_admin=>get_repo_f4_values( ).

  IF lt_repo_values IS INITIAL.
    MESSAGE 'No cached repositories found (ZAOG_REPO_STATE)' TYPE 'S'.
    RETURN.
  ENDIF.

  CLEAR ls_field.
  ls_field-fieldname = 'REPO_KEY'.
  ls_field-datatype  = 'C'.
  ls_field-intlen    = 24.
  ls_field-outputlen = 12.
  ls_field-inttype   = 'C'.
  ls_field-position  = 1.
  ls_field-offset    = 0.
  APPEND ls_field TO lt_field_tab.

  CLEAR ls_field.
  ls_field-fieldname = 'BRANCH_NAME'.
  ls_field-datatype  = 'C'.
  ls_field-intlen    = 510.
  ls_field-outputlen = 255.
  ls_field-inttype   = 'C'.
  ls_field-position  = 2.
  ls_field-offset    = 24.
  APPEND ls_field TO lt_field_tab.

  CLEAR ls_field.
  ls_field-fieldname = 'REMOTE_URL'.
  ls_field-datatype  = 'C'.
  ls_field-intlen    = 510.
  ls_field-outputlen = 255.
  ls_field-inttype   = 'C'.
  ls_field-position  = 3.
  ls_field-offset    = 534.
  APPEND ls_field TO lt_field_tab.

  CALL FUNCTION 'F4IF_INT_TABLE_VALUE_REQUEST'
    EXPORTING
      retfield    = 'REPO_KEY'
      dynpprog    = sy-repid
      dynpnr      = sy-dynnr
      dynprofield = 'P_REPO'
      value_org   = 'S'
    TABLES
      value_tab   = lt_repo_values
      field_tab   = lt_field_tab
      return_tab  = lt_return
    EXCEPTIONS
      parameter_error = 1
      no_values_found = 2
      OTHERS          = 3.

  IF sy-subrc = 0.
    READ TABLE lt_return INDEX 1 ASSIGNING FIELD-SYMBOL(<ls_return>).
    IF sy-subrc = 0 AND <ls_return>-fieldval IS NOT INITIAL.
      p_repo = <ls_return>-fieldval.
    ENDIF.
  ENDIF.

START-OF-SELECTION.

  AUTHORITY-CHECK OBJECT 'S_DEVELOP'
    ID 'DEVCLASS' DUMMY
    ID 'OBJTYPE'  FIELD 'DEBUG'
    ID 'OBJNAME'  DUMMY
    ID 'P_GROUP'  DUMMY
    ID 'ACTVT'    FIELD '02'.
  IF sy-subrc <> 0.
    MESSAGE 'No authorization to use the ORTEC cache admin report' TYPE 'E'.
  ENDIF.

  IF p_clear = abap_true.
    IF p_repo IS INITIAL.
      MESSAGE 'Enter a repository key to clear' TYPE 'E'.
    ENDIF.

    DATA lv_answer TYPE c LENGTH 1.
    CALL FUNCTION 'POPUP_TO_CONFIRM'
      EXPORTING
        titlebar       = 'Confirm cache clear'
        text_question  = |Clear all cached Git data for repository { p_repo }? This cannot be undone.|
        text_button_1  = 'Clear'
        text_button_2  = 'Cancel'
        default_button = '2'
      IMPORTING
        answer         = lv_answer
      EXCEPTIONS
        text_not_found = 1
        OTHERS         = 2.

    IF lv_answer = '1'.
      TRY.
          DATA(lv_message) = zcl_abapgit_ortec_cache_admin=>clear_repo( p_repo ).
          MESSAGE lv_message TYPE 'S'.
        CATCH zcx_abapgit_ortec_git INTO DATA(lx_error).
          MESSAGE lx_error->get_text( ) TYPE 'E'.
      ENDTRY.
    ELSE.
      MESSAGE 'Cache clear cancelled' TYPE 'S'.
    ENDIF.
  ENDIF.

  DATA(lt_overview) = zcl_abapgit_ortec_cache_admin=>get_overview( ).

  IF lt_overview IS INITIAL.
    MESSAGE 'No ORTEC cache data found' TYPE 'S'.
    RETURN.
  ENDIF.

  TRY.
      cl_salv_table=>factory(
        IMPORTING
          r_salv_table = DATA(lo_alv)
        CHANGING
          t_table      = lt_overview ).
      lo_alv->get_functions( )->set_all( abap_true ).
      lo_alv->display( ).
    CATCH cx_salv_msg INTO DATA(lx_salv).
      MESSAGE lx_salv->get_text( ) TYPE 'E'.
  ENDTRY.
