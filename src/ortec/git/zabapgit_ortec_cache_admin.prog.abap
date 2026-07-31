REPORT zabapgit_ortec_cache_admin.

DATA gv_repo_key     TYPE c LENGTH 12.
DATA gt_repo_pending TYPE zcl_abapgit_ortec_cache_admin=>ty_repo_key_tt.

SELECTION-SCREEN BEGIN OF BLOCK b1 WITH FRAME TITLE TEXT-001.
  SELECT-OPTIONS s_repo FOR gv_repo_key LOWER CASE NO INTERVALS.
SELECTION-SCREEN END OF BLOCK b1.

SELECTION-SCREEN BEGIN OF BLOCK b2 WITH FRAME TITLE TEXT-002.
  PARAMETERS p_clear TYPE abap_bool AS CHECKBOX DEFAULT abap_false.
SELECTION-SCREEN END OF BLOCK b2.

AT SELECTION-SCREEN ON VALUE-REQUEST FOR s_repo-low.
  PERFORM repo_f4_help USING 'S_REPO-LOW'.

AT SELECTION-SCREEN ON VALUE-REQUEST FOR s_repo-high.
  PERFORM repo_f4_help USING 'S_REPO-HIGH'.

AT SELECTION-SCREEN OUTPUT.
  " Merge repository keys picked via the multi-select F4 popup here (a genuine
  " PBO event) instead of directly inside the value-request (POV) event.
  " A POV round trip only reliably repaints the field that triggered F4;
  " it does not guarantee the select-options table/indicator are redrawn on
  " that same round trip. AT SELECTION-SCREEN OUTPUT always runs immediately
  " before the screen is actually painted, so entries merged here are
  " guaranteed to be visible right away instead of only "on the next
  " GUI interaction".
  IF gt_repo_pending IS NOT INITIAL.
    LOOP AT gt_repo_pending INTO DATA(lv_pending_key).
      IF NOT line_exists( s_repo[ sign = 'I' option = 'EQ' low = lv_pending_key ] ).
        APPEND VALUE #( sign = 'I' option = 'EQ' low = lv_pending_key ) TO s_repo[].
      ENDIF.
    ENDLOOP.
    IF gt_repo_pending IS NOT INITIAL.
      CLEAR gt_repo_pending.
      LEAVE SCREEN.
    ENDIF.
    CLEAR gt_repo_pending.
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
    IF s_repo IS INITIAL.
      MESSAGE 'Enter a repository key to clear' TYPE 'E'.
    ENDIF.

    DATA lv_answer TYPE c LENGTH 1.
    CALL FUNCTION 'POPUP_TO_CONFIRM'
      EXPORTING
        titlebar       = 'Confirm cache clear'
        text_question  = |Clear all cached Git data for repository { s_repo-low }? This cannot be undone.|
        text_button_1  = 'Clear'
        text_button_2  = 'Cancel'
        default_button = '2'
      IMPORTING
        answer         = lv_answer
      EXCEPTIONS
        text_not_found = 1
        OTHERS         = 2.

    IF lv_answer = '1'.
      LOOP AT s_repo INTO DATA(ls_repo).
        TRY.

            DATA(ls_result) =
              zcl_abapgit_ortec_cache_admin=>clear_repo(
                iv_repo_key = ls_repo-low ).

            DATA(lv_message) =
              zcl_abapgit_ortec_cache_admin=>format_clear_result(
                ls_result ).

            MESSAGE lv_message TYPE 'S'.

          CATCH zcx_abapgit_ortec_git INTO DATA(lx_error).
            MESSAGE lx_error->get_text( ) TYPE 'E'.

        ENDTRY.
      ENDLOOP.

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

FORM repo_f4_help USING iv_dynprofield TYPE csequence.
  DATA lt_repo_values TYPE STANDARD TABLE OF zcl_abapgit_ortec_cache_admin=>ty_repo_f4 WITH EMPTY KEY.
  DATA lt_return      TYPE STANDARD TABLE OF ddshretval WITH EMPTY KEY.
  DATA lt_field_tab   TYPE STANDARD TABLE OF dfies.
  DATA ls_field       TYPE dfies.
  DATA lv_stepl       TYPE sy-stepl.

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

  CALL FUNCTION 'DYNP_GET_STEPL'
    IMPORTING
      povstepl = lv_stepl
    EXCEPTIONS
      OTHERS   = 0.

  CALL FUNCTION 'F4IF_INT_TABLE_VALUE_REQUEST'
    EXPORTING
      retfield        = 'REPO_KEY'
      dynpprog        = 'ZABAPGIT_ORTEC_CACHE_ADMIN' "sy-repid
      dynpnr          = '1000' "sy-dynnr
      dynprofield     = iv_dynprofield
      stepl           = lv_stepl
      value_org       = 'S'
      multiple_choice = 'X'
    TABLES
      value_tab       = lt_repo_values
      field_tab       = lt_field_tab
      return_tab      = lt_return
    EXCEPTIONS
      parameter_error = 1
      no_values_found = 2
      OTHERS          = 3.

  IF sy-subrc = 0.
    " Do not write s_repo here: this runs inside the value-request (POV)
    " event, whose round trip does not reliably repaint the select-options
    " table/indicator immediately. Stage the picked keys and let
    " AT SELECTION-SCREEN OUTPUT (a real PBO event) merge them into s_repo
    " right before the screen is actually painted.
    CLEAR gt_repo_pending.
    LOOP AT lt_return INTO DATA(ls_return).
      IF ls_return-fieldval IS NOT INITIAL.
        IF s_repo IS INITIAL AND sy-tabix = 1.
          s_repo = VALUE #( sign   = 'I'
                            option = 'EQ'
                            low    = ls_return-fieldval ).
        ENDIF.
        IF NOT line_exists( s_repo[ sign = 'I' option = 'EQ' low = ls_return-fieldval ] ).
          APPEND VALUE #( sign = 'I' option = 'EQ' low = ls_return-fieldval ) TO s_repo[].
        ENDIF.
        APPEND ls_return-fieldval TO gt_repo_pending.
      ENDIF.
    ENDLOOP.
  ENDIF.
ENDFORM.
