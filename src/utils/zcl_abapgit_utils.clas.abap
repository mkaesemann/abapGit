class ZCL_ABAPGIT_UTILS definition
  public
  final
  create public .

public section.

  class-methods IS_BINARY
    importing
      !IV_DATA type XSTRING
    returning
      value(RV_YES) type ABAP_BOOL .
  class-methods EXTRACT_AUTHOR_DATA
    importing
      !IV_AUTHOR type STRING
    exporting
      !EV_AUTHOR type ZIF_ABAPGIT_DEFINITIONS=>TY_COMMIT-AUTHOR
      !EV_EMAIL type ZIF_ABAPGIT_DEFINITIONS=>TY_COMMIT-EMAIL
      !EV_TIME type ZIF_ABAPGIT_DEFINITIONS=>TY_COMMIT-TIME
    raising
      ZCX_ABAPGIT_EXCEPTION .
  class-methods TRANSLATE_POSTDATA
    importing
      !IT_POSTDATA type CNHT_POST_DATA_TAB
    returning
      value(R_STRING) type STRING .
  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.



CLASS ZCL_ABAPGIT_UTILS IMPLEMENTATION.


  METHOD extract_author_data.

    " unix time stamps are in same time zone, so ignore the zone
    FIND REGEX zif_abapgit_definitions=>c_author_regex IN iv_author
      SUBMATCHES
      ev_author
      ev_email
      ev_time ##NO_TEXT.

    IF sy-subrc <> 0.
      zcx_abapgit_exception=>raise( |Error author regex value='{ iv_author }'| ).
    ENDIF.

  ENDMETHOD.


  METHOD is_binary.

    DATA: lv_len TYPE i,
          lv_idx TYPE i,
          lv_x   TYPE x.

    lv_len = xstrlen( iv_data ).
    IF lv_len = 0.
      RETURN.
    ENDIF.

    IF lv_len > 100.
      lv_len = 100.
    ENDIF.

    " Simple char range test
    " stackoverflow.com/questions/277521/how-to-identify-the-file-content-as-ascii-or-binary
    DO lv_len TIMES. " I'm sure there is more efficient way ...
      lv_idx = sy-index - 1.
      lv_x = iv_data+lv_idx(1).

      IF NOT ( lv_x BETWEEN 9 AND 13 OR lv_x BETWEEN 32 AND 126 ).
        rv_yes = abap_true.
        EXIT.
      ENDIF.
    ENDDO.

  ENDMETHOD.


  METHOD translate_postdata.

    DATA: lt_post_data       TYPE cnht_post_data_tab,
          ls_last_line       TYPE cnht_post_data_line,
          lv_last_line_index TYPE i.

    IF it_postdata IS INITIAL.
      "Nothing to do
      RETURN.
    ENDIF.

    lt_post_data = it_postdata.

    "Save the last line for separate merge, because we don't need its trailing spaces
    WHILE ls_last_line IS INITIAL.
      lv_last_line_index = lines( lt_post_data ).
      READ TABLE lt_post_data INTO ls_last_line INDEX lv_last_line_index.
      DELETE lt_post_data INDEX lv_last_line_index.
    ENDWHILE.

    CONCATENATE LINES OF lt_post_data INTO r_string
      IN CHARACTER MODE RESPECTING BLANKS.
    CONCATENATE r_string ls_last_line INTO r_string
      IN CHARACTER MODE.

  ENDMETHOD.
ENDCLASS.
