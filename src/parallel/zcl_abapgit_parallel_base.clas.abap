class ZCL_ABAPGIT_PARALLEL_BASE definition
  public
  abstract
  create public .

public section.

  class-methods GET_FREE_WORK_PROCESSES
    importing
      !I_RESERVED type I default 1
    returning
      value(R_FREE) type I .
protected section.

  class-data MO_PROGRESS type ref to ZCL_ABAPGIT_PROGRESS .

  class-methods GET_MAX_MODES
    returning
      value(R_MAX_MODES) type I .
private section.
ENDCLASS.



CLASS ZCL_ABAPGIT_PARALLEL_BASE IMPLEMENTATION.


  METHOD GET_FREE_WORK_PROCESSES.

    DATA: lt_wpinfo TYPE STANDARD TABLE OF wpinfo
            WITH DEFAULT KEY.

    r_free = 0.

    "Get Work process Info
    CALL FUNCTION 'TH_WPINFO'
      TABLES
        wplist = lt_wpinfo
      EXCEPTIONS
        OTHERS = 0.

    "Get Number of unused Dialog Work Processes
    LOOP AT lt_wpinfo TRANSPORTING NO FIELDS
      WHERE wp_typ     = 'DIA' "Dialog
        AND wp_istatus = 2.    "Waiting
      ADD 1 TO r_free.
    ENDLOOP.

    "Leave some WPs for other users
    r_free = COND #( WHEN r_free <= i_reserved THEN 0
                     ELSE r_free - i_reserved ).

*    "Limit to max. no. of allowed WPs
**    r_free = COND #( WHEN r_free > get_max_modes( ) THEN get_max_modes( )
**                        ELSE r_free ).

  ENDMETHOD.


  METHOD GET_MAX_MODES.

    DATA: cvalue TYPE pfepvalue.

    CALL FUNCTION 'RSAN_SYSTEM_PARAMETER_READ'
      EXPORTING
        i_name     = 'rdisp/max_alt_modes'
      IMPORTING
        e_value    = cvalue
      EXCEPTIONS
        read_error = 1
        OTHERS     = 2.
    IF sy-subrc <> 0.
      "Return System Default
      r_max_modes = 6.
    ELSE.
      r_max_modes = cvalue.
    ENDIF.

  ENDMETHOD.
ENDCLASS.
