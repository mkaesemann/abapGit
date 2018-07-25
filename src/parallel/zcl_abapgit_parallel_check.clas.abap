class ZCL_ABAPGIT_PARALLEL_CHECK definition
  public
  inheriting from ZCL_ABAPGIT_PARALLEL_BASE
  final
  create private .

public section.

  class-methods CHECK_EXISTS
    importing
      !IT_TADIR type ZIF_ABAPGIT_DEFINITIONS=>TY_TADIR_TT
      !IO_LOG type ref to ZCL_ABAPGIT_LOG optional
    returning
      value(RT_TADIR) type ZIF_ABAPGIT_DEFINITIONS=>TY_TADIR_TT
    raising
      ZCX_ABAPGIT_EXCEPTION .
  class-methods CHECK_FINISHED
    importing
      !P_TASK type CLIKE .
protected section.

  types:
    BEGIN OF ty_check_block,
      task     TYPE string,
      started  TYPE abap_bool,
      done     TYPE abap_bool,
      block    TYPE zpp_abapgit_t_tadir,
      existing TYPE zpp_abapgit_t_tadir,
      errors   TYPE zpp_abapgit_t_serial_error,
    END OF ty_check_block .
  types:
    tty_check_block TYPE SORTED TABLE OF ty_check_block
           WITH UNIQUE KEY task
           WITH NON-UNIQUE SORTED KEY status COMPONENTS started done .

  class-data MT_CHECK_BLOCKS type TTY_CHECK_BLOCK .
  class-data:
    BEGIN OF ms_block_info,
      started  TYPE i,
      finished TYPE i,
      total    TYPE i,
    END OF ms_block_info .

  class-methods PROCESS_CHECK_BLOCK
    importing
      !P_TASK type CLIKE
      !IT_OBJECTS type ZPP_ABAPGIT_T_TADIR
      !IT_ERRORS type ZPP_ABAPGIT_T_SERIAL_ERROR optional .
private section.
ENDCLASS.



CLASS ZCL_ABAPGIT_PARALLEL_CHECK IMPLEMENTATION.


  METHOD check_exists.

    DATA: lt_files_info  TYPE zpp_abapgit_t_file_item.
    DATA: st_check_block TYPE ty_check_block.

    DATA: lt_tadir_block LIKE it_tadir.

    DATA: lt_objects TYPE zpp_abapgit_t_tadir.
    DATA: lt_errors  TYPE zpp_abapgit_t_serial_error.

    CONSTANTS: lc_block_size TYPE i VALUE 50.

    DATA(lv_objects_to_process) = lines( it_tadir ).

    "**********************************
    " Setup Buffer Space
    "**********************************
    CLEAR: mt_check_blocks.

    "**********************************
    " Partition Objects
    "**********************************
    DATA(lv_free_wp) = get_free_work_processes( ).
    DATA(lv_initial_free_wp) = lv_free_wp.
    DATA(lv_blocks) = ceil( CONV f( lv_objects_to_process ) / lc_block_size ).
    IF lv_initial_free_wp = 0.
      "No Free Work Processes: Run Locally
      lv_blocks = 1.
      CLEAR: st_check_block.
      st_check_block-task = |LOCAL-{ sy-index WIDTH = 6 ALIGN = RIGHT PAD = '0'  }|.
      st_check_block-block = it_tadir.
      INSERT st_check_block INTO TABLE mt_check_blocks.
    ELSE.
      "Run with Parallelization
      DO lv_blocks TIMES.

        CLEAR: st_check_block.
        st_check_block-task = |WORKER-{ sy-index WIDTH = 6 ALIGN = RIGHT PAD = '0'  }|.

        DATA(start) = ( sy-index - 1 ) * lc_block_size + 1.
        DATA(end)   = ( sy-index ) * lc_block_size.

        LOOP AT it_tadir ASSIGNING FIELD-SYMBOL(<ls_tadir>) FROM start TO end.
          INSERT <ls_tadir> INTO TABLE st_check_block-block.
        ENDLOOP.

        INSERT st_check_block INTO TABLE mt_check_blocks.

      ENDDO.
    ENDIF.

    ms_block_info-total = lines( mt_check_blocks ).

    "**********************************
    " Setup Progress Indicator
    "**********************************
    CREATE OBJECT mo_progress
      EXPORTING
        iv_total = ms_block_info-total.

    ##NO_TEXT
    mo_progress->show(
        iv_current = 0
        iv_text    = COND #( WHEN ms_block_info-total = 1
                               THEN |Checking { lv_objects_to_process } objects in { ms_block_info-total } block|
                             ELSE |Checking { lv_objects_to_process } objects in { ms_block_info-total } blocks| ) ).


    "**********************************
    "Process Work Packages
    "**********************************
    DO.

      "Determine unprocessed blocks
      DATA(lt_unprocessed) = FILTER tty_check_block( mt_check_blocks
                                                           USING KEY status
                                                           WHERE started = abap_false
                                                             AND done    = abap_false ).

      IF NOT line_exists( mt_check_blocks[ done = abap_false ] ).
        " All processing has ended
        EXIT.
      ENDIF.

      "Determine number of available work processes
      DATA(lv_scheduled) = 0.
      lv_free_wp = get_free_work_processes( ).
      IF lv_initial_free_wp > 0.
        "There should be free tasks in the system at some point,
        "so we schedule more blocks than can can be processed.
        "This way the system can process them directly if any of
        "the running processes finishes
        lv_free_wp = lv_free_wp + ( ( lv_free_wp * 3 ) / 4 ).
      ENDIF.

      " Schedule Unprocessed Blocks
      LOOP AT lt_unprocessed ASSIGNING FIELD-SYMBOL(<st_block>).

        IF lines( mt_check_blocks ) = 1.

          "There is only one block: We process it synchronously
          CALL FUNCTION 'Z_FM_ABAPGIT_CHECK_EXISTS'
            EXPORTING
              it_tadir          = <st_block>-block
            IMPORTING
              rt_tadir_existing = lt_objects
              rt_errors         = lt_errors
            EXCEPTIONS
              OTHERS            = 0.

          mt_check_blocks[ task = <st_block>-task ]-started = abap_true.
          process_check_block(
              p_task     = <st_block>-task
              it_objects = lt_objects
              it_errors  = lt_errors ) .

        ELSE.

          "Schedule block for processing
          CALL FUNCTION 'Z_FM_ABAPGIT_CHECK_EXISTS'
            STARTING NEW TASK <st_block>-task
            CALLING check_finished ON END OF TASK
            EXPORTING
              it_tadir              = <st_block>-block
            EXCEPTIONS
              system_failure        = 1
              communication_failure = 2
              resource_failure      = 3
              OTHERS                = 4.
          IF sy-subrc = 0.
            mt_check_blocks[ task = <st_block>-task ]-started = abap_true.
          ENDIF.

        ENDIF.

        ADD 1 TO lv_scheduled.
        IF lv_scheduled > lv_free_wp.
          "We only schedule as many packages as we have free work processes
          EXIT.
        ENDIF.

      ENDLOOP.

      "Wait 250ms or until all scheduled blocks have finished
      WAIT FOR ASYNCHRONOUS TASKS
        UNTIL NOT line_exists( mt_check_blocks[ done = abap_false ] )
        UP TO '0.25' SECONDS.

      "Update Progress Indicator
      ms_block_info-started = 0.
      ms_block_info-finished = 0.
      LOOP AT mt_check_blocks ASSIGNING FIELD-SYMBOL(<st_status>)
        USING KEY status
        WHERE started = abap_true.
        IF <st_status>-done = abap_false.
          ADD 1 TO ms_block_info-started.
        ELSEIF <st_status>-done = abap_true.
          ADD 1 TO ms_block_info-finished.
        ENDIF.
      ENDLOOP.

      mo_progress->show(
        iv_current = ms_block_info-finished
        iv_text    = |Checked { ms_block_info-finished }/{ ms_block_info-total } Blocks ({  ms_block_info-total -  ms_block_info-finished } remaining)| ) ##NO_TEXT.

    ENDDO.

    mo_progress->show(
        iv_current = ms_block_info-total
        iv_text    = |Finished Serialization| ) ##NO_TEXT.

    LOOP AT mt_check_blocks ASSIGNING <st_block>.
      INSERT LINES OF <st_block>-existing INTO TABLE rt_tadir.

      IF io_log IS BOUND.
        LOOP AT <st_block>-errors ASSIGNING FIELD-SYMBOL(<st_error>).
          io_log->add( iv_msg = |Existence Check failed for Object { <st_error>-obj_type }-{ <st_error>-obj_name }: { <st_error>-msg }|
                       iv_type = 'E' ).
        ENDLOOP.
      ENDIF.

    ENDLOOP.

  ENDMETHOD.                    "check_exists


  METHOD check_finished.

    DATA: lt_objects TYPE zpp_abapgit_t_tadir.
    DATA: lt_errors  TYPE zpp_abapgit_t_serial_error.

    RECEIVE RESULTS FROM FUNCTION 'Z_FM_ABAPGIT_CHECK_EXISTS'
      IMPORTING
        rt_tadir_existing     = lt_objects
        rt_errors             = lt_errors
      EXCEPTIONS
        abapgit_exception     = 1
        system_failure        = 2
        communication_failure = 3
        OTHERS                = 4.

    IF sy-subrc = 0.
      "If the check task fails we cannot process results
      process_check_block(
        EXPORTING
          p_task      = p_task
          it_objects  = lt_objects
          it_errors   = lt_errors ) .
    ELSE.
      "Block is no longer being processed
      mt_check_blocks[ task = p_task ]-started = abap_false.
    ENDIF.

  ENDMETHOD.


  METHOD process_check_block.

    IF line_exists( mt_check_blocks[ task = p_task ] ).
      mt_check_blocks[ task = p_task ]-done     = abap_true.
      mt_check_blocks[ task = p_task ]-existing = it_objects.
      mt_check_blocks[ task = p_task ]-errors   = it_errors.
    ENDIF.

  ENDMETHOD.
ENDCLASS.
