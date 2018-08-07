CLASS zcl_abapgit_parallel_serialize DEFINITION
  PUBLIC
  INHERITING FROM zcl_abapgit_parallel_base
  FINAL
  CREATE PRIVATE .

  PUBLIC SECTION.

    CLASS-METHODS serialize_objects
      IMPORTING
        !i_last_serialization TYPE timestamp
        !i_master_language    TYPE spras
        !io_log               TYPE REF TO zcl_abapgit_log OPTIONAL
        !it_tadir             TYPE zif_abapgit_definitions=>ty_tadir_tt
        !it_filter            TYPE zif_abapgit_definitions=>ty_tadir_tt OPTIONAL
        !it_files             TYPE zif_abapgit_definitions=>ty_files_item_tt OPTIONAL
      RETURNING
        VALUE(rt_files)       TYPE zif_abapgit_definitions=>ty_files_item_tt .
    CLASS-METHODS serialize_finished
      IMPORTING
        !p_task TYPE clike .
  PROTECTED SECTION.

    TYPES:
      BEGIN OF ty_serialize_block,
        task    TYPE string,
        started TYPE abap_bool,
        done    TYPE abap_bool,
        block   TYPE zpp_abapgit_t_tadir,
        files   TYPE zif_abapgit_definitions=>ty_files_item_tt,
        errors  TYPE zpp_abapgit_t_serial_error,
      END OF ty_serialize_block .
    TYPES:
      tty_serialize_block TYPE SORTED TABLE OF ty_serialize_block
             WITH UNIQUE KEY task
             WITH NON-UNIQUE SORTED KEY status COMPONENTS started done .

    CLASS-DATA mt_serialize_blocks TYPE tty_serialize_block .
    CLASS-DATA:
      BEGIN OF ms_block_info,
        started  TYPE i,
        finished TYPE i,
        total    TYPE i,
      END OF ms_block_info .

    CLASS-METHODS process_serialize_block
      IMPORTING
        !p_task    TYPE clike
        !it_files  TYPE zpp_abapgit_t_file_item
        !it_errors TYPE zpp_abapgit_t_serial_error OPTIONAL .
  PRIVATE SECTION.
ENDCLASS.



CLASS ZCL_ABAPGIT_PARALLEL_SERIALIZE IMPLEMENTATION.


  METHOD process_serialize_block.

    DATA: lt_files TYPE zif_abapgit_definitions=>ty_files_item_tt.

    LOOP AT it_files ASSIGNING FIELD-SYMBOL(<st_file>).
      APPEND INITIAL LINE TO lt_files ASSIGNING FIELD-SYMBOL(<ls_return>).
      <ls_return>-file = <st_file>-file.
      <ls_return>-item = <st_file>-item.
    ENDLOOP.

    IF line_exists( mt_serialize_blocks[ task = p_task ] ).
      mt_serialize_blocks[ task = p_task ]-done   = abap_true.
      mt_serialize_blocks[ task = p_task ]-files  = lt_files.
      mt_serialize_blocks[ task = p_task ]-errors = it_errors.
    ENDIF.

  ENDMETHOD.


  METHOD serialize_finished.

    DATA: lt_files  TYPE zpp_abapgit_t_file_item.
    DATA: lt_errors TYPE zpp_abapgit_t_serial_error.

    RECEIVE RESULTS FROM FUNCTION 'Z_FM_ABAPGIT_SERIALIZE'
      IMPORTING
        rt_files              = lt_files
        rt_errors             = lt_errors
      EXCEPTIONS
        abapgit_exception     = 1
        system_failure        = 2
        communication_failure = 3
        OTHERS                = 4.

    IF sy-subrc = 0.
      "If the serialization task fails we cannot process results
      process_serialize_block(
        EXPORTING
          p_task    = p_task
          it_files  = lt_files
          it_errors = lt_errors ) .
    ELSE.
      "Block is no longer being processed
      mt_serialize_blocks[ task = p_task ]-started = abap_false.
    ENDIF.

  ENDMETHOD.


  METHOD serialize_objects.

    DATA: lt_files_info  TYPE zpp_abapgit_t_file_item.
    DATA: st_serialize_block TYPE ty_serialize_block.

    DATA: lt_tadir_block LIKE it_tadir.

    DATA: lt_files  TYPE zpp_abapgit_t_file_item.
    DATA: lt_errors TYPE zpp_abapgit_t_serial_error.
    DATA: lt_filter TYPE STANDARD TABLE OF tadir
            WITH DEFAULT KEY.
    FIELD-SYMBOLS: <st_filter>      LIKE LINE OF it_filter[],
                   <st_filter_line> LIKE LINE OF lt_filter.

    CONSTANTS: lc_block_size TYPE i VALUE 50.

    DATA(lv_objects_to_process) = lines( it_tadir ).

    "**********************************
    " Setup Buffer Space & Remap Filter
    "**********************************
    CLEAR: mt_serialize_blocks.

    LOOP AT it_filter ASSIGNING <st_filter>.
      APPEND INITIAL LINE TO lt_filter ASSIGNING <st_filter_line>.
      MOVE-CORRESPONDING <st_filter> TO <st_filter_line>.
    ENDLOOP.

    "**********************************
    " Partition Objects
    "**********************************
    DATA(lv_free_wp) = get_free_work_processes( ).
    DATA(lv_initial_free_wp) = lv_free_wp.
    DATA(lv_blocks) = ceil( CONV f( lv_objects_to_process ) / lc_block_size ).
    IF lv_initial_free_wp = 0.
      "No Free Work Processes: Run Locally
      lv_blocks = 1.
      CLEAR: st_serialize_block.
      st_serialize_block-task = |LOCAL-{ sy-index WIDTH = 6 ALIGN = RIGHT PAD = '0'  }|.
      st_serialize_block-block = it_tadir.
      INSERT st_serialize_block INTO TABLE mt_serialize_blocks.
    ELSE.
      "Run with Parallelization
      DO lv_blocks TIMES.

        CLEAR: st_serialize_block.
        st_serialize_block-task = |WORKER-{ sy-index WIDTH = 6 ALIGN = RIGHT PAD = '0'  }|.

        DATA(start) = ( sy-index - 1 ) * lc_block_size + 1.
        DATA(end)   = ( sy-index ) * lc_block_size.

        LOOP AT it_tadir ASSIGNING FIELD-SYMBOL(<ls_tadir>) FROM start TO end.
          INSERT <ls_tadir> INTO TABLE st_serialize_block-block.
        ENDLOOP.

        INSERT st_serialize_block INTO TABLE mt_serialize_blocks.

      ENDDO.
    ENDIF.

    ms_block_info-total = lines( mt_serialize_blocks ).

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
                               THEN |Serializing { lv_objects_to_process } objects in { ms_block_info-total } block|
                             ELSE |Serializing { lv_objects_to_process } objects in { ms_block_info-total } blocks| ) ).


    "**********************************
    "Process Work Packages
    "**********************************
    DO.

      "Determine unprocessed blocks
      DATA(lt_unprocessed) = FILTER tty_serialize_block( mt_serialize_blocks
                                                           USING KEY status
                                                           WHERE started = abap_false
                                                             AND done    = abap_false ).

      IF NOT line_exists( mt_serialize_blocks[ done = abap_false ] ).
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

        IF lines( mt_serialize_blocks ) = 1.

          "There is only one block: We process it synchronously
          CALL FUNCTION 'Z_FM_ABAPGIT_SERIALIZE'
            EXPORTING
              it_tadir             = <st_block>-block
              i_last_serialization = i_last_serialization
              i_master_language    = i_master_language
            IMPORTING
              rt_files             = lt_files
              rt_errors            = lt_errors
            TABLES
              it_filter            = lt_filter
            EXCEPTIONS
              OTHERS               = 0.

          mt_serialize_blocks[ task = <st_block>-task ]-started = abap_true.
          process_serialize_block(
              p_task    = <st_block>-task
              it_files  = lt_files
              it_errors = lt_errors ) .

        ELSE.

          "Schedule block for processing
          CALL FUNCTION 'Z_FM_ABAPGIT_SERIALIZE'
            STARTING NEW TASK <st_block>-task
            CALLING serialize_finished ON END OF TASK
            EXPORTING
              it_tadir              = <st_block>-block
              i_last_serialization  = i_last_serialization
              i_master_language     = i_master_language
            TABLES
              it_filter             = lt_filter
            EXCEPTIONS
              system_failure        = 1
              communication_failure = 2
              resource_failure      = 3
              OTHERS                = 4.
          IF sy-subrc = 0.
            mt_serialize_blocks[ task = <st_block>-task ]-started = abap_true.
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
        UNTIL NOT line_exists( mt_serialize_blocks[ done = abap_false ] )
        UP TO '0.25' SECONDS.

      "Update Progress Indicator
      ms_block_info-started = 0.
      ms_block_info-finished = 0.
      LOOP AT mt_serialize_blocks ASSIGNING FIELD-SYMBOL(<st_status>)
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
        iv_text    = |Processed { ms_block_info-finished }/{ ms_block_info-total } Blocks ({  ms_block_info-total -  ms_block_info-finished } remaining)| ) ##NO_TEXT.

    ENDDO.

    mo_progress->show(
        iv_current = 1
        iv_text    = |Finished Serialization| ) ##NO_TEXT.

    rt_files = it_files.
    LOOP AT mt_serialize_blocks ASSIGNING <st_block>.
      INSERT LINES OF <st_block>-files INTO TABLE rt_files.

      IF io_log IS BOUND.
        LOOP AT <st_block>-errors ASSIGNING FIELD-SYMBOL(<st_error>).
          io_log->add( iv_msg = |Serialization Issue for Object { <st_error>-obj_type }-{ <st_error>-obj_name }: { <st_error>-msg }|
                       iv_type = 'E' ).
        ENDLOOP.
      ENDIF.

    ENDLOOP.

  ENDMETHOD.
ENDCLASS.
