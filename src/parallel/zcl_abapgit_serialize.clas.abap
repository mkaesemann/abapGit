class ZCL_ABAPGIT_SERIALIZE definition
  public
  final
  create private .

public section.

  class-methods SERIALIZE_OBJECTS
    importing
      !I_LAST_SERIALIZATION type TIMESTAMP
      !I_MASTER_LANGUAGE type SPRAS
      !IO_LOG type ref to ZCL_ABAPGIT_LOG optional
      !IT_TADIR type ZIF_ABAPGIT_DEFINITIONS=>TY_TADIR_TT
      !IT_FILTER type SCTS_TADIR optional
      !IT_FILES type ZIF_ABAPGIT_DEFINITIONS=>TY_FILES_ITEM_TT optional
    returning
      value(RT_FILES) type ZIF_ABAPGIT_DEFINITIONS=>TY_FILES_ITEM_TT .
  class-methods SERIALIZE_FINISHED
    importing
      !P_TASK type CLIKE .
protected section.

  class-data MT_FILE_INFO type ZIF_ABAPGIT_DEFINITIONS=>TY_FILES_ITEM_TT .
  class-data MV_FINISHED_TASKS type I .
  class-data MV_TOTAL_TASKS type I .
  class-data MO_PROGRESS type ref to ZCL_ABAPGIT_PROGRESS .

  class-methods GET_MAX_MODES
    returning
      value(R_MAX_MODES) type I .
  class-methods PROCESS_SERIALIZE_BLOCK
    importing
      !P_TASK type CLIKE
      !IT_FILES type ZPP_ABAPGIT_T_FILE_ITEM .
private section.
ENDCLASS.



CLASS ZCL_ABAPGIT_SERIALIZE IMPLEMENTATION.


  METHOD get_max_modes.

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


  METHOD process_serialize_block.

    LOOP AT it_files ASSIGNING FIELD-SYMBOL(<st_file>).
      APPEND INITIAL LINE TO mt_file_info ASSIGNING FIELD-SYMBOL(<ls_return>).
      <ls_return>-file = <st_file>-file.
      <ls_return>-item = <st_file>-item.
    ENDLOOP.

    ADD 1 TO mv_finished_tasks.

  ENDMETHOD.


  METHOD serialize_finished.

    DATA(lt_files) = VALUE zpp_abapgit_t_file_item( ).

    RECEIVE RESULTS FROM FUNCTION 'Z_FM_ABAPGIT_SERIALIZE'
      IMPORTING
        rt_files = lt_files
      EXCEPTIONS
        OTHERS = 1.

    process_serialize_block(
      EXPORTING
        p_task   = p_task
        it_files = lt_files ) .

  ENDMETHOD.


  METHOD serialize_objects.

    CONSTANTS: c_reserved_wps TYPE i VALUE 2.

    DATA: lt_tadir_block TYPE zpp_abapgit_t_tadir,
          lt_files_info  TYPE zpp_abapgit_t_file_item.

    DATA(lt_files) = VALUE zpp_abapgit_t_file_item( ).

    "**********************************
    "Determine Number of Available Work Processes for Helper Tasks
    "**********************************
    DATA: lt_wpinfo TYPE STANDARD TABLE OF wpinfo
            WITH DEFAULT KEY.
    DATA(free_wp) = 0.

    CALL FUNCTION 'TH_WPINFO'
      TABLES
        wplist = lt_wpinfo
      EXCEPTIONS
        OTHERS = 0.

    LOOP AT lt_wpinfo TRANSPORTING NO FIELDS
      WHERE wp_typ     = 'DIA' "Dialog
        AND wp_istatus = 2.    "Waiting
      ADD 1 TO free_wp.
    ENDLOOP.
    "Leave some WPs for other users
    free_wp = COND #( WHEN free_wp <= c_reserved_wps THEN 0
                      ELSE free_wp - c_reserved_wps ).
*    "Limit to max. no. of allowed WPs
**    free_wp = COND #( WHEN free_wp > get_max_modes( ) THEN get_max_modes( )
**                        ELSE free_wp ).

    "We determine how many WPs we use based on the number of objects
    " we want to process. If enough objects exist, we use all free WPs
    " Otherwise we use blocks of the minimum size and only use the
    " required fraction of free WPs
    CONSTANTS: min_no_objects_per_wp TYPE i VALUE 10.
    DATA(objects_to_process) = lines( it_tadir ).

    IF objects_to_process < ( min_no_objects_per_wp * free_wp ).
      "We don't need all free WPs and limit the use
      free_wp = ceil( objects_to_process / min_no_objects_per_wp ).
    ENDIF.

    "**********************************
    "Determine Number of Helper Tasks
    "**********************************
    DATA(helper_tasks) = free_wp.
    mv_total_tasks = COND #( WHEN helper_tasks = 0 THEN 1
                             ELSE helper_tasks ).

    CREATE OBJECT mo_progress
      EXPORTING
        iv_total = mv_total_tasks.

    ##NO_TEXT
    mo_progress->show(
        iv_current = 0
        iv_text    = COND #( WHEN helper_tasks = 0
                               THEN |Serializing in { mv_total_tasks } Task|
                             ELSE |Serializing in { mv_total_tasks } Tasks| ) ).

    "**********************************
    "Determine Processing Block Size
    "**********************************
    DATA(block_size) = objects_to_process.
    IF helper_tasks > 0.
      "Async. Processing in Multiple Blocks
      block_size = COND i( WHEN objects_to_process MOD helper_tasks > 0
                             THEN objects_to_process DIV ( helper_tasks - 1 )
                           ELSE objects_to_process DIV helper_tasks ).
    ENDIF.

    "**********************************
    "Setup Buffer Space
    "**********************************
    CLEAR: mv_finished_tasks.
    mt_file_info = it_files.

    "**********************************
    "Start Async. Helper Tasks
    "**********************************
    DO helper_tasks TIMES.

      DATA(task_id) = |SERI{ sy-index }|.
      DATA(start) = ( sy-index - 1 ) * block_size + 1.
      DATA(end)   = ( sy-index ) * block_size.

      CLEAR: lt_tadir_block.

      LOOP AT it_tadir ASSIGNING FIELD-SYMBOL(<ls_tadir>) FROM start TO end.
        INSERT <ls_tadir> INTO TABLE lt_tadir_block.
      ENDLOOP.

      CALL FUNCTION 'Z_FM_ABAPGIT_SERIALIZE'
        STARTING NEW TASK task_id
        CALLING serialize_finished ON END OF TASK
        EXPORTING
          it_tadir             = lt_tadir_block
          i_last_serialization = i_last_serialization
          i_master_language    = i_master_language
        TABLES
          it_filter            = it_filter[]
        EXCEPTIONS
          OTHERS               = 0.

    ENDDO.

    IF helper_tasks = 0.
      "No Async.Helper Tasks: Process Synchronously
      CLEAR: lt_tadir_block.

      LOOP AT it_tadir ASSIGNING <ls_tadir> FROM 1 TO block_size.
        INSERT <ls_tadir> INTO TABLE lt_tadir_block.
      ENDLOOP.

      CALL FUNCTION 'Z_FM_ABAPGIT_SERIALIZE'
        EXPORTING
          it_tadir             = lt_tadir_block
          i_last_serialization = i_last_serialization
          i_master_language    = i_master_language
        IMPORTING
          rt_files             = lt_files
        TABLES
          it_filter            = it_filter[]
        EXCEPTIONS
          OTHERS               = 0.

      process_serialize_block(
        EXPORTING
          p_task   = 'LOCAL'
          it_files = lt_files ) .

      mo_progress->show(
          iv_current = 1
          iv_text    = |Finished Serialization| ) ##NO_TEXT.

    ELSE.
      "Wait for Async.Helpers and Provide User Feedback

      WHILE mv_finished_tasks < helper_tasks.

        DATA(async_helpers) = helper_tasks + 1.
        WAIT FOR ASYNCHRONOUS TASKS
          UNTIL mv_finished_tasks >= async_helpers
          UP TO '0.5' SECONDS.

        mo_progress->show(
          iv_current = mv_finished_tasks
          iv_text    = |Processing { mv_total_tasks - mv_finished_tasks } Serialization Tasks ({ mv_finished_tasks } Tasks finished)| ) ##NO_TEXT.

      ENDWHILE.

      mo_progress->show(
          iv_current = 1
          iv_text    = |Finished Serialization| ) ##NO_TEXT.

    ENDIF.

    helper_tasks = helper_tasks + 1.
    WAIT FOR ASYNCHRONOUS TASKS
      UNTIL mv_finished_tasks >= helper_tasks.

    "Reconstitute Result
    rt_files = mt_file_info.

  ENDMETHOD.
ENDCLASS.
