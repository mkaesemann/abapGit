class ZCL_ABAPGIT_CTS_INTEGRATION definition
  public
  final
  create private .

public section.

  class-methods PROPOSE_DEFAULT_TEXTS
    importing
      !IT_STAGED type ZIF_ABAPGIT_DEFINITIONS=>TY_STAGE_TT
      !IT_STATUS type ZIF_ABAPGIT_DEFINITIONS=>TY_RESULTS_TT
      !IO_FORM type ref to ZCL_ABAPGIT_STRING_MAP
    changing
      !CS_COMMIT type ZIF_ABAPGIT_SERVICES_GIT=>TY_COMMIT_FIELDS .
  class-methods SUPPLEMENT_TASK_INFO
    importing
      !IT_STAGED type ZIF_ABAPGIT_DEFINITIONS=>TY_STAGE_TT
      !IT_STATUS type ZIF_ABAPGIT_DEFINITIONS=>TY_RESULTS_TT
    changing
      !CS_COMMIT type ZIF_ABAPGIT_SERVICES_GIT=>TY_COMMIT_FIELDS .
  PROTECTED SECTION.

    TYPES: BEGIN OF ty_trkorr,
             trkorr TYPE trkorr,
           END OF ty_trkorr.
    TYPES: tty_trkorr TYPE SORTED TABLE OF ty_trkorr
             WITH UNIQUE KEY trkorr.
    TYPES: BEGIN OF ty_object,
             obj_type TYPE trobjtype,
             obj_name TYPE trobj_name,
           END OF ty_object.

    TYPES: BEGIN OF ty_lock_info,
             obj_type       TYPE trobjtype,
             obj_name       TYPE trobj_name,
             trkorr         TYPE trkorr,
             task           TYPE trkorr,      "Last or Open Task
             transport      TYPE trkorr,
             function       TYPE trfunction,
             status         TYPE trstatus,
             task_text      TYPE as4text,
             transport_text TYPE as4text,
             user           TYPE tr_as4user,
             current_user   TYPE abap_bool,
           END OF ty_lock_info.
    TYPES: tty_lock_info TYPE SORTED TABLE OF ty_lock_info
            WITH UNIQUE KEY obj_type obj_name
            WITH NON-UNIQUE SORTED KEY task COMPONENTS task
            WITH NON-UNIQUE SORTED KEY transport COMPONENTS transport.

    CLASS-METHODS get_lock_info
      IMPORTING
                !it_staged          TYPE zif_abapgit_definitions=>ty_stage_tt
                !it_status          TYPE zif_abapgit_definitions=>ty_results_tt
      RETURNING VALUE(rt_lock_info) TYPE tty_lock_info.

    CLASS-METHODS propose_default_body
      IMPORTING
                !it_lock_info TYPE tty_lock_info
      RETURNING VALUE(r_body) TYPE string .
    CLASS-METHODS propose_default_comment
      IMPORTING
                !it_lock_info    TYPE tty_lock_info
      RETURNING VALUE(r_comment) TYPE string .

PRIVATE SECTION.

  CONSTANTS:
    BEGIN OF cs_formid,
      committer       TYPE string VALUE 'committer',
      committer_name  TYPE string VALUE 'committer_name',
      committer_email TYPE string VALUE 'committer_email',
      message         TYPE string VALUE 'message',
      comment         TYPE string VALUE 'comment',
      body            TYPE string VALUE 'body',
      author          TYPE string VALUE 'author',
      author_name     TYPE string VALUE 'author_name',
      author_email    TYPE string VALUE 'author_email',
    END OF cs_formid.

  CLASS-METHODS get_lock_text
    IMPORTING
      !is_lock_info TYPE zcl_abapgit_cts_integration=>ty_lock_info
    RETURNING
      VALUE(r_text) TYPE string .
  CLASS-METHODS get_task_docu
    IMPORTING
      !i_trkorr     TYPE trkorr
    RETURNING
      VALUE(r_docu) TYPE string .
ENDCLASS.



CLASS ZCL_ABAPGIT_CTS_INTEGRATION IMPLEMENTATION.


  METHOD get_lock_info.

    "Collect Requests of Staged Objects
    DATA: lt_items TYPE zif_abapgit_definitions=>ty_items_tt.

    DATA: lt_objects TYPE SORTED TABLE OF ty_object
            WITH UNIQUE KEY obj_type obj_name.
    DATA: lrt_trkorr TYPE RANGE OF trkorr.

    CLEAR: rt_lock_info.

    "Reliably determine the object information for the staged files
    LOOP AT it_staged INTO DATA(ls_staged)
      WHERE file-path IS NOT INITIAL
        AND file-filename IS NOT INITIAL.
      TRY.
          DATA(ls_file) = it_status[ path     = ls_staged-file-path
                                     filename = ls_staged-file-filename ].
          INSERT CORRESPONDING #( ls_file ) INTO TABLE lt_items.
        CATCH cx_root.
          CONTINUE.
      ENDTRY.
    ENDLOOP.

    "Determine the transports/tasks for the staged objects that currently hold a lock in an open transport
    TRY.
        DATA(lt_transports) = zcl_abapgit_factory=>get_cts_api( )->get_transports_for_list( lt_items ).
        rt_lock_info = CORRESPONDING #( lt_transports DISCARDING DUPLICATES ).
      CATCH zcx_abapgit_exception.
        RETURN.
    ENDTRY.

    "Determine Task and Transport Information

    "Retrieve all Tasks and Transports Entries related to our Locks
    IF rt_lock_info IS INITIAL.
      RETURN.
    ENDIF.

    "Retrieve all Transports
    lrt_trkorr = VALUE #( FOR ls_object IN rt_lock_info
                            ( sign = 'I' option = 'EQ' low = ls_object-trkorr ) ).
    SELECT DISTINCT
           'I' AS sign,
           'EQ' AS option,
           CASE WHEN e070~strkorr IS NULL OR e070~strkorr = ' '
                  THEN e070~trkorr
                ELSE e070~strkorr
           END AS low
      FROM e070
      WHERE trkorr IN @lrt_trkorr
      INTO CORRESPONDING FIELDS OF TABLE @lrt_trkorr.

    "We now have a list of all Transports that hold tasks for objects the staging object list

    "Collect Object/Task/Transport Information for all Objects in the Tasks of the Transports
    SELECT e071~pgmid, e071~object, e071~obj_name,
           e071~trkorr      AS task,
           e070~trfunction  AS type,
           e070~trstatus    AS status,
           e070~strkorr     AS transport,
           e070~as4user     AS user,
           CASE WHEN e070~as4user = @sy-uname
                  THEN 'X'
                ELSE ' '
           END AS current_user,
           tasktx~as4text   AS task_text,
           transptx~as4text AS transport_text
      FROM e071 AS e071
      INNER JOIN e070 AS e070 ON e070~trkorr = e071~trkorr
      LEFT OUTER JOIN e07t AS tasktx ON tasktx~trkorr = e070~trkorr
                                    AND tasktx~langu  = 'E'
      LEFT OUTER JOIN e07t AS transptx ON transptx~trkorr = e070~strkorr
                                      AND transptx~langu  = 'E'
      WHERE e070~strkorr    IN @lrt_trkorr
        AND e070~trfunction = 'S'               "Tasks Only
        AND e071~pgmid IN ( 'R3TR', 'LIMU' )
      ORDER BY e071~pgmid, e071~trkorr
      INTO TABLE @DATA(lt_ott_list).

    "Tasks may contain LIMU Objects, that we need to resolve
    LOOP AT lt_ott_list ASSIGNING FIELD-SYMBOL(<ls_ott>).

      IF <ls_ott>-pgmid = 'R3TR'.
        "When we reach the first R3TR, we can abort, since all LIMUs are collected at the beginning due to the ORDER BY
        EXIT.
      ENDIF.

      "Resolve LIMU Object
      cl_wb_object_type=>get_tadir_from_limu(
        EXPORTING
          p_object         = <ls_ott>-object
          p_obj_name       = <ls_ott>-obj_name
        RECEIVING
          p_tadir          = DATA(ls_tadir)
        EXCEPTIONS
          conversion_error = 1 ).
      IF sy-subrc <> 0.
        CONTINUE.
      ENDIF.

      "Replace Link
      <ls_ott>-pgmid    = ls_tadir-pgmid.
      <ls_ott>-object   = ls_tadir-object.
      <ls_ott>-obj_name = ls_tadir-obj_name.

    ENDLOOP.

    "The only LIMUs that remain are unresolvable: We will ignore them
    SORT lt_ott_list BY
      pgmid    ASCENDING
      object   ASCENDING
      obj_name ASCENDING
      status   ASCENDING   "Open before Released
      task     DESCENDING. "Newest task before older ones (Assuming that we want texts from the latest closed task if we can't find an open one

    "Supplement Lock Information Table
    LOOP AT rt_lock_info ASSIGNING FIELD-SYMBOL(<ls_lock_info>).

      "According to the sort order of the OTT List, we grab the first hit, which retrieves a task in the following order:
      "  1. Latest Open Task containing the object (or part of it)
      "  2. Latest Released Task containing the object (or part of it)
      READ TABLE lt_ott_list ASSIGNING <ls_ott>
        BINARY SEARCH
        WITH KEY pgmid    = 'R3TR'
                 object   = <ls_lock_info>-obj_type
                 obj_name = <ls_lock_info>-obj_name.
      IF sy-subrc <> 0.
        CONTINUE. "This should normally not happen
      ENDIF.

      "Supplement Information
      <ls_lock_info>-task           = <ls_ott>-task.
      <ls_lock_info>-transport      = <ls_ott>-transport.
      <ls_lock_info>-function       = <ls_ott>-type.
      <ls_lock_info>-status         = <ls_ott>-status.
      <ls_lock_info>-task_text      = <ls_ott>-task_text.
      <ls_lock_info>-transport_text = <ls_ott>-transport_text.
      <ls_lock_info>-user           = <ls_ott>-user.
      <ls_lock_info>-current_user   = <ls_ott>-current_user.

    ENDLOOP.

  ENDMETHOD.


  METHOD get_lock_text.
    r_text = is_lock_info-task_text.
    IF r_text IS INITIAL.
      "Fallback to Request Text
      r_text = is_lock_info-transport_text.
    ENDIF.
  ENDMETHOD.


  METHOD get_task_docu.

    DATA: lt_request_docu TYPE STANDARD TABLE OF tline
            WITH EMPTY KEY.

    CLEAR: r_docu.

    "Get Documentation of Task/Transport
    CLEAR: lt_request_docu.
    CALL FUNCTION 'TRINT_DOCU_INTERFACE'
      EXPORTING
        iv_object           = i_trkorr
        iv_action           = 'R'
        iv_modify_appending = 'X'
      TABLES
        tt_line             = lt_request_docu
      EXCEPTIONS
        OTHERS              = 1.
    IF sy-subrc <> 0 OR lt_request_docu IS INITIAL.
      "No Documentation for Task
      RETURN.
    ENDIF.

    r_docu = |{ i_trkorr }:|.

    LOOP AT lt_request_docu INTO DATA(ls_docu).
      r_docu = SWITCH #( ls_docu-tdformat
                         WHEN '=' "Line Continuation
                           THEN r_docu && ls_docu-tdline
                         ELSE COND #( WHEN r_docu IS INITIAL
                                        THEN r_docu && ls_docu-tdline
                                      ELSE r_docu && cl_abap_char_utilities=>cr_lf && ls_docu-tdline ) ).
    ENDLOOP.

  ENDMETHOD.


  METHOD propose_default_body.

    DATA: lt_docu TYPE STANDARD TABLE OF ty_lock_info
             WITH EMPTY KEY.

    lt_docu = it_lock_info.
    SORT lt_docu
      BY current_user DESCENDING    "Own before foreign
         status       ASCENDING     "Open before released
         task         DESCENDING.   "Newest before oldest
    DELETE ADJACENT DUPLICATES FROM lt_docu
      COMPARING current_user status task.

    "We propose the task documentation of all tasks in the current staging
    CLEAR: r_body.

    LOOP AT lt_docu INTO DATA(ls_docu).

      DATA(docu) = get_task_docu( ls_docu-task ).

      r_body = COND #( WHEN r_body IS INITIAL AND docu IS INITIAL
                         THEN space
                       WHEN r_body IS INITIAL AND docu IS NOT INITIAL
                         THEN docu
                       WHEN r_body IS NOT INITIAL AND docu IS INITIAL
                         THEN r_body
                       WHEN r_body IS NOT INITIAL AND docu IS NOT INITIAL
                         THEN r_body && cl_abap_char_utilities=>cr_lf && docu ).

    ENDLOOP.

  ENDMETHOD.


  METHOD propose_default_comment.

    DATA: lt_docu TYPE STANDARD TABLE OF ty_lock_info
             WITH EMPTY KEY.

    lt_docu = it_lock_info.
    SORT lt_docu
      BY current_user DESCENDING    "Own before foreign
         status       ASCENDING     "Open before released
         task         DESCENDING.   "Newest before oldest

    READ TABLE lt_docu INTO DATA(ls_docu)
      BINARY SEARCH
      WITH KEY current_user = abap_true
               status       = 'D'.
    IF sy-subrc = 0.
      "We found the latest open task of this user: Best Match
      r_comment = get_lock_text( ls_docu ).
      RETURN.
    ENDIF.

    READ TABLE lt_docu INTO ls_docu
      BINARY SEARCH
      WITH KEY current_user = abap_true.
    IF sy-subrc = 0.
      "We found the latest released task of this user: Propose
      r_comment = get_lock_text( ls_docu ).
      RETURN.
    ENDIF.

    r_comment = get_lock_text( lt_docu[ 1 ] ).

  ENDMETHOD.


  METHOD propose_default_texts.

    TYPES: BEGIN OF ty_userdata,
             bname      TYPE xubname,
             name_first TYPE ad_namefir,
             name_last  TYPE ad_namelas,
             smtp_addr  TYPE ad_smtpadr,
           END OF ty_userdata,
           tty_user_data TYPE STANDARD TABLE OF ty_userdata
                           WITH EMPTY KEY.

    DATA: fixdate TYPE d VALUE '00010101'.

    DATA(lt_lock_info) = get_lock_info(
                           it_staged = it_staged
                           it_status = it_status ).

    IF lt_lock_info IS INITIAL.
      RETURN.
    ENDIF.

    "Propose Comment
    DATA(default_comment) = propose_default_comment( lt_lock_info ).
    cs_commit-comment = COND #( WHEN cs_commit-comment IS NOT INITIAL AND default_comment IS NOT INITIAL
                          THEN |{ default_comment } - { cs_commit-comment }|
                        WHEN cs_commit-comment IS NOT INITIAL AND default_comment IS INITIAL
                          THEN cs_commit-comment
                        ELSE default_comment ).
    TRY.
        io_form->set(
          iv_key = cs_formid-comment
          iv_val = cs_commit-comment ).
      CATCH cx_root.
    ENDTRY.

    "Propose Body
    cs_commit-body = COND #( WHEN cs_commit-body IS NOT INITIAL
                          THEN |{ cs_commit-body }| &
                               |{ cl_abap_char_utilities=>cr_lf }| &
                               |{ cl_abap_char_utilities=>cr_lf }| &
                               |--------------------------------------------------------------------------------| &
                               |{ cl_abap_char_utilities=>cr_lf }| &
                               |{ cl_abap_char_utilities=>cr_lf }| &
                               |{ propose_default_body( lt_lock_info ) }|
                        ELSE propose_default_body( lt_lock_info ) ).
    TRY.
        io_form->set(
          iv_key = cs_formid-body
          iv_val = cs_commit-body ).
      CATCH cx_root.
    ENDTRY.

    DATA(lt_userdata) = VALUE tty_user_data( ).
    SELECT
           user~bname,
           name~name_first,
           name~name_last,
           addr~smtp_addr
      FROM usr21 AS user
      INNER JOIN adrp AS name ON name~persnumber = user~persnumber
                             AND name~date_from  = @fixdate
                             AND name~nation     = ''
      INNER JOIN adr6 AS addr ON addr~addrnumber = user~addrnumber
                             AND addr~persnumber = user~persnumber
                             AND addr~date_from  = @fixdate
      WHERE user~bname = @sy-uname
      ORDER BY consnumber DESCENDING
      INTO CORRESPONDING FIELDS OF TABLE @lt_userdata.
    DATA(ls_userdata) = VALUE #( lt_userdata[ 1 ]
                                 DEFAULT VALUE ty_userdata( ) ).

    "Propose Commiter Name
    IF cs_commit-committer_name IS INITIAL OR
       cs_commit-committer_name = sy-uname.
      cs_commit-committer_name = |{ ls_userdata-name_first } { ls_userdata-name_last }|.
      TRY.
          io_form->set(
            iv_key = cs_formid-committer_name
            iv_val = cs_commit-committer_name ).
        CATCH cx_root.
      ENDTRY.
    ENDIF.

    "Propose Commiter eMail
    IF cs_commit-committer_email IS INITIAL.
      cs_commit-committer_email = to_lower( ls_userdata-smtp_addr ).
      TRY.
          io_form->set(
            iv_key = cs_formid-committer_email
            iv_val = cs_commit-committer_email ).
        CATCH cx_root.
      ENDTRY.
    ENDIF.

  ENDMETHOD.


  METHOD supplement_task_info.

    DATA: lt_transports TYPE SORTED TABLE OF trkorr
            WITH UNIQUE KEY table_line,
          lt_tasks      TYPE STANDARD TABLE OF ty_lock_info
            WITH EMPTY KEY.

    "Supplement Transport Request/Task Lock Links in Comment

    DATA(lt_lock_info) = get_lock_info(
                           it_staged = it_staged
                           it_status = it_status ).

    IF lt_lock_info IS INITIAL.
      RETURN.
    ENDIF.

    "Collect Distinct Transports
    LOOP AT lt_lock_info INTO DATA(ls_lock_info).
      INSERT ls_lock_info-transport INTO TABLE lt_transports.
    ENDLOOP.

    "Collect Distinct Tasks
    lt_tasks = lt_lock_info.
    SORT lt_tasks
      BY task.
    DELETE ADJACENT DUPLICATES FROM lt_tasks
      COMPARING task.

    IF lt_transports IS INITIAL AND lt_tasks IS INITIAL.
      RETURN.
    ENDIF.

    "Add Transport Locks
    DATA(tr_links) = VALUE string( ).
    LOOP AT lt_transports INTO DATA(trkorr).
      tr_links = SWITCH #( sy-tabix
                           WHEN 1 THEN trkorr
                           ELSE |, { trkorr }| ).
    ENDLOOP.
    SHIFT tr_links LEFT DELETING LEADING ', '.

    "Add Task Locks
    DATA(ta_links) = VALUE string( ).
    LOOP AT lt_tasks INTO DATA(ls_task).
      ta_links = SWITCH #( sy-tabix
                           WHEN 1 THEN ls_task-task
                           ELSE |, { ls_task-task }| ).
    ENDLOOP.
    SHIFT ta_links LEFT DELETING LEADING ', '.

    "Supplement Information to Comment
    cs_commit-comment = cs_commit-comment &&
                        | ({ tr_links } // { ta_links } )|.

  ENDMETHOD.
ENDCLASS.
