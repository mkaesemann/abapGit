CLASS zcl_abapgit_cts_integration DEFINITION
  PUBLIC
  FINAL
  CREATE PRIVATE .

  PUBLIC SECTION.
    TYPE-POOLS trsel .

    CLASS-METHODS propose_default_texts
      IMPORTING
        !it_staged TYPE zif_abapgit_definitions=>ty_stage_tt
        !io_form   TYPE REF TO zcl_abapgit_string_map
        !io_repo   TYPE REF TO zif_abapgit_repo_online
      CHANGING
        !cs_commit TYPE zif_abapgit_services_git=>ty_commit_fields .
    CLASS-METHODS supplement_task_info
      IMPORTING
        !it_staged TYPE zif_abapgit_definitions=>ty_stage_tt
      CHANGING
        !cs_commit TYPE zif_abapgit_services_git=>ty_commit_fields .
    CLASS-METHODS get_open_user_requests
      IMPORTING
        !i_tasks           TYPE abap_bool DEFAULT abap_true
        !i_requests        TYPE abap_bool DEFAULT abap_true
        !i_parent_request  TYPE abap_bool DEFAULT abap_true
        !i_recent_days     TYPE i DEFAULT 2
        !i_eval_os4        TYPE abap_bool DEFAULT abap_true
      RETURNING
        VALUE(rt_requests) TYPE trsel_trt_trkorr .
    CLASS-METHODS popup_select_own_tr_requests
      IMPORTING
        !is_selection        TYPE trwbo_selection
        !iv_title            TYPE trwbo_title
        !iv_username_pattern TYPE any DEFAULT sy-uname
      RETURNING
        VALUE(rt_r_trkorr)   TYPE zif_abapgit_definitions=>ty_trrngtrkor_tt
      RAISING
        zcx_abapgit_exception .
    CLASS-METHODS on_event
      IMPORTING
        !action   TYPE csequence
        !getdata  TYPE csequence
        !postdata TYPE zif_abapgit_html_viewer=>ty_post_data .
    TYPES:
      BEGIN OF ty_changed_by,
        item     TYPE zif_abapgit_definitions=>ty_item,
        filename TYPE string,
        name     TYPE syuname,
      END OF ty_changed_by.
    TYPES ty_changed_by_tt TYPE SORTED TABLE OF ty_changed_by WITH UNIQUE KEY item filename.

    CLASS-METHODS changed_by_bulk
      IMPORTING
        !it_files             TYPE zif_abapgit_definitions=>ty_files_item_tt
      RETURNING
        VALUE(rt_changed_by) TYPE ty_changed_by_tt .

    CLASS-METHODS get_transportable_transports
      IMPORTING
        !it_items             TYPE zif_abapgit_definitions=>ty_items_tt
      RETURNING
        VALUE(rt_transports) TYPE zif_abapgit_cts_api=>ty_transport_list .

    CLASS-METHODS find_changed_by
      IMPORTING
        !ii_repo              TYPE REF TO zif_abapgit_repo
        !it_files             TYPE zif_abapgit_definitions=>ty_stage_files
        !it_transports        TYPE zif_abapgit_cts_api=>ty_transport_list
      RETURNING
        VALUE(rt_changed_by) TYPE ty_changed_by_tt .
  PROTECTED SECTION.
    TYPES: BEGIN OF ty_trkorr,
             trkorr TYPE trkorr,
           END OF ty_trkorr.
    TYPES tty_trkorr TYPE SORTED TABLE OF ty_trkorr WITH UNIQUE KEY trkorr.
    TYPES: BEGIN OF ty_object,
             obj_type TYPE trobjtype,
             obj_name TYPE trobj_name,
           END OF ty_object.

    TYPES: BEGIN OF ty_lock_info,
             obj_type       TYPE trobjtype,
             obj_name       TYPE trobj_name,
             trkorr         TYPE trkorr,
             task           TYPE trkorr, " Last or Open Task
             transport      TYPE trkorr,
             function       TYPE trfunction,
             status         TYPE trstatus,
             task_text      TYPE as4text,
             transport_text TYPE as4text,
             user           TYPE tr_as4user,
             current_user   TYPE abap_bool,
           END OF ty_lock_info.
    TYPES tty_lock_info TYPE SORTED TABLE OF ty_lock_info
           WITH UNIQUE KEY obj_type obj_name
           WITH NON-UNIQUE SORTED KEY task COMPONENTS task
           WITH NON-UNIQUE SORTED KEY transport COMPONENTS transport.

    CLASS-METHODS get_lock_info
      IMPORTING it_staged           TYPE zif_abapgit_definitions=>ty_stage_tt
      RETURNING VALUE(rt_lock_info) TYPE tty_lock_info.

    CLASS-METHODS propose_default_body
      IMPORTING it_lock_info  TYPE tty_lock_info
      RETURNING VALUE(r_body) TYPE string.

    CLASS-METHODS propose_default_comment
      IMPORTING it_lock_info     TYPE tty_lock_info
      RETURNING VALUE(r_comment) TYPE string.

  PRIVATE SECTION.
    CLASS-DATA m_current_repo_key TYPE string.

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
      IMPORTING is_lock_info  TYPE zcl_abapgit_cts_integration=>ty_lock_info
      RETURNING VALUE(r_text) TYPE string.

    CLASS-METHODS get_task_docu
      IMPORTING i_trkorr      TYPE trkorr
      RETURNING VALUE(r_docu) TYPE string.

ENDCLASS.



CLASS ZCL_ABAPGIT_CTS_INTEGRATION IMPLEMENTATION.


  METHOD get_lock_info.

    " Collect Requests of Staged Objects
    DATA lt_items   TYPE zif_abapgit_definitions=>ty_items_tt.
    DATA lrt_trkorr TYPE RANGE OF trkorr.

    CLEAR rt_lock_info.

    " Reliably determine the object information for the staged files
    lt_items = VALUE #( FOR <ls_staged_object> IN it_staged
                        ( obj_type  = <ls_staged_object>-status-obj_type
                          obj_name  = <ls_staged_object>-status-obj_name
                          devclass  = <ls_staged_object>-status-package
                          inactive  = <ls_staged_object>-status-inactive
                          origlang  = <ls_staged_object>-status-origlang
                          srcsystem = <ls_staged_object>-status-srcsystem ) ).

    " Determine the transports/tasks for the staged objects that currently hold a lock in an open transport
    TRY.
        DATA(lt_transports) = zcl_abapgit_factory=>get_cts_api( )->get_transports_for_list(
                                  lt_items ).
        LOOP AT lt_transports INTO DATA(ls_transport).
          INSERT CORRESPONDING ty_lock_info( ls_transport )
                 INTO TABLE rt_lock_info.
        ENDLOOP.
      CATCH zcx_abapgit_exception.
        RETURN.
    ENDTRY.

    " Determine Task and Transport Information

    " Retrieve all Tasks and Transports Entries related to our Locks
    IF rt_lock_info IS INITIAL.
      RETURN.
    ENDIF.

    " Retrieve all Transports
    lrt_trkorr = VALUE #( FOR ls_object IN rt_lock_info
                          ( sign = 'I' option = 'EQ' low = ls_object-trkorr ) ).
    SELECT DISTINCT 'I'                                                  AS sign,
                    'EQ'                                                 AS option,
                    CASE WHEN e070~strkorr IS NULL OR e070~strkorr = ' '
                           THEN e070~trkorr
                         ELSE e070~strkorr
                    END                                                  AS low
      FROM e070
      WHERE trkorr IN @lrt_trkorr
      INTO CORRESPONDING FIELDS OF TABLE @lrt_trkorr.

    " We now have a list of all Transports that hold tasks for objects the staging object list

    " Collect Object/Task/Transport Information for all Objects in the Tasks of the Transports
    SELECT e071~pgmid,
           e071~object,
           e071~obj_name,
           e071~trkorr                        AS task,
           e070~trfunction                    AS type,
           e070~trstatus                      AS status,
           e070~strkorr                       AS transport,
           e070~as4user                       AS user,
           CASE WHEN e070~as4user = @sy-uname
                  THEN 'X'
                ELSE ' '
           END                                AS current_user,
           tasktx~as4text                     AS task_text,
           transptx~as4text                   AS transport_text
      FROM e071                 AS e071
           INNER JOIN e070      AS e070     ON e070~trkorr = e071~trkorr
           LEFT OUTER JOIN e07t AS tasktx   ON  tasktx~trkorr = e070~trkorr
                                            AND tasktx~langu  = 'E'
           LEFT OUTER JOIN e07t AS transptx ON  transptx~trkorr = e070~strkorr
                                            AND transptx~langu  = 'E'
      WHERE e070~strkorr    IN @lrt_trkorr
        AND e070~trfunction  = 'S'               " Tasks Only
        AND e071~pgmid      IN ( 'R3TR', 'LIMU' )
      ORDER BY e071~pgmid,
               e071~trkorr
      INTO TABLE @DATA(lt_ott_list).

    " Tasks may contain LIMU Objects, that we need to resolve
    LOOP AT lt_ott_list ASSIGNING FIELD-SYMBOL(<ls_ott>).

      IF <ls_ott>-pgmid = 'R3TR'.
        " When we reach the first R3TR, we can abort, since all LIMUs are collected at the beginning due to the ORDER BY
        EXIT.
      ENDIF.

      " Resolve LIMU Object
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

      " Replace Link
      <ls_ott>-pgmid    = ls_tadir-pgmid.
      <ls_ott>-object   = ls_tadir-object.
      <ls_ott>-obj_name = ls_tadir-obj_name.

    ENDLOOP.

    " The only LIMUs that remain are unresolvable: We will ignore them
    SORT lt_ott_list BY
      pgmid    ASCENDING
      object   ASCENDING
      obj_name ASCENDING
      status   ASCENDING   " Open before Released
      task     DESCENDING. " Newest task before older ones (Assuming that we want texts from the latest closed task if we can't find an open one

    " Supplement Lock Information Table
    LOOP AT rt_lock_info ASSIGNING FIELD-SYMBOL(<ls_lock_info>).

      " According to the sort order of the OTT List, we grab the first hit, which retrieves a task in the following order:
      "  1. Latest Open Task containing the object (or part of it)
      "  2. Latest Released Task containing the object (or part of it)
      READ TABLE lt_ott_list ASSIGNING <ls_ott>
           BINARY SEARCH
           WITH KEY pgmid    = 'R3TR'
                    object   = <ls_lock_info>-obj_type
                    obj_name = <ls_lock_info>-obj_name.
      IF sy-subrc <> 0.
        CONTINUE. " This should normally not happen
      ENDIF.

      " Supplement Information
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
      " Fallback to Request Text
      r_text = is_lock_info-transport_text.
    ENDIF.
  ENDMETHOD.


  METHOD get_task_docu.

    DATA lt_request_docu TYPE STANDARD TABLE OF tline WITH EMPTY KEY.

    CLEAR r_docu.

    " Get Documentation of Task/Transport
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
      " No Documentation for Task
      RETURN.
    ENDIF.

    r_docu = |{ i_trkorr }:|.

    LOOP AT lt_request_docu INTO DATA(ls_docu).
      r_docu = SWITCH #( ls_docu-tdformat
                         WHEN '=' " Line Continuation
                         THEN r_docu && ls_docu-tdline
                         ELSE COND #( WHEN r_docu IS INITIAL
                                      THEN r_docu && ls_docu-tdline
                                      ELSE r_docu && cl_abap_char_utilities=>cr_lf && ls_docu-tdline ) ).
    ENDLOOP.

  ENDMETHOD.


  METHOD changed_by_bulk.

    TYPES ty_prog_name_tt TYPE SORTED TABLE OF reposrc-progname WITH UNIQUE KEY table_line.
    TYPES ty_clas_name_tt TYPE SORTED TABLE OF vseoclass-clsname WITH UNIQUE KEY table_line.
    TYPES ty_intf_name_tt TYPE SORTED TABLE OF vseointerf-clsname WITH UNIQUE KEY table_line.
    TYPES ty_tabl_name_tt TYPE SORTED TABLE OF dd02l-tabname WITH UNIQUE KEY table_line.
    TYPES ty_view_name_tt TYPE SORTED TABLE OF dd25l-viewname WITH UNIQUE KEY table_line.
    TYPES ty_dtel_name_tt TYPE SORTED TABLE OF dd04l-rollname WITH UNIQUE KEY table_line.
    TYPES ty_doma_name_tt TYPE SORTED TABLE OF dd01l-domname WITH UNIQUE KEY table_line.
    TYPES ty_msag_name_tt TYPE SORTED TABLE OF t100a-arbgb WITH UNIQUE KEY table_line.
    TYPES:
      BEGIN OF ty_obj_user,
        obj_type TYPE tadir-object,
        obj_name TYPE tadir-obj_name,
        name     TYPE syuname,
      END OF ty_obj_user.
    TYPES ty_obj_user_tt TYPE HASHED TABLE OF ty_obj_user WITH UNIQUE KEY obj_type obj_name.
    TYPES:
      BEGIN OF ty_name_user,
        obj_name TYPE tadir-obj_name,
        name     TYPE syuname,
      END OF ty_name_user.

    DATA lt_prog_names TYPE ty_prog_name_tt.
    DATA lt_clas_names TYPE ty_clas_name_tt.
    DATA lt_intf_names TYPE ty_intf_name_tt.
    DATA lt_tabl_names TYPE ty_tabl_name_tt.
    DATA lt_view_names TYPE ty_view_name_tt.
    DATA lt_dtel_names TYPE ty_dtel_name_tt.
    DATA lt_doma_names TYPE ty_doma_name_tt.
    DATA lt_msag_names TYPE ty_msag_name_tt.
    DATA lt_name_user TYPE STANDARD TABLE OF ty_name_user WITH DEFAULT KEY.
    DATA lt_obj_user TYPE ty_obj_user_tt.
    DATA ls_obj_user LIKE LINE OF lt_obj_user.
    DATA ls_changed_by LIKE LINE OF rt_changed_by.
    DATA ls_name_user LIKE LINE OF lt_name_user.

    FIELD-SYMBOLS <ls_file> LIKE LINE OF it_files.

    LOOP AT it_files ASSIGNING <ls_file> WHERE item IS NOT INITIAL.
      CASE <ls_file>-item-obj_type.
        WHEN 'PROG'.
          INSERT CONV reposrc-progname( <ls_file>-item-obj_name ) INTO TABLE lt_prog_names.
        WHEN 'CLAS'.
          INSERT CONV vseoclass-clsname( <ls_file>-item-obj_name ) INTO TABLE lt_clas_names.
        WHEN 'INTF'.
          INSERT CONV vseointerf-clsname( <ls_file>-item-obj_name ) INTO TABLE lt_intf_names.
        WHEN 'TABL'.
          INSERT CONV dd02l-tabname( <ls_file>-item-obj_name ) INTO TABLE lt_tabl_names.
        WHEN 'VIEW'.
          INSERT CONV dd25l-viewname( <ls_file>-item-obj_name ) INTO TABLE lt_view_names.
        WHEN 'DTEL'.
          INSERT CONV dd04l-rollname( <ls_file>-item-obj_name ) INTO TABLE lt_dtel_names.
        WHEN 'DOMA'.
          INSERT CONV dd01l-domname( <ls_file>-item-obj_name ) INTO TABLE lt_doma_names.
        WHEN 'MSAG'.
          INSERT CONV t100a-arbgb( <ls_file>-item-obj_name ) INTO TABLE lt_msag_names.
      ENDCASE.
    ENDLOOP.

    IF lt_prog_names IS NOT INITIAL.
      SELECT progname AS obj_name unam AS name FROM reposrc
        INTO TABLE lt_name_user
        FOR ALL ENTRIES IN lt_prog_names
        WHERE progname = lt_prog_names-table_line
        AND r3state = 'A'.
      LOOP AT lt_name_user INTO ls_name_user.
        ls_obj_user-obj_type = 'PROG'.
        ls_obj_user-obj_name = ls_name_user-obj_name.
        ls_obj_user-name = ls_name_user-name.
        INSERT ls_obj_user INTO TABLE lt_obj_user.
      ENDLOOP.
    ENDIF.

    IF lt_clas_names IS NOT INITIAL.
      SELECT clsname AS obj_name changedby AS name FROM vseoclass
        INTO TABLE lt_name_user
        FOR ALL ENTRIES IN lt_clas_names
        WHERE clsname = lt_clas_names-table_line.
      LOOP AT lt_name_user INTO ls_name_user.
        ls_obj_user-obj_type = 'CLAS'.
        ls_obj_user-obj_name = ls_name_user-obj_name.
        ls_obj_user-name = ls_name_user-name.
        INSERT ls_obj_user INTO TABLE lt_obj_user.
      ENDLOOP.
    ENDIF.

    IF lt_intf_names IS NOT INITIAL.
      SELECT clsname AS obj_name changedby AS name FROM vseointerf
        INTO TABLE lt_name_user
        FOR ALL ENTRIES IN lt_intf_names
        WHERE clsname = lt_intf_names-table_line.
      LOOP AT lt_name_user INTO ls_name_user.
        ls_obj_user-obj_type = 'INTF'.
        ls_obj_user-obj_name = ls_name_user-obj_name.
        ls_obj_user-name = ls_name_user-name.
        INSERT ls_obj_user INTO TABLE lt_obj_user.
      ENDLOOP.
    ENDIF.

    IF lt_tabl_names IS NOT INITIAL.
      SELECT tabname AS obj_name as4user AS name FROM dd02l
        INTO TABLE lt_name_user
        FOR ALL ENTRIES IN lt_tabl_names
        WHERE tabname = lt_tabl_names-table_line.
      LOOP AT lt_name_user INTO ls_name_user.
        ls_obj_user-obj_type = 'TABL'.
        ls_obj_user-obj_name = ls_name_user-obj_name.
        ls_obj_user-name = ls_name_user-name.
        INSERT ls_obj_user INTO TABLE lt_obj_user.
      ENDLOOP.
    ENDIF.

    IF lt_view_names IS NOT INITIAL.
      SELECT viewname AS obj_name as4user AS name FROM dd25l
        INTO TABLE lt_name_user
        FOR ALL ENTRIES IN lt_view_names
        WHERE viewname = lt_view_names-table_line.
      LOOP AT lt_name_user INTO ls_name_user.
        ls_obj_user-obj_type = 'VIEW'.
        ls_obj_user-obj_name = ls_name_user-obj_name.
        ls_obj_user-name = ls_name_user-name.
        INSERT ls_obj_user INTO TABLE lt_obj_user.
      ENDLOOP.
    ENDIF.

    IF lt_dtel_names IS NOT INITIAL.
      SELECT rollname AS obj_name as4user AS name FROM dd04l
        INTO TABLE lt_name_user
        FOR ALL ENTRIES IN lt_dtel_names
        WHERE rollname = lt_dtel_names-table_line.
      LOOP AT lt_name_user INTO ls_name_user.
        ls_obj_user-obj_type = 'DTEL'.
        ls_obj_user-obj_name = ls_name_user-obj_name.
        ls_obj_user-name = ls_name_user-name.
        INSERT ls_obj_user INTO TABLE lt_obj_user.
      ENDLOOP.
    ENDIF.

    IF lt_doma_names IS NOT INITIAL.
      SELECT domname AS obj_name as4user AS name FROM dd01l
        INTO TABLE lt_name_user
        FOR ALL ENTRIES IN lt_doma_names
        WHERE domname = lt_doma_names-table_line.
      LOOP AT lt_name_user INTO ls_name_user.
        ls_obj_user-obj_type = 'DOMA'.
        ls_obj_user-obj_name = ls_name_user-obj_name.
        ls_obj_user-name = ls_name_user-name.
        INSERT ls_obj_user INTO TABLE lt_obj_user.
      ENDLOOP.
    ENDIF.

    IF lt_msag_names IS NOT INITIAL.
      SELECT arbgb AS obj_name lastuser AS name FROM t100a
        INTO TABLE lt_name_user
        FOR ALL ENTRIES IN lt_msag_names
        WHERE arbgb = lt_msag_names-table_line.
      LOOP AT lt_name_user INTO ls_name_user.
        ls_obj_user-obj_type = 'MSAG'.
        ls_obj_user-obj_name = ls_name_user-obj_name.
        ls_obj_user-name = ls_name_user-name.
        INSERT ls_obj_user INTO TABLE lt_obj_user.
      ENDLOOP.
    ENDIF.

    LOOP AT it_files ASSIGNING <ls_file> WHERE item IS NOT INITIAL.
      CLEAR ls_changed_by.
      ls_changed_by-item = <ls_file>-item.
      ls_changed_by-filename = <ls_file>-file-filename.

      READ TABLE lt_obj_user INTO ls_obj_user WITH TABLE KEY
        obj_type = <ls_file>-item-obj_type
        obj_name = <ls_file>-item-obj_name.
      IF sy-subrc = 0 AND ls_obj_user-name IS NOT INITIAL.
        ls_changed_by-name = ls_obj_user-name.
      ELSE.
        ls_changed_by-name = zcl_abapgit_objects=>changed_by(
          is_item     = <ls_file>-item
          iv_filename = <ls_file>-file-filename ).
      ENDIF.

      INSERT ls_changed_by INTO TABLE rt_changed_by.
    ENDLOOP.

  ENDMETHOD.


  METHOD find_changed_by.

    TYPES: BEGIN OF ty_transport_user,
             trkorr TYPE trkorr,
             name   TYPE syuname,
           END OF ty_transport_user.
    TYPES ty_transport_user_tt TYPE HASHED TABLE OF ty_transport_user WITH UNIQUE KEY trkorr.

    DATA: ls_remote            LIKE LINE OF it_files-remote,
          ls_changed_by        LIKE LINE OF rt_changed_by,
          lt_changed_by_remote LIKE rt_changed_by,
          lt_changed_by_local  LIKE rt_changed_by,
          ls_item              TYPE zif_abapgit_definitions=>ty_item,
          lv_transport         LIKE LINE OF it_transports,
          lt_trkorr            TYPE zif_abapgit_cts_api=>ty_trkorr_tt,
          lt_transport_users   TYPE ty_transport_user_tt,
          ls_transport_user    LIKE LINE OF lt_transport_users.

    FIELD-SYMBOLS <ls_changed_by> LIKE LINE OF lt_changed_by_remote.

    lt_changed_by_local = changed_by_bulk( it_files-local ).
    INSERT LINES OF lt_changed_by_local INTO TABLE rt_changed_by.

    LOOP AT it_files-remote INTO ls_remote WHERE filename IS NOT INITIAL.
      TRY.
          zcl_abapgit_filename_logic=>file_to_object(
            EXPORTING
              iv_filename = ls_remote-filename
              iv_path     = ls_remote-path
              io_dot      = ii_repo->get_dot_abapgit( )
            IMPORTING
              es_item     = ls_item ).
          ls_changed_by-item = ls_item.
          ls_changed_by-filename = ls_remote-filename.
          INSERT ls_changed_by INTO TABLE lt_changed_by_remote.
        CATCH zcx_abapgit_exception ##NO_HANDLER.
      ENDTRY.
    ENDLOOP.

    LOOP AT it_transports INTO lv_transport WHERE trkorr IS NOT INITIAL.
      APPEND lv_transport-trkorr TO lt_trkorr.
    ENDLOOP.
    SORT lt_trkorr.
    DELETE ADJACENT DUPLICATES FROM lt_trkorr.

    IF lt_trkorr IS NOT INITIAL.
      SELECT trkorr as4user AS name FROM e070
        INTO TABLE lt_transport_users
        FOR ALL ENTRIES IN lt_trkorr
        WHERE trkorr = lt_trkorr-table_line.
    ENDIF.

    LOOP AT lt_changed_by_remote ASSIGNING <ls_changed_by>.
      CLEAR lv_transport.
      READ TABLE it_transports WITH KEY
        obj_type = <ls_changed_by>-item-obj_type
        obj_name = <ls_changed_by>-item-obj_name
        INTO lv_transport.
      IF sy-subrc = 0.
        READ TABLE lt_transport_users INTO ls_transport_user
          WITH TABLE KEY trkorr = lv_transport-trkorr.
        IF sy-subrc = 0 AND ls_transport_user-name IS NOT INITIAL.
          <ls_changed_by>-name = ls_transport_user-name.
        ENDIF.
      ENDIF.
      IF <ls_changed_by>-name IS INITIAL.
        <ls_changed_by>-name = zcl_abapgit_objects_super=>c_user_unknown.
      ENDIF.
    ENDLOOP.

    INSERT LINES OF lt_changed_by_remote INTO TABLE rt_changed_by.

  ENDMETHOD.


  METHOD get_transportable_transports.

    TYPES: BEGIN OF ty_transportable_item,
             obj_type TYPE e071-object,
             obj_name TYPE e071-obj_name,
           END OF ty_transportable_item.
    TYPES ty_transportable_items TYPE SORTED TABLE OF ty_transportable_item WITH UNIQUE KEY obj_type obj_name.

    DATA lt_transportable_items TYPE ty_transportable_items.
    DATA ls_transportable_item LIKE LINE OF lt_transportable_items.
    DATA lt_db_transports TYPE zif_abapgit_cts_api=>ty_transport_list.
    DATA ls_transport LIKE LINE OF rt_transports.

    FIELD-SYMBOLS <ls_item> LIKE LINE OF it_items.
    FIELD-SYMBOLS <ls_db_transport> LIKE LINE OF lt_db_transports.

    LOOP AT it_items ASSIGNING <ls_item> WHERE obj_type IS NOT INITIAL AND obj_name IS NOT INITIAL.
      ls_transportable_item-obj_type = <ls_item>-obj_type.
      ls_transportable_item-obj_name = <ls_item>-obj_name.
      INSERT ls_transportable_item INTO TABLE lt_transportable_items.
    ENDLOOP.

    CHECK lt_transportable_items IS NOT INITIAL.

    SELECT a~trkorr b~object AS obj_type b~obj_name
      FROM e070 AS a INNER JOIN e071 AS b ON a~trkorr = b~trkorr
      INTO CORRESPONDING FIELDS OF TABLE lt_db_transports
      FOR ALL ENTRIES IN lt_transportable_items
      WHERE ( a~trstatus = 'D' OR a~trstatus = 'L' )
        AND a~trfunction <> 'G'
        AND NOT ( a~trfunction = 'F' AND ( a~tarsystem = '' OR a~tarsystem = 'SAP' ) )
        AND b~pgmid = 'R3TR'
        AND b~object = lt_transportable_items-obj_type
        AND b~obj_name = lt_transportable_items-obj_name.

    LOOP AT lt_db_transports ASSIGNING <ls_db_transport>.
      READ TABLE rt_transports INTO ls_transport WITH KEY
        obj_type = <ls_db_transport>-obj_type
        obj_name = <ls_db_transport>-obj_name.
      IF sy-subrc <> 0.
        INSERT <ls_db_transport> INTO TABLE rt_transports.
      ELSEIF ls_transport-trkorr <> <ls_db_transport>-trkorr.
        ls_transport-trkorr = zif_abapgit_definitions=>c_multiple_transports.
        MODIFY TABLE rt_transports FROM ls_transport.
      ENDIF.
    ENDLOOP.

  ENDMETHOD.


  METHOD propose_default_body.

    DATA lt_docu TYPE STANDARD TABLE OF ty_lock_info WITH EMPTY KEY.

    lt_docu = it_lock_info.
    SORT lt_docu
         BY current_user DESCENDING    " Own before foreign
            status       ASCENDING     " Open before released
            task         DESCENDING.   " Newest before oldest
    DELETE ADJACENT DUPLICATES FROM lt_docu
           COMPARING current_user status task.

    " We propose the task documentation of all tasks in the current staging
    CLEAR r_body.

    LOOP AT lt_docu INTO DATA(ls_docu).

      DATA(docu) = get_task_docu(
        ls_docu-task ).

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

    DATA lt_docu TYPE STANDARD TABLE OF ty_lock_info WITH EMPTY KEY.

    lt_docu = it_lock_info.
    SORT lt_docu
         BY current_user DESCENDING    " Own before foreign
            status       ASCENDING     " Open before released
            task         DESCENDING.   " Newest before oldest

    READ TABLE lt_docu INTO DATA(ls_docu)
         BINARY SEARCH
         WITH KEY current_user = abap_true
                  status       = 'D'.
    IF sy-subrc = 0.
      " We found the latest open task of this user: Best Match
      r_comment = get_lock_text(
        ls_docu ).
      RETURN.
    ENDIF.

    READ TABLE lt_docu INTO ls_docu
         BINARY SEARCH
         WITH KEY current_user = abap_true.
    IF sy-subrc = 0.
      " We found the latest released task of this user: Propose
      r_comment = get_lock_text(
        ls_docu ).
      RETURN.
    ENDIF.

    r_comment = get_lock_text(
                    lt_docu[
                        1 ] ).

  ENDMETHOD.


  METHOD propose_default_texts.

    TYPES: BEGIN OF ty_userdata,
             bname      TYPE xubname,
             name_first TYPE ad_namefir,
             name_last  TYPE ad_namelas,
             smtp_addr  TYPE ad_smtpadr,
           END OF ty_userdata,
           tty_user_data TYPE STANDARD TABLE OF ty_userdata WITH EMPTY KEY.

    DATA fixdate TYPE d VALUE '00010101'.

    DATA(lo_repo) = CAST zcl_abapgit_repo_online( io_repo ).

    DATA(lt_staged) = it_staged.
    LOOP AT lt_staged ASSIGNING FIELD-SYMBOL(<ls_staged>).
      TRY.
          zcl_abapgit_filename_logic=>file_to_object(
            EXPORTING
              iv_filename = <ls_staged>-file-filename
              iv_path     = <ls_staged>-file-path
              io_dot      = lo_repo->get_dot_abapgit( )
            IMPORTING
              es_item     = DATA(ls_item) ).
        CATCH zcx_abapgit_exception. " abapGit - Exception
          ls_item = CORRESPONDING #( <ls_staged>-status ).
      ENDTRY.
      <ls_staged>-status-obj_type = ls_item-obj_type.
      <ls_staged>-status-obj_name = ls_item-obj_name.
    ENDLOOP.

    DATA(lt_lock_info) = get_lock_info(
      it_staged = lt_staged ).

    IF lt_lock_info IS INITIAL.
      RETURN.
    ENDIF.

    " Propose Comment
    DATA(default_comment) = propose_default_comment(
      lt_lock_info ).
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

    " Propose Body
    cs_commit-body = COND #( WHEN cs_commit-body IS NOT INITIAL
                             THEN |{ cs_commit-body }| &
                                  |{ cl_abap_char_utilities=>cr_lf }| &
                                  |{ cl_abap_char_utilities=>cr_lf }| &
                                  |--------------------------------------------------------------------------------| &
                                  |{ cl_abap_char_utilities=>cr_lf }| &
                                  |{ cl_abap_char_utilities=>cr_lf }| &
                                  |{ propose_default_body(
                                         lt_lock_info ) }|
                             ELSE propose_default_body( lt_lock_info ) ).
    TRY.
        io_form->set(
          iv_key = cs_formid-body
          iv_val = cs_commit-body ).
      CATCH cx_root.
    ENDTRY.

    DATA(lt_userdata) = VALUE tty_user_data( ).
    SELECT user~bname,
           name~name_first,
           name~name_last,
           addr~smtp_addr
      FROM usr21           AS user
           INNER JOIN adrp AS name ON  name~persnumber = user~persnumber
                                   AND name~date_from  = @fixdate
                                   AND name~nation     = ''
           INNER JOIN adr6 AS addr ON  addr~addrnumber = user~addrnumber
                                   AND addr~persnumber = user~persnumber
                                   AND addr~date_from  = @fixdate
      WHERE user~bname = @sy-uname
      ORDER BY consnumber DESCENDING
      INTO CORRESPONDING FIELDS OF TABLE @lt_userdata.
    DATA(ls_userdata) = VALUE #( lt_userdata[
                                     1 ]
                                 DEFAULT VALUE ty_userdata( ) ).

    " Propose Commiter Name
    IF    cs_commit-committer_name IS INITIAL
       OR cs_commit-committer_name  = sy-uname.
      cs_commit-committer_name = |{ ls_userdata-name_first } { ls_userdata-name_last }|.
      TRY.
          io_form->set(
            iv_key = cs_formid-committer_name
            iv_val = cs_commit-committer_name ).
        CATCH cx_root.
      ENDTRY.
    ENDIF.

    " Propose Commiter eMail
    IF cs_commit-committer_email IS INITIAL.
      cs_commit-committer_email = to_lower(
        ls_userdata-smtp_addr ).
      TRY.
          io_form->set(
            iv_key = cs_formid-committer_email
            iv_val = cs_commit-committer_email ).
        CATCH cx_root.
      ENDTRY.
    ENDIF.

  ENDMETHOD.


  METHOD supplement_task_info.

    DATA lt_transports TYPE SORTED TABLE OF trkorr WITH UNIQUE KEY table_line.
    DATA lt_tasks      TYPE STANDARD TABLE OF ty_lock_info WITH EMPTY KEY.

    " Supplement Transport Request/Task Lock Links in Comment

    DATA(lt_lock_info) = get_lock_info(
      it_staged = it_staged ).

    IF lt_lock_info IS INITIAL.
      RETURN.
    ENDIF.

    " Collect Distinct Transports
    LOOP AT lt_lock_info INTO DATA(ls_lock_info).
      INSERT ls_lock_info-transport INTO TABLE lt_transports.
    ENDLOOP.

    " Collect Distinct Tasks
    lt_tasks = lt_lock_info.
    SORT lt_tasks
         BY task.
    DELETE ADJACENT DUPLICATES FROM lt_tasks
           COMPARING task.

    IF lt_transports IS INITIAL AND lt_tasks IS INITIAL.
      RETURN.
    ENDIF.

    " Add Transport Locks
    DATA(tr_links) = VALUE string( ).
    LOOP AT lt_transports INTO DATA(trkorr).
      tr_links = SWITCH #( sy-tabix
                           WHEN 1
                           THEN trkorr
                           ELSE |, { trkorr }| ).
    ENDLOOP.
    SHIFT tr_links LEFT DELETING LEADING ', '.

    " Add Task Locks
    DATA(ta_links) = VALUE string( ).
    LOOP AT lt_tasks INTO DATA(ls_task).
      ta_links = SWITCH #( sy-tabix
                           WHEN 1
                           THEN ls_task-task
                           ELSE |, { ls_task-task }| ).
    ENDLOOP.
    SHIFT ta_links LEFT DELETING LEADING ', '.

    " Supplement Information to Comment
    cs_commit-comment = cs_commit-comment &&
                        | ({ tr_links } // { ta_links } )|.

  ENDMETHOD.


  METHOD get_open_user_requests.

    CONSTANTS c_SECSOFDAY TYPE i VALUE 86400.

    DATA lt_rt_function TYPE RANGE OF trfunction.
    DATA lt_rt_os4      TYPE RANGE OF abap_bool.

    IF i_tasks = abap_true.
      lt_rt_function = VALUE #( BASE lt_rt_function
                                sign   = 'I'
                                option = 'EQ'
                                ( low = 'S' ) ).
    ENDIF.

    IF i_requests = abap_true.
      lt_rt_function = VALUE #( BASE lt_rt_function
                                sign   = 'I'
                                option = 'EQ'
                                ( low = 'W' )
                                ( low = 'T' ) ).
    ENDIF.

    IF lt_rt_function IS INITIAL.
      " No Request Types set
      RETURN.
    ENDIF.

    GET TIME STAMP FIELD DATA(now).
    DATA(earliest) = now.
    IF i_recent_days > 0.
      TRY.
          earliest = cl_abap_tstmp=>subtractsecs_to_short(
            tstmp = now
            secs  = ( i_recent_days * c_secsofday ) ).
        CATCH cx_root.
          earliest = now.
      ENDTRY.
    ENDIF.

    " Special Handling: If we are working on the OS4 Repo we only propose use OS4 Requests/Task
    "   For other repos we exclude them
    IF i_eval_os4 = abap_true AND m_current_repo_key IS NOT INITIAL.
      TRY.
          DATA(lo_repo) = zcl_abapgit_repo_srv=>get_instance( )->get(
                              CONV #( m_current_repo_key ) ).
          IF lo_repo->ms_data-package = '/LOT/OS'.
            " OS4 Repo: Select OS4 Requests Only
            INSERT VALUE #( sign   = 'I'
                            option = 'EQ'
                            low    = 'X' ) INTO TABLE lt_rt_os4.
          ELSE.
            " Not an OS4 Repo: Select non-OS4 Requests Only
            INSERT VALUE #( sign   = 'I'
                            option = 'EQ'
                            low    = '' ) INTO TABLE lt_rt_os4.
          ENDIF.
        CATCH cx_root.
          CLEAR lt_rt_os4.
      ENDTRY.
    ENDIF.

    " Read Open and Recent User Tasks/Request
    SELECT * FROM ZPI_TransportRequests
      WHERE Function IN @lt_rt_function
        AND SystemId  = @sy-sysid
        AND UserName  = @sy-uname
        AND (    status IN ( 'D', 'L' )
              OR
                 (     status      IN ( 'R', 'O', 'P' )
                   AND LastChanged  > @earliest ) )
        AND IsOS4Request IN @lt_rt_os4
      ORDER BY LastChanged DESCENDING
      INTO TABLE @DATA(lt_requests).

    " Process
    LOOP AT lt_requests ASSIGNING FIELD-SYMBOL(<ls_request>).

      INSERT VALUE #( sign   = 'I'
                      option = 'EQ'
                      low    = <ls_request>-Request
       ) INTO TABLE rt_requests.

      IF     i_parent_request            = abap_true
         AND <ls_request>-ParentRequest IS NOT INITIAL.
        " If we are dealing with a task, and it is requested to also retrieve the parent
        INSERT VALUE #( sign   = 'I'
                        option = 'EQ'
                        low    = <ls_request>-ParentRequest
         ) INTO TABLE rt_requests.
      ENDIF.

    ENDLOOP.

  ENDMETHOD.


  METHOD popup_select_own_tr_requests.

    DATA lt_request  TYPE trwbo_request_headers.
    DATA lr_request  TYPE REF TO trwbo_request_header.
    DATA ls_r_trkorr TYPE LINE OF zif_abapgit_definitions=>ty_trrngtrkor_tt.

    DATA(ls_position) = zcl_abapgit_popups=>center(
      iv_width  = 120
      iv_height = 10 ).

    DATA(ls_selection) = is_selection.
    DATA(ls_ranges) = VALUE trsel_ts_ranges( trkorr = zcl_abapgit_cts_integration=>get_open_user_requests(
                                                          i_tasks          = abap_true
                                                          i_requests       = abap_false
                                                          i_parent_request = abap_false
                                                          i_recent_days    = 0 ) ).
    IF ls_ranges-trkorr IS NOT INITIAL.
      CLEAR ls_selection.
      ls_ranges-task_funcs     = VALUE #( sign   = 'I'
                                          option = 'EQ'
                                          ( low = 'K' )
                                          ( low = 'T' )
                                          ( low = 'R' )
                                          ( low = 'X' )
                                          ( low = 'S' )  ).
      ls_ranges-task_status    = VALUE #( sign   = 'I'
                                          option = 'EQ'
                                          ( low = 'R' )
                                          ( low = 'N' )
                                          ( low = 'O' )
                                          ( low = 'D' )
                                          ( low = 'L' )  ).
      ls_ranges-request_funcs  = ls_ranges-task_funcs.
      ls_ranges-request_status = ls_ranges-task_status.
    ENDIF.

    CALL FUNCTION 'TRINT_SELECT_REQUESTS'
      EXPORTING
        iv_username_pattern    = iv_username_pattern
        is_selection           = ls_selection
        iv_complete_projects   = abap_false
        is_popup               = ls_position
        iv_via_selscreen       = 'X'
        iv_title               = iv_title
      IMPORTING
        et_requests            = lt_request
      CHANGING
        cs_ranges              = ls_ranges
      EXCEPTIONS
        action_aborted_by_user = 1
        OTHERS                 = 2.
    IF sy-subrc <> 0.
      zcx_abapgit_exception=>raise(
          'Selection canceled' ).
    ENDIF.

    IF lt_request IS INITIAL.
      zcx_abapgit_exception=>raise(
          'No Request Found' ).
    ENDIF.

    IF lines( lt_request ) > 10000.
      zcx_abapgit_exception=>raise(
          'Too many requests selected (max 10000)' ).
    ENDIF.

    LOOP AT lt_request REFERENCE INTO lr_request.
      ls_r_trkorr-sign   = 'I'.
      ls_r_trkorr-option = 'EQ'.
      ls_r_trkorr-low    = lr_request->trkorr.
      INSERT ls_r_trkorr INTO TABLE rt_r_trkorr.
    ENDLOOP.

  ENDMETHOD.


  METHOD on_event.

    DATA(o_gui_event) = zcl_abapgit_gui_event=>new(
      iv_action   = action
      iv_getdata  = getdata
      it_postdata = postdata ).

    IF o_gui_event->zif_abapgit_gui_event~mv_action = zif_abapgit_definitions=>c_action-go_stage_transport.
      TRY.
          m_current_repo_key = o_gui_event->zif_abapgit_gui_event~query( )->get(
                                   'KEY' ).
        CATCH cx_root.
          CLEAR m_current_repo_key.
      ENDTRY.
    ENDIF.

  ENDMETHOD.
ENDCLASS.
