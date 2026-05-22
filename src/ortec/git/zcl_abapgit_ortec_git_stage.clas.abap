CLASS zcl_abapgit_ortec_git_stage DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    CONSTANTS c_default_window_size TYPE i VALUE 150.

    CLASS-METHODS is_virtual_active
      IMPORTING
        !iv_changed_file_count TYPE i
        !iv_load_all           TYPE abap_bool
        !iv_threshold          TYPE i
      RETURNING
        VALUE(rv_active)       TYPE abap_bool.

    CLASS-METHODS render_virtual_list
      IMPORTING
        !ii_repo        TYPE REF TO zif_abapgit_repo
        !it_files       TYPE zif_abapgit_definitions=>ty_stage_files
        !iv_window_size TYPE i DEFAULT c_default_window_size
        !iv_offset      TYPE i DEFAULT 0
        !iv_total_count TYPE i OPTIONAL
        !iv_prev_action TYPE string OPTIONAL
        !iv_next_action TYPE string OPTIONAL
        !iv_filter_value TYPE string OPTIONAL
      RETURNING
        VALUE(ri_html)  TYPE REF TO zif_abapgit_html
      RAISING
        zcx_abapgit_exception.

    CLASS-METHODS render_virtual_adapter_script
      IMPORTING
        !iv_window_size TYPE i DEFAULT c_default_window_size
      RETURNING
        VALUE(ri_html)  TYPE REF TO zif_abapgit_html.
  PROTECTED SECTION.
  PRIVATE SECTION.
    CLASS-METHODS find_transports
      IMPORTING
        !ii_repo             TYPE REF TO zif_abapgit_repo
        !it_files            TYPE zif_abapgit_definitions=>ty_stage_files
      RETURNING
        VALUE(rt_transports) TYPE zif_abapgit_cts_api=>ty_transport_list.

    CLASS-METHODS render_stage_data_json
      IMPORTING
        !ii_repo       TYPE REF TO zif_abapgit_repo
        !it_files      TYPE zif_abapgit_definitions=>ty_stage_files
        !it_transports TYPE zif_abapgit_cts_api=>ty_transport_list
        !it_changed_by TYPE zcl_abapgit_cts_integration=>ty_changed_by_tt
      RETURNING
        VALUE(rv_json) TYPE string
      RAISING
        zcx_abapgit_exception.

    CLASS-METHODS append_json_field
      IMPORTING
        !iv_name  TYPE string
        !iv_value TYPE csequence
        !iv_last  TYPE abap_bool OPTIONAL
      CHANGING
        !cv_json  TYPE string.

    CLASS-METHODS escape_json_string
      IMPORTING
        !iv_value         TYPE csequence
      RETURNING
        VALUE(rv_escaped) TYPE string.
ENDCLASS.



CLASS zcl_abapgit_ortec_git_stage IMPLEMENTATION.


  METHOD append_json_field.

    cv_json = cv_json && |"{ iv_name }":"{ escape_json_string( iv_value ) }"|.
    IF iv_last = abap_false.
      cv_json = cv_json && ','.
    ENDIF.

  ENDMETHOD.


  METHOD escape_json_string.

    rv_escaped = escape(
      val    = iv_value
      format = cl_abap_format=>e_json_string ).
    REPLACE ALL OCCURRENCES OF `</` IN rv_escaped WITH `<\/`.

  ENDMETHOD.


  METHOD find_transports.

    DATA li_cts_api TYPE REF TO zif_abapgit_cts_api.
    DATA lt_items TYPE zif_abapgit_definitions=>ty_items_tt.
    DATA ls_item TYPE zif_abapgit_definitions=>ty_item.
    DATA lo_dot TYPE REF TO zcl_abapgit_dot_abapgit.

    FIELD-SYMBOLS <ls_local> LIKE LINE OF it_files-local.
    FIELD-SYMBOLS <ls_remote> LIKE LINE OF it_files-remote.

    li_cts_api = zcl_abapgit_factory=>get_cts_api( ).

    TRY.
        LOOP AT it_files-local ASSIGNING <ls_local> WHERE item IS NOT INITIAL.
          IF li_cts_api->is_chrec_possible_for_package( <ls_local>-item-devclass ) = abap_false.
            RETURN.
          ENDIF.
          APPEND <ls_local>-item TO lt_items.
        ENDLOOP.

        lo_dot = ii_repo->get_dot_abapgit( ).
        LOOP AT it_files-remote ASSIGNING <ls_remote> WHERE filename IS NOT INITIAL.
          zcl_abapgit_filename_logic=>file_to_object(
            EXPORTING
              iv_filename = <ls_remote>-filename
              iv_path     = <ls_remote>-path
              io_dot      = lo_dot
            IMPORTING
              es_item     = ls_item ).
          IF ls_item IS INITIAL.
            CONTINUE.
          ENDIF.
          APPEND ls_item TO lt_items.
        ENDLOOP.

        SORT lt_items BY obj_type obj_name.
        DELETE ADJACENT DUPLICATES FROM lt_items COMPARING obj_type obj_name.

        rt_transports = li_cts_api->get_transports_for_list( lt_items ).

      CATCH zcx_abapgit_exception ##NO_HANDLER.
    ENDTRY.

  ENDMETHOD.


  METHOD is_virtual_active.

    rv_active = boolc( iv_changed_file_count > iv_threshold ).

  ENDMETHOD.


  METHOD render_stage_data_json.

    DATA lv_first TYPE abap_bool VALUE abap_true.
    DATA lv_key TYPE string.
    DATA lv_filename TYPE string.
    DATA lv_diff_action TYPE string.
    DATA lv_state_html TYPE string.
    DATA lv_transport_html TYPE string.
    DATA lv_changed_by_html TYPE string.
    DATA lv_user_action TYPE string.
    DATA ls_changed_by LIKE LINE OF it_changed_by.
    DATA ls_transport LIKE LINE OF it_transports.
    DATA ls_item_remote TYPE zif_abapgit_definitions=>ty_item.

    FIELD-SYMBOLS <ls_local> LIKE LINE OF it_files-local.
    FIELD-SYMBOLS <ls_remote> LIKE LINE OF it_files-remote.
    FIELD-SYMBOLS <ls_status> LIKE LINE OF it_files-status.

    rv_json = '['.

    LOOP AT it_files-local ASSIGNING <ls_local>.
      CLEAR: ls_changed_by, ls_transport, lv_diff_action, lv_transport_html, lv_changed_by_html, lv_user_action.
      READ TABLE it_files-status ASSIGNING <ls_status>
        WITH TABLE KEY path = <ls_local>-file-path filename = <ls_local>-file-filename.
      ASSERT sy-subrc = 0.
      READ TABLE it_changed_by INTO ls_changed_by WITH TABLE KEY
        item = <ls_local>-item filename = <ls_local>-file-filename.
      IF sy-subrc <> 0.
        READ TABLE it_changed_by INTO ls_changed_by WITH KEY item = <ls_local>-item.
      ENDIF.
      READ TABLE it_transports INTO ls_transport WITH KEY
        obj_type = <ls_local>-item-obj_type obj_name = <ls_local>-item-obj_name.

      lv_key = <ls_local>-file-path && <ls_local>-file-filename.
      lv_filename = lv_key.
      lv_diff_action = |{ zif_abapgit_definitions=>c_action-go_file_diff }?| &&
        zcl_abapgit_html_action_utils=>file_encode( iv_key = ii_repo->get_key( ) ig_file = <ls_local>-file ).
      lv_state_html = zcl_abapgit_gui_chunk_lib=>render_item_state(
        iv_lstate = <ls_status>-lstate iv_rstate = <ls_status>-rstate ).
      TRY.
          lv_changed_by_html = zcl_abapgit_gui_chunk_lib=>render_user_name(
            iv_username    = ls_changed_by-name
            iv_interactive = abap_false )->render( ).
        CATCH zcx_abapgit_exception ##NO_HANDLER.
      ENDTRY.
      IF ls_changed_by-name IS NOT INITIAL.
        lv_user_action = |{ zif_abapgit_definitions=>c_action-jump_user }?user={ ls_changed_by-name }|.
      ENDIF.
      TRY.
          lv_transport_html = zcl_abapgit_gui_chunk_lib=>render_transport(
            iv_transport = ls_transport-trkorr
            iv_obj_type  = <ls_local>-item-obj_type
            iv_obj_name  = <ls_local>-item-obj_name )->render( ).
        CATCH zcx_abapgit_exception ##NO_HANDLER.
      ENDTRY.

      IF lv_first = abap_true.
        lv_first = abap_false.
      ELSE.
        rv_json = rv_json && ','.
      ENDIF.
      rv_json = rv_json && '{'.
      append_json_field( EXPORTING iv_name = 'key' iv_value = lv_key CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'context' iv_value = 'local' CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'path' iv_value = <ls_local>-file-path CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'filename' iv_value = <ls_local>-file-filename CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'objType' iv_value = <ls_local>-item-obj_type CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'objName' iv_value = <ls_local>-item-obj_name CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'displayName' iv_value = lv_filename CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'diffAction' iv_value = lv_diff_action CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'changedBy' iv_value = ls_changed_by-name CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'changedByHtml' iv_value = lv_changed_by_html CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'userAction' iv_value = lv_user_action CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'transport' iv_value = ls_transport-trkorr CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'lstate' iv_value = <ls_status>-lstate CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'rstate' iv_value = <ls_status>-rstate CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'stateHtml' iv_value = lv_state_html CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'transportHtml' iv_value = lv_transport_html CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'defaultMethod' iv_value = zif_abapgit_definitions=>c_method-add iv_last = abap_true CHANGING cv_json = rv_json ).
      rv_json = rv_json && '}'.
    ENDLOOP.

    LOOP AT it_files-remote ASSIGNING <ls_remote>.
      CLEAR: ls_changed_by, ls_transport, ls_item_remote, lv_diff_action, lv_transport_html, lv_changed_by_html, lv_user_action.
      READ TABLE it_files-status ASSIGNING <ls_status>
        WITH TABLE KEY path = <ls_remote>-path filename = <ls_remote>-filename.
      ASSERT sy-subrc = 0.
      TRY.
          zcl_abapgit_filename_logic=>file_to_object(
            EXPORTING iv_filename = <ls_remote>-filename iv_path = <ls_remote>-path io_dot = ii_repo->get_dot_abapgit( )
            IMPORTING es_item = ls_item_remote ).
        CATCH zcx_abapgit_exception ##NO_HANDLER.
      ENDTRY.
      READ TABLE it_transports INTO ls_transport WITH KEY
        obj_type = ls_item_remote-obj_type obj_name = ls_item_remote-obj_name.
      READ TABLE it_changed_by INTO ls_changed_by WITH TABLE KEY
        item = ls_item_remote filename = <ls_remote>-filename.
      IF sy-subrc <> 0.
        READ TABLE it_changed_by INTO ls_changed_by WITH KEY item = ls_item_remote.
      ENDIF.

      lv_key = <ls_remote>-path && <ls_remote>-filename.
      lv_filename = lv_key.
      lv_state_html = zcl_abapgit_gui_chunk_lib=>render_item_state(
        iv_lstate = <ls_status>-lstate iv_rstate = <ls_status>-rstate ).
      TRY.
          lv_changed_by_html = zcl_abapgit_gui_chunk_lib=>render_user_name(
            iv_username    = ls_changed_by-name
            iv_interactive = abap_false )->render( ).
        CATCH zcx_abapgit_exception ##NO_HANDLER.
      ENDTRY.
      IF ls_changed_by-name IS NOT INITIAL.
        lv_user_action = |{ zif_abapgit_definitions=>c_action-jump_user }?user={ ls_changed_by-name }|.
      ENDIF.
      TRY.
          lv_transport_html = zcl_abapgit_gui_chunk_lib=>render_transport(
            iv_transport = ls_transport-trkorr
            iv_obj_type  = ls_item_remote-obj_type
            iv_obj_name  = ls_item_remote-obj_name )->render( ).
        CATCH zcx_abapgit_exception ##NO_HANDLER.
      ENDTRY.

      IF lv_first = abap_true.
        lv_first = abap_false.
      ELSE.
        rv_json = rv_json && ','.
      ENDIF.
      rv_json = rv_json && '{'.
      append_json_field( EXPORTING iv_name = 'key' iv_value = lv_key CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'context' iv_value = 'remote' CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'path' iv_value = <ls_remote>-path CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'filename' iv_value = <ls_remote>-filename CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'objType' iv_value = ls_item_remote-obj_type CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'objName' iv_value = ls_item_remote-obj_name CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'displayName' iv_value = lv_filename CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'diffAction' iv_value = lv_diff_action CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'changedBy' iv_value = ls_changed_by-name CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'changedByHtml' iv_value = lv_changed_by_html CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'userAction' iv_value = lv_user_action CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'transport' iv_value = ls_transport-trkorr CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'lstate' iv_value = <ls_status>-lstate CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'rstate' iv_value = <ls_status>-rstate CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'stateHtml' iv_value = lv_state_html CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'transportHtml' iv_value = lv_transport_html CHANGING cv_json = rv_json ).
      append_json_field( EXPORTING iv_name = 'defaultMethod' iv_value = zif_abapgit_definitions=>c_method-rm iv_last = abap_true CHANGING cv_json = rv_json ).
      rv_json = rv_json && '}'.
    ENDLOOP.

    rv_json = rv_json && ']'.

  ENDMETHOD.


  METHOD render_virtual_list.

    DATA ls_filtered TYPE zif_abapgit_definitions=>ty_stage_files.
    DATA ls_window TYPE zif_abapgit_definitions=>ty_stage_files.
    DATA ls_status LIKE LINE OF it_files-status.
    DATA ls_item TYPE zif_abapgit_definitions=>ty_item.
    DATA lv_pattern TYPE string.
    DATA lv_total_count TYPE i.
    DATA lv_offset TYPE i.
    DATA lv_end TYPE i.
    DATA lv_local_count TYPE i.
    DATA lv_from TYPE i.
    DATA lv_to TYPE i.
    DATA lt_transports TYPE zif_abapgit_cts_api=>ty_transport_list.
    DATA lt_changed_by TYPE zcl_abapgit_cts_integration=>ty_changed_by_tt.
    DATA lt_trkorr TYPE zif_abapgit_cts_api=>ty_trkorr_tt.
    DATA lv_json TYPE string.
    DATA lt_filter_transports TYPE zif_abapgit_cts_api=>ty_transport_list.
    DATA lt_filter_changed_by TYPE zcl_abapgit_cts_integration=>ty_changed_by_tt.
    DATA ls_filter_transport LIKE LINE OF lt_filter_transports.
    DATA ls_filter_changed_by LIKE LINE OF lt_filter_changed_by.

    FIELD-SYMBOLS <ls_local> LIKE LINE OF it_files-local.
    FIELD-SYMBOLS <ls_remote> LIKE LINE OF it_files-remote.

    IF iv_filter_value IS INITIAL.
      ls_filtered = it_files.
    ELSE.
      lv_pattern = '*' && to_upper( iv_filter_value ) && '*'.

      " Pre-fetch transports and changed-by for all files so they can be included in filter matching.
      lt_filter_transports = find_transports( ii_repo = ii_repo it_files = it_files ).
      DATA(lt_filter_trkorr_pf) = VALUE zif_abapgit_cts_api=>ty_trkorr_tt(
        FOR ls_t IN lt_filter_transports ( ls_t-trkorr ) ).
      SORT lt_filter_trkorr_pf.
      DELETE ADJACENT DUPLICATES FROM lt_filter_trkorr_pf.
      zcl_abapgit_factory=>get_cts_api( )->prefetch_descriptions( lt_filter_trkorr_pf ).
      lt_filter_changed_by = zcl_abapgit_cts_integration=>find_changed_by(
        ii_repo       = ii_repo
        it_files      = it_files
        it_transports = lt_filter_transports ).

      LOOP AT it_files-local ASSIGNING <ls_local>.
        CLEAR: ls_filter_transport, ls_filter_changed_by.
        READ TABLE lt_filter_transports INTO ls_filter_transport
          WITH KEY obj_type = <ls_local>-item-obj_type obj_name = <ls_local>-item-obj_name.
        READ TABLE lt_filter_changed_by INTO ls_filter_changed_by
          WITH TABLE KEY item = <ls_local>-item filename = <ls_local>-file-filename.
        IF sy-subrc <> 0.
          READ TABLE lt_filter_changed_by INTO ls_filter_changed_by
            WITH KEY item = <ls_local>-item.
        ENDIF.
        IF to_upper( <ls_local>-item-obj_type ) CP lv_pattern
            OR to_upper( <ls_local>-item-obj_name ) CP lv_pattern
            OR to_upper( <ls_local>-item-devclass ) CP lv_pattern
            OR to_upper( <ls_local>-file-path ) CP lv_pattern
            OR to_upper( <ls_local>-file-filename ) CP lv_pattern
            OR to_upper( ls_filter_transport-trkorr ) CP lv_pattern
            OR to_upper( ls_filter_changed_by-name ) CP lv_pattern.
          APPEND <ls_local> TO ls_filtered-local.
          READ TABLE it_files-status INTO ls_status
            WITH TABLE KEY path = <ls_local>-file-path filename = <ls_local>-file-filename.
          IF sy-subrc = 0.
            INSERT ls_status INTO TABLE ls_filtered-status.
          ENDIF.
        ENDIF.
      ENDLOOP.

      LOOP AT it_files-remote ASSIGNING <ls_remote>.
        CLEAR: ls_item, ls_filter_transport, ls_filter_changed_by.
        TRY.
            zcl_abapgit_filename_logic=>file_to_object(
              EXPORTING
                iv_filename = <ls_remote>-filename
                iv_path     = <ls_remote>-path
                io_dot      = ii_repo->get_dot_abapgit( )
              IMPORTING
                es_item     = ls_item ).
          CATCH zcx_abapgit_exception ##NO_HANDLER.
        ENDTRY.
        READ TABLE lt_filter_transports INTO ls_filter_transport
          WITH KEY obj_type = ls_item-obj_type obj_name = ls_item-obj_name.
        READ TABLE lt_filter_changed_by INTO ls_filter_changed_by
          WITH TABLE KEY item = ls_item filename = <ls_remote>-filename.
        IF sy-subrc <> 0.
          READ TABLE lt_filter_changed_by INTO ls_filter_changed_by
            WITH KEY item = ls_item.
        ENDIF.

        IF to_upper( <ls_remote>-path ) CP lv_pattern
            OR to_upper( <ls_remote>-filename ) CP lv_pattern
            OR to_upper( ls_item-obj_type ) CP lv_pattern
            OR to_upper( ls_item-obj_name ) CP lv_pattern
            OR to_upper( ls_filter_transport-trkorr ) CP lv_pattern
            OR to_upper( ls_filter_changed_by-name ) CP lv_pattern.
          APPEND <ls_remote> TO ls_filtered-remote.
          READ TABLE it_files-status INTO ls_status
            WITH TABLE KEY path = <ls_remote>-path filename = <ls_remote>-filename.
          IF sy-subrc = 0.
            INSERT ls_status INTO TABLE ls_filtered-status.
          ENDIF.
        ENDIF.
      ENDLOOP.
    ENDIF.

    lv_total_count = lines( ls_filtered-local ) + lines( ls_filtered-remote ).
    IF iv_total_count IS NOT INITIAL AND iv_filter_value IS INITIAL.
      lv_total_count = iv_total_count.
    ENDIF.
    lv_offset = nmax( val1 = 0 val2 = iv_offset ).
    lv_end = nmin( val1 = lv_offset + iv_window_size val2 = lv_total_count ).
    lv_local_count = lines( ls_filtered-local ).

    IF lv_offset < lv_local_count.
      lv_from = lv_offset + 1.
      lv_to = nmin( val1 = lv_end val2 = lv_local_count ).
      LOOP AT ls_filtered-local ASSIGNING <ls_local> FROM lv_from TO lv_to.
        APPEND <ls_local> TO ls_window-local.
        READ TABLE ls_filtered-status INTO ls_status
          WITH TABLE KEY path = <ls_local>-file-path filename = <ls_local>-file-filename.
        IF sy-subrc = 0.
          INSERT ls_status INTO TABLE ls_window-status.
        ENDIF.
      ENDLOOP.
    ENDIF.

    IF lv_end > lv_local_count.
      lv_from = nmax( val1 = 1 val2 = lv_offset - lv_local_count + 1 ).
      lv_to = lv_end - lv_local_count.
      LOOP AT ls_filtered-remote ASSIGNING <ls_remote> FROM lv_from TO lv_to.
        APPEND <ls_remote> TO ls_window-remote.
        READ TABLE ls_filtered-status INTO ls_status
          WITH TABLE KEY path = <ls_remote>-path filename = <ls_remote>-filename.
        IF sy-subrc = 0.
          INSERT ls_status INTO TABLE ls_window-status.
        ENDIF.
      ENDLOOP.
    ENDIF.

    lt_transports = find_transports( ii_repo = ii_repo it_files = ls_window ).
    lt_trkorr = VALUE zif_abapgit_cts_api=>ty_trkorr_tt(
      FOR ls_transport IN lt_transports ( ls_transport-trkorr ) ).
    SORT lt_trkorr.
    DELETE ADJACENT DUPLICATES FROM lt_trkorr.
    zcl_abapgit_factory=>get_cts_api( )->prefetch_descriptions( lt_trkorr ).

    lt_changed_by = zcl_abapgit_cts_integration=>find_changed_by(
      ii_repo       = ii_repo
      it_files      = ls_window
      it_transports = lt_transports ).

    lv_json = render_stage_data_json(
      ii_repo       = ii_repo
      it_files      = ls_window
      it_transports = lt_transports
      it_changed_by = lt_changed_by ).

    CREATE OBJECT ri_html TYPE zcl_abapgit_html.
    ri_html->add( '<div id="stageVirtualPager" class="stage-virtual-pager margin-v5" style="display:flex;justify-content:space-between;align-items:center;">' ).
    ri_html->add( '<span id="stageVirtualInfo" class="pad-sides"></span>' ).
    ri_html->add( '<span class="stage-virtual-nav">' ).
    IF lv_offset > 0 AND iv_prev_action IS NOT INITIAL.
      ri_html->add( |&#x25C0;&nbsp;<a id="stageVirtualPrev" href="sapevent:{ iv_prev_action }">Previous</a>| ).
    ELSE.
      ri_html->add( '&#x25C0;&nbsp;<span id="stageVirtualPrev" class="grey">Previous</span>' ).
    ENDIF.
    ri_html->add( '&nbsp;&nbsp;' ).
    IF lv_end < lv_total_count AND iv_next_action IS NOT INITIAL.
      ri_html->add( |<a id="stageVirtualNext" href="sapevent:{ iv_next_action }">Next</a>&nbsp;&#x25B6;| ).
    ELSE.
      ri_html->add( '<span id="stageVirtualNext" class="grey">Next</span>&nbsp;&#x25B6;' ).
    ENDIF.
    ri_html->add( '</span>' ).
    ri_html->add( '</div>' ).
    ri_html->add( '<table id="stageTab" class="stage_tab w100">' ).
    ri_html->add( '<thead><tr class="local">' ).
    ri_html->add( '<th class="stage-status"></th>' ).
    ri_html->add( '<th class="stage-objtype">Type</th>' ).
    ri_html->add( '<th title="Click filename to see diff">File</th>' ).
    ri_html->add( '<th style="width:10em">Changed by <a href="#" id="stageFilterByMe" title="Filter to my changes">(me)</a></th>' ).
    ri_html->add( '<th style="width:12em">Transport</th>' ).
    ri_html->add( '<th style="width:3em"></th>' ).
    ri_html->add( '<th class="cmd" style="width:12em">Command</th>' ).
    ri_html->add( '</tr></thead>' ).
    ri_html->add( '<tbody id="stageVirtualBody"><tr><td colspan="7">Loading stage data...</td></tr></tbody>' ).
    ri_html->add( '</table>' ).
    ri_html->add( '<script>' ).
    ri_html->add( |window.OrtecStageRows = { lv_json };| ).
    ri_html->add( 'window.OrtecStagePage = {' ).
    ri_html->add( |  offset: { lv_offset },| ).
    ri_html->add( |  end: { lv_end },| ).
    ri_html->add( |  total: { lv_total_count },| ).
    ri_html->add( |  windowSize: { iv_window_size }| ).
    ri_html->add( '};' ).
    ri_html->add( 'window.OrtecStageMethods = {' ).
    ri_html->add( |  add: "{ zif_abapgit_definitions=>c_method-add }",| ).
    ri_html->add( |  remove: "{ zif_abapgit_definitions=>c_method-rm }",| ).
    ri_html->add( |  ignore: "{ zif_abapgit_definitions=>c_method-ignore }",| ).
    ri_html->add( |  skip: "{ zif_abapgit_definitions=>c_method-skip }"| ).
    ri_html->add( '};' ).
    ri_html->add( '</script>' ).

  ENDMETHOD.


  METHOD render_virtual_adapter_script.

    CREATE OBJECT ri_html TYPE zcl_abapgit_html.
    ri_html->set_title( 'ZCL_ABAPGIT_ORTEC_GIT_STAGE virtual adapter' ).

    ri_html->add( 'if (window.gStageParams && gStageParams.virtualActive && window.OrtecStageRows) {' ).
    ri_html->add( '  window.OrtecStageVirtualReady = true;' ).
    ri_html->add( '  (function(){' ).
    ri_html->add( '  try {' ).
    ri_html->add( '    var rows = window.OrtecStageRows || [];' ).
    ri_html->add( '    var pageMeta = window.OrtecStagePage || { offset: 0, end: rows.length, total: rows.length };' ).
    ri_html->add( '    var filtered = rows.slice(0);' ).
    ri_html->add( '    var methods = window.OrtecStageMethods || {};' ).
    ri_html->add( '    var pageSize = gStageParams.virtualWindowSize || 150;' ).
    ri_html->add( '    var page = 0, filterTimer = null, store = "ortec.stage." + (gStageParams.seed || "default");' ).
    ri_html->add( '    function load(){try{return JSON.parse(sessionStorage.getItem(store)||"{}")}catch(e){return {}}}' ).
    ri_html->add( '    var state = load();' ).
    ri_html->add( '    function save(){try{sessionStorage.setItem(store,JSON.stringify(state))}catch(e){}}' ).
    ri_html->add( '    function id(name) { return document.getElementById(name); }' ).
    ri_html->add( '    function addCell(row, value) { var cell = document.createElement("td"); cell.appendChild(document.createTextNode(value || "")); row.appendChild(cell); return cell; }' ).
    ri_html->add( '    function addAction(cell, text, method) { var link = document.createElement("a"); link.href = "#"; link.setAttribute("data-method", method); link.appendChild(document.createTextNode(text)); cell.appendChild(link); }' ).
    ri_html->add( '    function selectedCount() { var n = 0, key; for (key in state) { if (state.hasOwnProperty(key)) { n++; } } return n; }' ).
    ri_html->add( '    function methodText(method) { if (method === methods.add) { return "add"; } if (method === methods.remove) { return "remove"; } if (method === methods.ignore) { return "ignore"; } return method || ""; }' ).
    ri_html->add( '    function rowText(row) { return [row.objType, row.objName, row.displayName, row.changedBy, row.transport].join(" ").toUpperCase(); }' ).
    ri_html->add( '    function submitVirtualFilter() {' ).
    ri_html->add( '      var input = id(gStageParams.ids.objectSearch);' ).
    ri_html->add( '      var form = document.createElement("form");' ).
    ri_html->add( '      var field = document.createElement("input");' ).
    ri_html->add( '      form.method = "post"; form.action = "sapevent:" + (gStageParams.virtualFilterAction || "stage_virtual_filter");' ).
    ri_html->add( '      field.type = "hidden"; field.name = "filterValue"; field.value = input ? input.value : "";' ).
    ri_html->add( '      form.appendChild(field); document.body.appendChild(form); form.submit();' ).
    ri_html->add( '    }' ).
    ri_html->add( '    function applyFilter() {' ).
    ri_html->add( '      if (filterTimer) { clearTimeout(filterTimer); }' ).
    ri_html->add( '      filterTimer = setTimeout(submitVirtualFilter, 600);' ).
    ri_html->add( '    }' ).
    ri_html->add( '    function updateButtons() {' ).
    ri_html->add( '      var n = selectedCount();' ).
    ri_html->add( '      var selectedButton = id(gStageParams.ids.commitSelectedBtn);' ).
    ri_html->add( '      var filteredButton = id(gStageParams.ids.commitFilteredBtn);' ).
    ri_html->add( '      var allButton = id(gStageParams.ids.commitAllBtn);' ).
    ri_html->add( '      if (n > 0) {' ).
    ri_html->add( '        /* Items selected: show Commit Selected only */' ).
    ri_html->add( '        if (selectedButton) { selectedButton.style.display = ""; }' ).
    ri_html->add( '        if (selectedButton && selectedButton.querySelector(".counter")) { selectedButton.querySelector(".counter").innerHTML = n; }' ).
    ri_html->add( '        if (filteredButton) { filteredButton.style.display = "none"; }' ).
    ri_html->add( '        if (allButton) { allButton.style.display = "none"; }' ).
    ri_html->add( '      } else {' ).
    ri_html->add( '        /* Nothing selected: show Add Filtered and Add All, hide Commit Selected */' ).
    ri_html->add( '        if (selectedButton) { selectedButton.style.display = "none"; }' ).
    ri_html->add( '        if (filteredButton) { filteredButton.style.display = filtered.length ? "" : "none"; }' ).
    ri_html->add( '        if (filteredButton && filteredButton.querySelector(".counter")) { filteredButton.querySelector(".counter").innerHTML = filtered.length; }' ).
    ri_html->add( '        if (allButton) { allButton.style.display = ""; }' ).
    ri_html->add( '      }' ).
    ri_html->add( '    }' ).
    ri_html->add( '    function addSectionHeader(body, context) {' ).
    ri_html->add( '      var row = document.createElement("tr"); row.className = context;' ).
    ri_html->add( '      function th(cls, span) { var t = document.createElement("th"); if (cls) { t.className = cls; } if (span) { t.colSpan = span; } return t; }' ).
    ri_html->add( '      if (context === "local") {' ).
    ri_html->add( '        row.appendChild(th("stage-status")); row.appendChild(th("stage-objtype"));' ).
    ri_html->add( '        var fileCell = th(); fileCell.appendChild(document.createTextNode("Local changes")); row.appendChild(fileCell);' ).
    ri_html->add( '        row.appendChild(th()); row.appendChild(th()); row.appendChild(th());' ).
    ri_html->add( '        var cmdCell = th("cmd"); cmdCell.setAttribute("data-bulk-context", context);' ).
    ri_html->add( '        addAction(cmdCell, "add\u2193", methods.add); cmdCell.appendChild(document.createTextNode(" "));' ).
    ri_html->add( '        addAction(cmdCell, "reset\u2193", ""); row.appendChild(cmdCell);' ).
    ri_html->add( '      } else {' ).
    ri_html->add( '        row.appendChild(th()); row.appendChild(th());' ).
    ri_html->add( '        var fileCell = th(null, 3); fileCell.appendChild(document.createTextNode("Files to remove or non-code")); row.appendChild(fileCell);' ).
    ri_html->add( '        row.appendChild(th());' ).
    ri_html->add( '        var cmdCell = th("cmd"); cmdCell.setAttribute("data-bulk-context", context);' ).
    ri_html->add( '        addAction(cmdCell, "ignore\u2193", methods.ignore); cmdCell.appendChild(document.createTextNode(" "));' ).
    ri_html->add( '        addAction(cmdCell, "remove\u2193", methods.remove); cmdCell.appendChild(document.createTextNode(" "));' ).
    ri_html->add( '        addAction(cmdCell, "reset\u2193", ""); row.appendChild(cmdCell);' ).
    ri_html->add( '      }' ).
    ri_html->add( '      body.appendChild(row);' ).
    ri_html->add( '    }' ).
    ri_html->add( '    function render() {' ).
    ri_html->add( '      var body = id("stageVirtualBody"); if (!body) { return; }' ).
    ri_html->add( '      while (body.firstChild) { body.removeChild(body.firstChild); }' ).
    ri_html->add( '      var start = page * pageSize; var end = Math.min(start + pageSize, filtered.length);' ).
    ri_html->add( '      var lastContext = "";' ).
    ri_html->add( '      for (var i = start; i < end; i++) {' ).
    ri_html->add( '        var data = filtered[i]; var row = document.createElement("tr"); row.className = data.context; row.setAttribute("data-key", data.key);' ).
    ri_html->add( '        if (data.context !== lastContext) { addSectionHeader(body, data.context); lastContext = data.context; }' ).
    ri_html->add( '        var stateCell = document.createElement("td"); stateCell.innerHTML = data.stateHtml || ""; row.appendChild(stateCell);' ).
    ri_html->add( '        var typeCell = document.createElement("td"); typeCell.className = "type"; typeCell.appendChild(document.createTextNode(data.objType || "")); row.appendChild(typeCell);' ).
    ri_html->add( '        var nameCell = document.createElement("td"); nameCell.className = "name";' ).
    ri_html->add( '        if (data.context === "local" && data.diffAction) {' ).
    ri_html->add( '          var nLink = document.createElement("a"); nLink.href = "sapevent:" + data.diffAction;' ).
    ri_html->add( '          nLink.appendChild(document.createTextNode(data.displayName || "")); nameCell.appendChild(nLink);' ).
    ri_html->add( '        } else { nameCell.appendChild(document.createTextNode(data.displayName || "")); }' ).
    ri_html->add( '        row.appendChild(nameCell);' ).
    ri_html->add( '        var userCell = document.createElement("td"); userCell.className = "user";' ).
    ri_html->add( '        userCell.innerHTML = data.changedByHtml || data.changedBy || "";' ).
    ri_html->add( '        if (data.userAction) { userCell.style.cursor = "pointer";' ).
    ri_html->add( '          (function(cell, act) { cell.onclick = function(e) {' ).
    ri_html->add( '            e = e || window.event; if (e.stopPropagation) { e.stopPropagation(); }' ).
    ri_html->add( '            e.cancelBubble = true; window.location.href = "sapevent:" + act; return false;' ).
    ri_html->add( '          }; })(userCell, data.userAction); }' ).
    ri_html->add( '        row.appendChild(userCell);' ).
    ri_html->add( '        var transportCell = document.createElement("td"); transportCell.className = "transport"; transportCell.innerHTML = data.transportHtml || data.transport || ""; row.appendChild(transportCell);' ).
    ri_html->add( '        var selected = state[data.key] || "";' ).
    ri_html->add( '        var status = document.createElement("td"); status.className = "status"; row.appendChild(status);' ).
    ri_html->add( '        var cmd = document.createElement("td"); cmd.className = "cmd"; row.appendChild(cmd);' ).
    ri_html->add( '        if (selected) { status.innerHTML = "<span style=\"background-color:#ddd;color:#000;font-weight:bold;padding:1px 4px\">" + selected + "</span>"; } else { status.appendChild(document.createTextNode("?")); }' ).
    ri_html->add( '        if (selected) { addAction(cmd, "reset", ""); }' ).
    ri_html->add( '        else if (data.context === "local") { addAction(cmd, "add", methods.add); }' ).
    ri_html->add( '        else { addAction(cmd, "ignore", methods.ignore); cmd.appendChild(document.createTextNode(" ")); addAction(cmd, "remove", methods.remove); }' ).
    ri_html->add( '        body.appendChild(row);' ).
    ri_html->add( '      }' ).
    ri_html->add( '      if (!filtered.length) { var empty = document.createElement("tr"); var emptyCell = addCell(empty, "No files match the current filter"); emptyCell.colSpan = 7; body.appendChild(empty); }' ).
    ri_html->add( '      var info = id("stageVirtualInfo"); if (info) { info.innerHTML = "Showing " + (filtered.length ? pageMeta.offset + start + 1 : 0) + "-" + (pageMeta.offset + end) + " of " + pageMeta.total + " files"; }' ).
    ri_html->add( '      var prevButton = id("stageVirtualPrev"); if (prevButton) { prevButton.disabled = page <= 0; }' ).
    ri_html->add( '      var nextButton = id("stageVirtualNext"); if (nextButton) { nextButton.disabled = end >= filtered.length; }' ).
    ri_html->add( '      updateButtons();' ).
    ri_html->add( '    }' ).
    ri_html->add( '    function submit(action) {' ).
    ri_html->add( '      var form = id("form_" + gStageParams.formAction);' ).
    ri_html->add( '      if (!form) { return; }' ).
    ri_html->add( '      form.innerHTML = "";' ).
    ri_html->add( '      var n = 0, key;' ).
    ri_html->add( '      for (key in state) {' ).
    ri_html->add( '        if (state.hasOwnProperty(key)) {' ).
    ri_html->add( '          var input = document.createElement("input");' ).
    ri_html->add( '          input.type = "hidden"; input.name = key; input.value = state[key];' ).
    ri_html->add( '          form.appendChild(input); n++;' ).
    ri_html->add( '        }' ).
    ri_html->add( '      }' ).
    ri_html->add( '      if (!n) { alert("No files selected"); return; }' ).
    ri_html->add( '      form.setAttribute("action", "sapevent:" + action); form.submit();' ).
    ri_html->add( '    }' ).
    ri_html->add( '    function markFilteredAndSubmit() {' ).
    ri_html->add( '      for (var i = 0; i < filtered.length; i++) { state[filtered[i].key] = filtered[i].defaultMethod; }' ).
    ri_html->add( '      save(); submit(gStageParams.formAction);' ).
    ri_html->add( '    }' ).
    ri_html->add( '    /* Previous/Next are server-side links; do not override them here. */' ).
    ri_html->add( '    var body = id("stageVirtualBody");' ).
    ri_html->add( '    if (body) { body.onclick = function(event) {' ).
    ri_html->add( '      event = event || window.event; var target = event.target || event.srcElement;' ).
    ri_html->add( '      var bulkContext = target && target.parentNode && target.parentNode.getAttribute ? target.parentNode.getAttribute("data-bulk-context") : "";' ).
    ri_html->add( '      if (bulkContext && target.getAttribute && target.getAttribute("data-method") !== null) {' ).
    ri_html->add( '        if (event.preventDefault) { event.preventDefault(); } event.cancelBubble = true;' ).
    ri_html->add( '        var bulkMethod = target.getAttribute("data-method");' ).
    ri_html->add( '        for (var bi = 0; bi < filtered.length; bi++) { if (filtered[bi].context === bulkContext) { if (bulkMethod) { state[filtered[bi].key] = bulkMethod; } else { delete state[filtered[bi].key]; } } }' ).
    ri_html->add( '        save(); render(); return false;' ).
    ri_html->add( '      }' ).
    ri_html->add( '      if (target && target.getAttribute && target.getAttribute("data-method") !== null) {' ).
    ri_html->add( '        if (event.preventDefault) { event.preventDefault(); } event.cancelBubble = true;' ).
    ri_html->add( '        var row = target; while (row && row.tagName !== "TR") { row = row.parentNode; }' ).
    ri_html->add( '        if (row) { var key = row.getAttribute("data-key"); var method = target.getAttribute("data-method"); if (method) { state[key] = method; } else { delete state[key]; } save(); render(); }' ).
    ri_html->add( '        return false;' ).
    ri_html->add( '      }' ).
    ri_html->add( '    }; }' ).
    ri_html->add( '    var filterInput = id(gStageParams.ids.objectSearch);' ).
    ri_html->add( '    if (filterInput) { filterInput.oninput = applyFilter; filterInput.onkeyup = applyFilter; }' ).
    ri_html->add( '    var meBtn = id("stageFilterByMe");' ).
    ri_html->add( '    if (meBtn) { meBtn.onclick = function(evt) {' ).
    ri_html->add( '      if (evt && evt.preventDefault) { evt.preventDefault(); }' ).
    ri_html->add( '      var srch = id(gStageParams.ids.objectSearch);' ).
    ri_html->add( '      if (srch) { srch.value = gStageParams.user || ""; }' ).
    ri_html->add( '      submitVirtualFilter(); return false;' ).
    ri_html->add( '    }; }' ).
    ri_html->add( '    var commitSelected = id(gStageParams.ids.commitSelectedBtn); if (commitSelected) { commitSelected.onclick = function(){ submit(gStageParams.formAction); return false; }; }' ).
    ri_html->add( '    var commitFiltered = id(gStageParams.ids.commitFilteredBtn); if (commitFiltered) { commitFiltered.onclick = function(){ markFilteredAndSubmit(); return false; }; }' ).
    ri_html->add( '    var patchButton = id(gStageParams.ids.patchBtn); if (patchButton) { patchButton.onclick = function(){ submit(gStageParams.patchAction); return false; }; }' ).
    ri_html->add( '    window.submitPatch = function(){ submit(gStageParams.patchAction); return false; };' ).
    ri_html->add( '    render();' ).
    ri_html->add( '  } catch (e) {' ).
    ri_html->add( '    var body = document.getElementById("stageVirtualBody");' ).
    ri_html->add( '    if (body) { body.innerHTML = "<tr><td colspan=\"7\" class=\"error\">Virtual stage initialization failed: " + (e && e.message ? e.message : e) + "</td></tr>"; }' ).
    ri_html->add( '  }' ).
    ri_html->add( '  }());' ).
    ri_html->add( '}' ).

  ENDMETHOD.
ENDCLASS.
