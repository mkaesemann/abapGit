CLASS zcl_abapgit_ortec_wapa DEFINITION
  PUBLIC
  FINAL
  CREATE PRIVATE .

* Optimised serialize / exists path for abapGit object handler ZCL_ABAPGIT_OBJECT_WAPA.
* Activated from ZCL_ABAPGIT_OBJECT_WAPA via kill-switch constants c_use_ortec_serialize / c_use_ortec_exists.
* Output (XML + raw page files written to io_files) MUST be byte-identical to the legacy path.
*
* Stage B:  direct active-version page reads from O2PAGDIR / O2PAGCON instead of
*           cl_o2_api_pages=>load for each page.
* exists:   SELECT SINGLE on o2appl directly, matching cl_o2_api_application=>load semantic
*           (succeeds if either active or inactive version exists).

  PUBLIC SECTION.

    CONSTANTS c_max_page_size_bytes TYPE i VALUE 10000000 ##NO_TEXT.

    CLASS-METHODS serialize
      IMPORTING
        !is_item         TYPE zif_abapgit_definitions=>ty_item
        !io_files        TYPE REF TO zcl_abapgit_objects_files
        !io_xml          TYPE REF TO zif_abapgit_xml_output
        !io_i18n_params  TYPE REF TO zcl_abapgit_i18n_params
      RAISING
        zcx_abapgit_exception .

    CLASS-METHODS exists
      IMPORTING
        !iv_name        TYPE o2applname
      RETURNING
        VALUE(rv_bool)  TYPE abap_bool .

  PROTECTED SECTION.

  PRIVATE SECTION.

    CONSTANTS c_active TYPE so2_version VALUE 'A' ##NO_TEXT.

    TYPES: BEGIN OF ty_page,
             attributes     TYPE o2pagattr,
             event_handlers TYPE o2pagevh_tabletype,
             parameters     TYPE o2pagpar_tabletype,
             types          TYPE rswsourcet,
           END OF ty_page.
    TYPES ty_pages_tt TYPE STANDARD TABLE OF ty_page WITH DEFAULT KEY.

    CLASS-METHODS read_page
      IMPORTING
        !is_item        TYPE zif_abapgit_definitions=>ty_item
        !is_page        TYPE o2pagattr
        !io_files       TYPE REF TO zcl_abapgit_objects_files
      RETURNING
        VALUE(rs_page)  TYPE ty_page
      RAISING
        zcx_abapgit_exception .

    CLASS-METHODS get_page_content
      IMPORTING
        !it_content       TYPE o2pageline_table
      RETURNING
        VALUE(rv_content) TYPE xstring
      RAISING
        zcx_abapgit_exception .

ENDCLASS.



CLASS zcl_abapgit_ortec_wapa IMPLEMENTATION.


  METHOD exists.

    DATA lv_dummy TYPE o2appl-applname.

    SELECT SINGLE applname FROM o2appl INTO lv_dummy
      WHERE applname = iv_name
        AND version  = 'A'.
    IF sy-subrc = 0.
      rv_bool = abap_true.
      RETURN.
    ENDIF.

    SELECT SINGLE applname FROM o2appl INTO lv_dummy
      WHERE applname = iv_name
        AND version  = 'I'.
    rv_bool = boolc( sy-subrc = 0 ).

  ENDMETHOD.


  METHOD get_page_content.

    DATA lv_string TYPE string.
    DATA lv_size   TYPE i.

    CONCATENATE LINES OF it_content INTO lv_string
      SEPARATED BY cl_abap_char_utilities=>newline
      RESPECTING BLANKS.

    rv_content = zcl_abapgit_convert=>string_to_xstring_utf8( lv_string ).
    lv_size = xstrlen( rv_content ).

    IF lv_size > c_max_page_size_bytes.
      zcx_abapgit_exception=>raise(
        |WAPA page content too large for safe serialization ({ lv_size } bytes, limit { c_max_page_size_bytes } bytes).| ).
    ENDIF.

    CLEAR lv_string.

  ENDMETHOD.


  METHOD read_page.

    DATA lv_name               TYPE o2applname.
    DATA ls_pgdir              TYPE o2pagdir.
    DATA ls_pagecon_key        TYPE o2pconkey.
    DATA lt_content            TYPE o2pageline_table.
    DATA lt_converted_content  TYPE o2pageline_table.
    DATA lv_content            TYPE xstring.
    DATA lv_xml_source         TYPE xstring.
    DATA lv_extra              TYPE string.
    DATA lv_ext                TYPE string.
    DATA lv_descript           TYPE o2descr.
    DATA lv_master_language    TYPE langu.
    DATA lv_layout_language    TYPE langu.
    DATA lv_errorcode          TYPE boolean.
    DATA lt_used_guids         TYPE bsp_guids.
    DATA lt_ev_handlers        TYPE STANDARD TABLE OF o2pagevh.
    DATA ls_ev_handler_db      TYPE o2pagevh.
    DATA ls_ev_handler         TYPE o2pagevhs.
    DATA lt_ev_handler_sources TYPE so2_ev_handler_t.
    DATA lt_parameter_db       TYPE STANDARD TABLE OF o2pagpar.
    DATA ls_parameter_entry    TYPE o2pagpars.
    DATA lv_filename           TYPE skwf_filnm.

    FIELD-SYMBOLS <ls_ev_handler_source> TYPE so2_ev_handler.

    lv_name = is_item-obj_name.
    lv_master_language = cl_o2_api_pages=>get_master_language( lv_name ).

    SELECT SINGLE * FROM o2pagdir INTO ls_pgdir
      WHERE applname = lv_name
        AND pagekey  = is_page-pagekey.
    IF sy-subrc <> 0.
      zcx_abapgit_exception=>raise( |WAPA page { lv_name }/{ is_page-pagekey } does not exist| ).
    ENDIF.

    SELECT SINGLE descript FROM o2pagdirt INTO lv_descript
      WHERE applname = lv_name
        AND pagekey  = is_page-pagekey
        AND langu    = sy-langu.
    IF sy-subrc <> 0 AND sy-langu <> lv_master_language.
      SELECT SINGLE descript FROM o2pagdirt INTO lv_descript
        WHERE applname = lv_name
          AND pagekey  = is_page-pagekey
          AND langu    = lv_master_language.
    ENDIF.

    MOVE-CORRESPONDING ls_pgdir TO rs_page-attributes.
    rs_page-attributes-version = c_active.
    rs_page-attributes-langu = sy-langu.
    rs_page-attributes-descript = lv_descript.
    IF rs_page-attributes-layoutlangu IS INITIAL.
      rs_page-attributes-layoutlangu = lv_master_language.
    ENDIF.

    IF rs_page-attributes-pagetype <> so2_controller.
      ls_pagecon_key-applname = lv_name.
      ls_pagecon_key-pagekey  = is_page-pagekey.
      ls_pagecon_key-objtype  = so2_objtype_page.
      ls_pagecon_key-version  = c_active.

      IMPORT content    TO lt_content
             xml_source TO lv_xml_source
             FROM DATABASE o2pagcon(tr) ID ls_pagecon_key
             ACCEPTING PADDING
             IGNORING CONVERSION ERRORS.
      IF sy-subrc <> 0.
        zcx_abapgit_exception=>raise( |WAPA page { lv_name }/{ is_page-pagekey } has no active content| ).
      ENDIF.

      lv_layout_language = rs_page-attributes-layoutlangu.
      IF lv_layout_language IS INITIAL.
        lv_layout_language = lv_master_language.
      ENDIF.

      IF rs_page-attributes-langu <> lv_layout_language AND lv_xml_source IS NOT INITIAL.
        lt_used_guids = cl_o2_co2_pp_otr=>get_otr_guids( lv_xml_source ).
        IF lt_used_guids IS NOT INITIAL.
          cl_o2_helper=>call_int_to_ext_converter(
            EXPORTING
              p_ext_source   = lt_content
              p_int_source   = lv_xml_source
              p_target_langu = rs_page-attributes-langu
              p_stripmode    = rs_page-attributes-stripmode
              p_source_langu = lv_master_language
              p_pagekey      = rs_page-attributes-pagekey
              p_applname     = rs_page-attributes-applname
              p_devclass     = rs_page-attributes-devclass
            IMPORTING
              p_source       = lt_converted_content
              p_error        = lv_errorcode ).
          IF lv_errorcode <> 'X'.
            lt_content = lt_converted_content.
          ENDIF.
          rs_page-attributes-layoutlangu = rs_page-attributes-langu.
        ENDIF.
      ENDIF.

      IF rs_page-attributes-pagetype = so2_full_page.
        SELECT * FROM o2pagevh INTO TABLE lt_ev_handlers
          WHERE applname = lv_name
            AND pagekey  = is_page-pagekey
            AND version  = c_active.
        IF sy-subrc = 0.
          ls_pagecon_key-objtype = so2_objtype_evhndl.
          IMPORT evhandler TO lt_ev_handler_sources
                 FROM DATABASE o2pagcon(tr) ID ls_pagecon_key
                 ACCEPTING PADDING
                 IGNORING CONVERSION ERRORS.
          LOOP AT lt_ev_handlers INTO ls_ev_handler_db.
            CHECK cl_o2_helper=>is_standard_ev_handler( ls_ev_handler_db-evhandler ) = abap_true.

            CLEAR ls_ev_handler.
            MOVE-CORRESPONDING ls_ev_handler_db TO ls_ev_handler.
            READ TABLE lt_ev_handler_sources WITH KEY name = ls_ev_handler-evhandler
              ASSIGNING <ls_ev_handler_source>.
            IF sy-subrc = 0.
              ls_ev_handler-source = <ls_ev_handler_source>-source.
            ELSE.
              CLEAR ls_ev_handler-source.
            ENDIF.
            INSERT ls_ev_handler INTO TABLE rs_page-event_handlers.
          ENDLOOP.
        ENDIF.

        ls_pagecon_key-objtype = so2_objtype_types.
        IMPORT typesource TO rs_page-types
               FROM DATABASE o2pagcon(tr) ID ls_pagecon_key
               ACCEPTING PADDING
               IGNORING CONVERSION ERRORS.
      ENDIF.

      SELECT * FROM o2pagpar INTO TABLE lt_parameter_db
        WHERE applname = lv_name
          AND pagekey  = is_page-pagekey
          AND version  = c_active.
      LOOP AT lt_parameter_db INTO ls_parameter_entry-db.
        SELECT SINGLE descript FROM o2pagpart INTO ls_parameter_entry-text
          WHERE applname = lv_name
            AND pagekey  = is_page-pagekey
            AND compname = ls_parameter_entry-compname
            AND langu    = rs_page-attributes-langu.
        IF sy-subrc <> 0 AND rs_page-attributes-langu <> lv_master_language.
          SELECT SINGLE descript FROM o2pagpart INTO ls_parameter_entry-text
            WHERE applname = lv_name
              AND pagekey  = is_page-pagekey
              AND compname = ls_parameter_entry-compname
              AND langu    = lv_master_language.
        ENDIF.
        IF ls_parameter_entry-aliasname IS INITIAL.
          ls_parameter_entry-aliasname = ls_parameter_entry-compname.
          TRANSLATE ls_parameter_entry-aliasname TO LOWER CASE. "#EC SYNTCHAR
        ENDIF.
        APPEND ls_parameter_entry TO rs_page-parameters.
      ENDLOOP.
    ENDIF.

    IF rs_page-attributes-pagetype = so2_controller.
      CLEAR rs_page-attributes-mimetype.
    ELSEIF rs_page-attributes-mimetype IS INITIAL AND rs_page-attributes-pagetype <> so2_fragment_page.
      lv_filename = rs_page-attributes-pagekey.
      CALL FUNCTION 'SKWF_MIMETYPE_OF_FILE_GET'
        EXPORTING
          filename             = lv_filename
          x_use_local_registry = ' '
        IMPORTING
          mimetype             = rs_page-attributes-mimetype.
      IF rs_page-attributes-mimetype IS INITIAL.
        rs_page-attributes-mimetype = so2_default_mimetype.
      ENDIF.
    ENDIF.

    IF rs_page-attributes-pagetype <> so2_controller.
      lv_content = get_page_content( lt_content ).
      SPLIT rs_page-attributes-pagename AT '.' INTO lv_extra lv_ext.
      REPLACE ALL OCCURRENCES OF '/' IN lv_ext   WITH '_-'.
      REPLACE ALL OCCURRENCES OF '/' IN lv_extra WITH '_-'.

      io_files->add_raw(
        iv_extra = lv_extra
        iv_ext   = lv_ext
        iv_data  = lv_content ).

      CLEAR: lv_content, rs_page-attributes-implclass.
    ENDIF.

    CLEAR: rs_page-attributes-author,
           rs_page-attributes-createdon,
           rs_page-attributes-changedby,
           rs_page-attributes-changedon,
           rs_page-attributes-changetime,
           rs_page-attributes-gendate,
           rs_page-attributes-gentime,
           rs_page-attributes-devclass.

  ENDMETHOD.


  METHOD serialize.

    DATA lv_name       TYPE o2applname.
    DATA ls_attributes TYPE o2applattr.
    DATA lt_navgraph   TYPE o2applgrap_table.
    DATA lt_pages      TYPE o2pagelist.
    DATA lt_pages_info TYPE ty_pages_tt.
    DATA lo_bsp        TYPE REF TO cl_o2_api_application.

    FIELD-SYMBOLS <ls_page>                  LIKE LINE OF lt_pages.
    FIELD-SYMBOLS <lv_abap_language_version> TYPE uccheck.

    lv_name = is_item-obj_name.

    cl_o2_api_application=>load(
      EXPORTING
        p_application_name  = lv_name
      IMPORTING
        p_application       = lo_bsp
      EXCEPTIONS
        object_not_existing = 1
        permission_failure  = 2
        error_occured       = 3 ).
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    lo_bsp->get_attributes(
      EXPORTING
        p_version    = c_active
      IMPORTING
        p_attributes = ls_attributes ).

    CLEAR: ls_attributes-author,
           ls_attributes-createdon,
           ls_attributes-changedby,
           ls_attributes-changedon,
           ls_attributes-devclass.

    ASSIGN COMPONENT 'ABAP_LANGUAGE_VERSION' OF STRUCTURE ls_attributes
           TO <lv_abap_language_version>.
    IF sy-subrc = 0.
      CLEAR <lv_abap_language_version>.
    ENDIF.

    io_xml->add( iv_name = 'ATTRIBUTES'
                 ig_data = ls_attributes ).

    lo_bsp->get_navgraph(
      EXPORTING
        p_version  = c_active
      IMPORTING
        p_navgraph = lt_navgraph ).

    io_xml->add( iv_name = 'NAVGRAPH'
                 ig_data = lt_navgraph ).

    cl_o2_api_pages=>get_all_pages(
      EXPORTING
        p_applname = lv_name
        p_version  = c_active
      IMPORTING
        p_pages    = lt_pages ).

    LOOP AT lt_pages ASSIGNING <ls_page>.
      APPEND read_page(
               is_item  = is_item
               is_page  = <ls_page>
               io_files = io_files )
             TO lt_pages_info.
    ENDLOOP.

    io_xml->add( iv_name = 'PAGES'
                 ig_data = lt_pages_info ).

    CLEAR: lt_pages_info, lt_pages, lt_navgraph.

    zcl_abapgit_sotr_handler=>read_sotr(
      iv_pgmid       = 'LIMU'
      iv_object      = 'WAPP'
      iv_obj_name    = is_item-obj_name
      io_i18n_params = io_i18n_params
      io_xml         = io_xml ).

  ENDMETHOD.

ENDCLASS.

