CLASS zcl_abapgit_ortec_wapa DEFINITION
  PUBLIC
  FINAL
  CREATE PRIVATE .

* Optimised serialize / exists path for abapGit object handler ZCL_ABAPGIT_OBJECT_WAPA.
* Activated from ZCL_ABAPGIT_OBJECT_WAPA via switch in zcl_abapgit_ortec_git_switch.
* Output (XML + raw page files written to io_files) MUST be byte-identical to the legacy path.
*
* Stage B:  direct active-version page reads from O2PAGDIR / O2PAGCON instead of
*           cl_o2_api_pages=>load for each page.
* exists:   SELECT SINGLE on o2appl directly, matching cl_o2_api_application=>load semantic
*           (succeeds if either active or inactive version exists).

  PUBLIC SECTION.

    CLASS-METHODS serialize
      IMPORTING
        !is_item        TYPE zif_abapgit_definitions=>ty_item
        !io_files       TYPE REF TO zcl_abapgit_objects_files
        !io_xml         TYPE REF TO zif_abapgit_xml_output
        !io_i18n_params TYPE REF TO zcl_abapgit_i18n_params
      RAISING
        zcx_abapgit_exception .

    CLASS-METHODS exists
      IMPORTING
        !iv_name       TYPE o2applname
      RETURNING
        VALUE(rv_bool) TYPE abap_bool .

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

    TYPES ty_page_dirs       TYPE SORTED TABLE OF o2pagdir  WITH NON-UNIQUE KEY applname pagekey.
    TYPES ty_page_texts      TYPE SORTED TABLE OF o2pagdirt WITH NON-UNIQUE KEY applname pagekey langu.
    TYPES ty_event_handlers  TYPE SORTED TABLE OF o2pagevh  WITH NON-UNIQUE KEY applname pagekey version evhandler.
    TYPES ty_parameters      TYPE SORTED TABLE OF o2pagpar  WITH NON-UNIQUE KEY applname pagekey version compname.
    TYPES ty_parameter_texts TYPE SORTED TABLE OF o2pagpart WITH NON-UNIQUE KEY applname pagekey compname langu.

    TYPES: BEGIN OF ty_context,
             name            TYPE o2applname,
             master_language TYPE langu,
             page_dirs       TYPE ty_page_dirs,
             page_texts      TYPE ty_page_texts,
             event_handlers  TYPE ty_event_handlers,
             parameters      TYPE ty_parameters,
             parameter_texts TYPE ty_parameter_texts,
           END OF ty_context.

    CLASS-METHODS build_context
      IMPORTING
        !iv_name          TYPE o2applname
        !it_pages         TYPE o2pagelist
      RETURNING
        VALUE(rs_context) TYPE ty_context.

    CLASS-METHODS get_page_description
      IMPORTING
        !is_context        TYPE ty_context
        !iv_pagekey        TYPE o2pagdir-pagekey
      RETURNING
        VALUE(rv_descript) TYPE o2descr.

    CLASS-METHODS get_parameter_description
      IMPORTING
        !is_context        TYPE ty_context
        !iv_pagekey        TYPE o2pagpar-pagekey
        !iv_compname       TYPE o2pagpar-compname
        !iv_language       TYPE langu
      RETURNING
        VALUE(rv_descript) TYPE o2descr.

    CLASS-METHODS add_page_content_file
      IMPORTING
        !is_context TYPE ty_context
        !io_files   TYPE REF TO zcl_abapgit_objects_files
      CHANGING
        !cs_page    TYPE ty_page
      RAISING
        zcx_abapgit_exception.

    CLASS-METHODS add_full_page_details
      IMPORTING
        !is_context TYPE ty_context
      CHANGING
        !cs_page    TYPE ty_page.

    CLASS-METHODS add_parameters
      IMPORTING
        !is_context TYPE ty_context
      CHANGING
        !cs_page    TYPE ty_page.

    CLASS-METHODS adjust_mimetype
      CHANGING
        !cs_attributes TYPE o2pagattr.

    CLASS-METHODS clear_page_attributes
      CHANGING
        !cs_attributes TYPE o2pagattr.

    CLASS-METHODS read_page
      IMPORTING
        !is_context    TYPE ty_context
        !is_page       TYPE o2pagattr
        !io_files      TYPE REF TO zcl_abapgit_objects_files
      RETURNING
        VALUE(rs_page) TYPE ty_page
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
    DATA lt_versions TYPE RANGE OF so2_version.
    DATA ls_version LIKE LINE OF lt_versions.

    ls_version-sign = 'I'.
    ls_version-option = 'EQ'.
    ls_version-low = c_active.
    APPEND ls_version TO lt_versions.
    ls_version-low = 'I'.
    APPEND ls_version TO lt_versions.

    SELECT SINGLE applname FROM o2appl INTO lv_dummy
      WHERE applname = iv_name
        AND version IN lt_versions.
    rv_bool = boolc( sy-subrc = 0 ).

  ENDMETHOD.


  METHOD get_page_content.

    DATA lv_string TYPE string.

    CONCATENATE LINES OF it_content INTO lv_string
      SEPARATED BY cl_abap_char_utilities=>newline
      RESPECTING BLANKS.

* abapGit stores the final raw page as one xstring; do not add an artificial WAPA page size cap here.
    rv_content = zcl_abapgit_convert=>string_to_xstring_utf8( lv_string ).

    CLEAR lv_string.

  ENDMETHOD.


  METHOD build_context.

    DATA lt_languages TYPE RANGE OF langu.
    DATA ls_language LIKE LINE OF lt_languages.

    rs_context-name = iv_name.
    rs_context-master_language = cl_o2_api_pages=>get_master_language( iv_name ).

    IF it_pages IS INITIAL.
      RETURN.
    ENDIF.

    ls_language-sign = 'I'.
    ls_language-option = 'EQ'.
    ls_language-low = sy-langu.
    APPEND ls_language TO lt_languages.
    IF sy-langu <> rs_context-master_language.
      ls_language-low = rs_context-master_language.
      APPEND ls_language TO lt_languages.
    ENDIF.

    SELECT * FROM o2pagdir INTO TABLE rs_context-page_dirs
      FOR ALL ENTRIES IN it_pages
      WHERE applname = iv_name
        AND pagekey  = it_pages-pagekey
      ORDER BY PRIMARY KEY.

    SELECT * FROM o2pagdirt INTO TABLE rs_context-page_texts
      FOR ALL ENTRIES IN it_pages
      WHERE applname = iv_name
        AND pagekey  = it_pages-pagekey
        AND langu IN lt_languages
      ORDER BY PRIMARY KEY.

    SELECT * FROM o2pagevh INTO TABLE rs_context-event_handlers
      FOR ALL ENTRIES IN it_pages
      WHERE applname = iv_name
        AND pagekey  = it_pages-pagekey
        AND version  = c_active
      ORDER BY PRIMARY KEY.

    SELECT * FROM o2pagpar INTO TABLE rs_context-parameters
      FOR ALL ENTRIES IN it_pages
      WHERE applname = iv_name
        AND pagekey  = it_pages-pagekey
        AND version  = c_active
      ORDER BY PRIMARY KEY.

    IF rs_context-parameters IS NOT INITIAL.
      SELECT * FROM o2pagpart INTO TABLE rs_context-parameter_texts
        FOR ALL ENTRIES IN rs_context-parameters
        WHERE applname = iv_name
          AND pagekey  = rs_context-parameters-pagekey
          AND compname = rs_context-parameters-compname
          AND langu IN lt_languages
        ORDER BY PRIMARY KEY.
    ENDIF.

  ENDMETHOD.


  METHOD get_page_description.

    DATA ls_text TYPE o2pagdirt.

    READ TABLE is_context-page_texts INTO ls_text
      WITH KEY applname = is_context-name
               pagekey  = iv_pagekey
               langu    = sy-langu.
    IF sy-subrc <> 0 AND sy-langu <> is_context-master_language.
      READ TABLE is_context-page_texts INTO ls_text
        WITH KEY applname = is_context-name
                 pagekey  = iv_pagekey
                 langu    = is_context-master_language.
    ENDIF.

    rv_descript = ls_text-descript.

  ENDMETHOD.


  METHOD get_parameter_description.

    DATA ls_text TYPE o2pagpart.

    READ TABLE is_context-parameter_texts INTO ls_text
      WITH KEY applname = is_context-name
               pagekey  = iv_pagekey
               compname = iv_compname
               langu    = iv_language.
    IF sy-subrc <> 0 AND iv_language <> is_context-master_language.
      READ TABLE is_context-parameter_texts INTO ls_text
        WITH KEY applname = is_context-name
                 pagekey  = iv_pagekey
                 compname = iv_compname
                 langu    = is_context-master_language.
    ENDIF.

    rv_descript = ls_text-descript.

  ENDMETHOD.


  METHOD add_page_content_file.

    DATA ls_pagecon_key       TYPE o2pconkey.
    DATA lt_content           TYPE o2pageline_table.
    DATA lt_converted_content TYPE o2pageline_table.
    DATA lv_content           TYPE xstring.
    DATA lv_xml_source        TYPE xstring.
    DATA lv_extra             TYPE string.
    DATA lv_ext               TYPE string.
    DATA lv_layout_language   TYPE langu.
    DATA lv_errorcode         TYPE boolean.
    DATA lt_used_guids        TYPE bsp_guids.

    ls_pagecon_key-applname = is_context-name.
    ls_pagecon_key-pagekey  = cs_page-attributes-pagekey.
    ls_pagecon_key-objtype  = so2_objtype_page.
    ls_pagecon_key-version  = c_active.

    IMPORT content    TO lt_content
           xml_source TO lv_xml_source
           FROM DATABASE o2pagcon(tr) ID ls_pagecon_key
           ACCEPTING PADDING
           IGNORING CONVERSION ERRORS.
    IF sy-subrc <> 0.
      zcx_abapgit_exception=>raise(
        |WAPA page { is_context-name }/{ cs_page-attributes-pagekey } has no active content| ).
    ENDIF.

    lv_layout_language = cs_page-attributes-layoutlangu.
    IF lv_layout_language IS INITIAL.
      lv_layout_language = is_context-master_language.
    ENDIF.

    IF cs_page-attributes-langu <> lv_layout_language AND lv_xml_source IS NOT INITIAL.
      lt_used_guids = cl_o2_co2_pp_otr=>get_otr_guids( lv_xml_source ).
      IF lt_used_guids IS NOT INITIAL.
        cl_o2_helper=>call_int_to_ext_converter(
          EXPORTING
            p_ext_source   = lt_content
            p_int_source   = lv_xml_source
            p_target_langu = cs_page-attributes-langu
            p_stripmode    = cs_page-attributes-stripmode
            p_source_langu = is_context-master_language
            p_pagekey      = cs_page-attributes-pagekey
            p_applname     = cs_page-attributes-applname
            p_devclass     = cs_page-attributes-devclass
          IMPORTING
            p_source       = lt_converted_content
            p_error        = lv_errorcode ).
        IF lv_errorcode <> 'X'.
          lt_content = lt_converted_content.
        ENDIF.
        cs_page-attributes-layoutlangu = cs_page-attributes-langu.
      ENDIF.
    ENDIF.

    lv_content = get_page_content( lt_content ).
    SPLIT cs_page-attributes-pagename AT '.' INTO lv_extra lv_ext.
    REPLACE ALL OCCURRENCES OF '/' IN lv_ext   WITH '_-'.
    REPLACE ALL OCCURRENCES OF '/' IN lv_extra WITH '_-'.

    io_files->add_raw(
      iv_extra = lv_extra
      iv_ext   = lv_ext
      iv_data  = lv_content ).

    CLEAR: lv_content, lv_xml_source, cs_page-attributes-implclass.
    FREE: lt_content, lt_converted_content, lt_used_guids.

  ENDMETHOD.


  METHOD add_full_page_details.

    DATA ls_pagecon_key        TYPE o2pconkey.
    DATA ls_ev_handler_db      TYPE o2pagevh.
    DATA ls_ev_handler         TYPE o2pagevhs.
    DATA lt_ev_handler_sources TYPE so2_ev_handler_t.

    FIELD-SYMBOLS <ls_ev_handler_source> TYPE so2_ev_handler.

    IF cs_page-attributes-pagetype <> so2_full_page.
      RETURN.
    ENDIF.

    ls_pagecon_key-applname = is_context-name.
    ls_pagecon_key-pagekey  = cs_page-attributes-pagekey.
    ls_pagecon_key-version  = c_active.

    READ TABLE is_context-event_handlers TRANSPORTING NO FIELDS
      WITH KEY applname = is_context-name
               pagekey  = cs_page-attributes-pagekey
               version  = c_active.
    IF sy-subrc = 0.
      ls_pagecon_key-objtype = so2_objtype_evhndl.
      IMPORT evhandler TO lt_ev_handler_sources
             FROM DATABASE o2pagcon(tr) ID ls_pagecon_key
             ACCEPTING PADDING
             IGNORING CONVERSION ERRORS.

      LOOP AT is_context-event_handlers INTO ls_ev_handler_db
        WHERE applname = is_context-name
          AND pagekey  = cs_page-attributes-pagekey
          AND version  = c_active.
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
        INSERT ls_ev_handler INTO TABLE cs_page-event_handlers.
      ENDLOOP.
    ENDIF.

    ls_pagecon_key-objtype = so2_objtype_types.
    IMPORT typesource TO cs_page-types
           FROM DATABASE o2pagcon(tr) ID ls_pagecon_key
           ACCEPTING PADDING
           IGNORING CONVERSION ERRORS.

    FREE lt_ev_handler_sources.

  ENDMETHOD.


  METHOD add_parameters.

    DATA ls_parameter_db    TYPE o2pagpar.
    DATA ls_parameter_entry TYPE o2pagpars.

    LOOP AT is_context-parameters INTO ls_parameter_db
      WHERE applname = is_context-name
        AND pagekey  = cs_page-attributes-pagekey
        AND version  = c_active.
      CLEAR ls_parameter_entry.
      ls_parameter_entry-db = ls_parameter_db.
      MOVE-CORRESPONDING ls_parameter_db TO ls_parameter_entry.
      ls_parameter_entry-text = get_parameter_description(
        is_context  = is_context
        iv_pagekey  = ls_parameter_db-pagekey
        iv_compname = ls_parameter_db-compname
        iv_language = cs_page-attributes-langu ).
      IF ls_parameter_entry-aliasname IS INITIAL.
        ls_parameter_entry-aliasname = ls_parameter_entry-compname.
        TRANSLATE ls_parameter_entry-aliasname TO LOWER CASE. "#EC SYNTCHAR
      ENDIF.
      APPEND ls_parameter_entry TO cs_page-parameters.
    ENDLOOP.

  ENDMETHOD.


  METHOD adjust_mimetype.

    DATA lv_filename TYPE skwf_filnm.

    IF cs_attributes-pagetype = so2_controller.
      CLEAR cs_attributes-mimetype.
    ELSEIF cs_attributes-mimetype IS INITIAL AND cs_attributes-pagetype <> so2_fragment_page.
      lv_filename = cs_attributes-pagekey.
      CALL FUNCTION 'SKWF_MIMETYPE_OF_FILE_GET'
        EXPORTING
          filename             = lv_filename
          x_use_local_registry = ' '
        IMPORTING
          mimetype             = cs_attributes-mimetype.
      IF cs_attributes-mimetype IS INITIAL.
        cs_attributes-mimetype = so2_default_mimetype.
      ENDIF.
    ENDIF.

  ENDMETHOD.


  METHOD clear_page_attributes.

    CLEAR: cs_attributes-author,
           cs_attributes-createdon,
           cs_attributes-changedby,
           cs_attributes-changedon,
           cs_attributes-changetime,
           cs_attributes-gendate,
           cs_attributes-gentime,
           cs_attributes-devclass.

  ENDMETHOD.


  METHOD read_page.

    DATA ls_pgdir TYPE o2pagdir.

    READ TABLE is_context-page_dirs INTO ls_pgdir
      WITH KEY applname = is_context-name
               pagekey  = is_page-pagekey.
    IF sy-subrc <> 0.
      zcx_abapgit_exception=>raise( |WAPA page { is_context-name }/{ is_page-pagekey } does not exist| ).
    ENDIF.

    MOVE-CORRESPONDING ls_pgdir TO rs_page-attributes.
    rs_page-attributes-version = c_active.
    rs_page-attributes-langu = sy-langu.
    rs_page-attributes-descript = get_page_description(
      is_context = is_context
      iv_pagekey = is_page-pagekey ).
    IF rs_page-attributes-layoutlangu IS INITIAL.
      rs_page-attributes-layoutlangu = is_context-master_language.
    ENDIF.

    IF rs_page-attributes-pagetype <> so2_controller.
      add_page_content_file(
        EXPORTING
          is_context = is_context
          io_files   = io_files
        CHANGING
          cs_page    = rs_page ).
      add_full_page_details(
        EXPORTING
          is_context = is_context
        CHANGING
          cs_page    = rs_page ).
      add_parameters(
        EXPORTING
          is_context = is_context
        CHANGING
          cs_page    = rs_page ).
    ENDIF.

    adjust_mimetype( CHANGING cs_attributes = rs_page-attributes ).
    clear_page_attributes( CHANGING cs_attributes = rs_page-attributes ).

  ENDMETHOD.


  METHOD serialize.

    DATA lv_name       TYPE o2applname.
    DATA ls_attributes TYPE o2applattr.
    DATA lt_navgraph   TYPE o2applgrap_table.
    DATA lt_pages      TYPE o2pagelist.
    DATA lt_pages_info TYPE ty_pages_tt.
    DATA ls_context    TYPE ty_context.
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

    ls_context = build_context(
      iv_name  = lv_name
      it_pages = lt_pages ).

    LOOP AT lt_pages ASSIGNING <ls_page>.
      APPEND read_page(
               is_context = ls_context
               is_page    = <ls_page>
               io_files   = io_files )
             TO lt_pages_info.
    ENDLOOP.

    io_xml->add( iv_name = 'PAGES'
                 ig_data = lt_pages_info ).

    CLEAR: ls_context, lt_pages_info, lt_pages, lt_navgraph.

    zcl_abapgit_sotr_handler=>read_sotr(
      iv_pgmid       = 'LIMU'
      iv_object      = 'WAPP'
      iv_obj_name    = is_item-obj_name
      io_i18n_params = io_i18n_params
      io_xml         = io_xml ).

  ENDMETHOD.

ENDCLASS.

