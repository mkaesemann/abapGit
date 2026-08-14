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
* Stage C:  bounded, whole-WAPA, all-or-nothing raw O2PAGCON bulk prefetch
*           (TRY_RAW_PREFETCH) replacing the per-key IMPORT ... FROM DATABASE with
*           one bulk SELECT + SRTF2-ordered CLUSTR reconstruction + IMPORT FROM DATA
*           BUFFER; any anomaly (row/byte cap, malformed row, sequence gap, decode
*           failure) transparently falls back to the original per-key IMPORT path
*           for the WHOLE WAPA - never a partial mix (SER-FINAL, IT8-proven parity).
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

    "! #SER-FINAL raw O2PAGCON prefetch observability (IT8 handoff -
    "! Phase 7 "explicit prefetch hits/fallbacks" requirement). Session-
    "! lifetime, in-memory only - never persisted, never cross-request.
    CLASS-METHODS get_raw_prefetch_counters
      EXPORTING
        !ev_hits                  TYPE i
        !ev_fallbacks             TYPE i
        !ev_manifest_selects      TYPE i
        !ev_payload_selects       TYPE i
        !ev_verify_selects        TYPE i
        !ev_split_events          TYPE i
        !ev_depth_ceiling_hits    TYPE i
        !ev_range_fallbacks       TYPE i
        !ev_range_fallback_pages  TYPE i
        !ev_decode_fallback_pages TYPE i
        !ev_reference_pages       TYPE i
        !ev_max_manifest_rows     TYPE i
        !ev_max_charged_raw_bytes TYPE int8
        !ev_max_raw_split_depth   TYPE i.

    CLASS-METHODS reset_raw_prefetch_counters.

  PROTECTED SECTION.

  PRIVATE SECTION.

    CONSTANTS c_active TYPE so2_version VALUE 'A' ##NO_TEXT.

    "! HARD SAFETY BOUND on the number of physical O2PAGCON rows read for
    "! ONE WAPA's raw prefetch attempt. Derived, not copied from the IT8
    "! experiment's own 5000-row probe cap: the largest real payload
    "! observed across all six IT8 fixtures (including the 865-page
    "! /O4H/TPL_LIB_MAP) was 1126 physical rows for a SINGLE logical key;
    "! this constant bounds the WHOLE WAPA's aggregate row count with a
    "! ~18x safety margin over that single-key maximum, protecting
    "! against a pathological many-tiny-rows case that the byte cap alone
    "! would not catch. Hitting this cap is always treated as an
    "! ambiguous/possibly-truncated result -> safe fallback, never a
    "! partial read.
    CONSTANTS c_raw_prefetch_initial_pages  TYPE i VALUE 12000. "6000.
    CONSTANTS c_max_raw_manifest_rows       TYPE i VALUE 90000. "40000.
    CONSTANTS c_max_raw_prefetch_rows       TYPE i VALUE 30000.
    CONSTANTS c_max_raw_payload_bytes       TYPE int8 VALUE 268435456. "104857600.
    CONSTANTS c_raw_payload_row_bytes       TYPE i VALUE 2886.
    CONSTANTS c_max_raw_split_depth         TYPE i VALUE 5.
    CONSTANTS c_max_decoded_page_bytes      TYPE int8 VALUE 15728640.

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

    " One decoded PAGE-content logical key (content + XML source).
    TYPES: BEGIN OF ty_raw_content,
             pagekey    TYPE o2pagdir-pagekey,
             content    TYPE o2pageline_table,
             xml_source TYPE xstring,
           END OF ty_raw_content.
    TYPES ty_raw_content_tt TYPE SORTED TABLE OF ty_raw_content WITH UNIQUE KEY pagekey.

    " One decoded EVHNDL logical key.
    TYPES: BEGIN OF ty_raw_evhandler,
             pagekey   TYPE o2pagdir-pagekey,
             evhandler TYPE so2_ev_handler_t,
           END OF ty_raw_evhandler.
    TYPES ty_raw_evhandler_tt TYPE SORTED TABLE OF ty_raw_evhandler WITH UNIQUE KEY pagekey.

    " One decoded TYPES logical key.
    TYPES: BEGIN OF ty_raw_typesource,
             pagekey    TYPE o2pagdir-pagekey,
             typesource TYPE rswsourcet,
           END OF ty_raw_typesource.
    TYPES ty_raw_typesource_tt TYPE SORTED TABLE OF ty_raw_typesource WITH UNIQUE KEY pagekey.

    " One requested page's optional PAGE/EVHNDL/TYPES sub-keys. PAGE
    " content itself is implicit/mandatory for every non-controller page
    " and therefore not modelled as a flag here.
    TYPES: BEGIN OF ty_raw_key,
             pagekey     TYPE o2pagdir-pagekey,
             need_evhndl TYPE abap_bool,
             need_types  TYPE abap_bool,
           END OF ty_raw_key.
    TYPES ty_raw_key_tt TYPE SORTED TABLE OF ty_raw_key WITH UNIQUE KEY pagekey.

    TYPES: BEGIN OF ty_raw_manifest,
             pagekey TYPE o2pagdir-pagekey,
             objtype TYPE o2pconkey-objtype,
             srtf2   TYPE i,
             clustr  TYPE i,
           END OF ty_raw_manifest.
    TYPES ty_raw_manifest_tt TYPE STANDARD TABLE OF ty_raw_manifest
      WITH EMPTY KEY
      WITH NON-UNIQUE SORTED KEY by_row COMPONENTS pagekey objtype srtf2.
    TYPES ty_raw_reject_reason TYPE c LENGTH 1.

    " One physical O2PAGCON row (RELID is always the literal 'TR' area
    " and is filtered on, never carried in this local structure).
    TYPES: BEGIN OF ty_raw_row,
             pagekey TYPE o2pagdir-pagekey,
             objtype TYPE o2pconkey-objtype,
             srtf2   TYPE i,
             clustr  TYPE i,
             clustd  TYPE xstring,
           END OF ty_raw_row.
    TYPES ty_raw_row_tt TYPE STANDARD TABLE OF ty_raw_row
      WITH DEFAULT KEY
      WITH NON-UNIQUE SORTED KEY by_page COMPONENTS pagekey objtype srtf2.

    TYPES: BEGIN OF ty_context,
             name                TYPE o2applname,
             master_language     TYPE langu,
             page_dirs           TYPE ty_page_dirs,
             page_texts          TYPE ty_page_texts,
             event_handlers      TYPE ty_event_handlers,
             parameters          TYPE ty_parameters,
             parameter_texts     TYPE ty_parameter_texts,
             "! #SER-FINAL: TRUE only after a bounded, whole-WAPA,
             "! all-or-nothing raw O2PAGCON prefetch succeeded for every
             "! requested key - see TRY_RAW_PREFETCH. FALSE means every
             "! page transparently uses the original per-key
             "! IMPORT ... FROM DATABASE path, unchanged.
             raw_prefetch_active TYPE abap_bool,
             raw_content         TYPE ty_raw_content_tt,
             raw_evhandler       TYPE ty_raw_evhandler_tt,
             raw_typesource      TYPE ty_raw_typesource_tt,
           END OF ty_context.

    CLASS-DATA gv_raw_prefetch_hits      TYPE i.
    CLASS-DATA gv_raw_prefetch_fallbacks TYPE i.
    CLASS-DATA gv_manifest_selects       TYPE i.
    CLASS-DATA gv_payload_selects        TYPE i.
    CLASS-DATA gv_verify_selects         TYPE i.
    CLASS-DATA gv_split_events           TYPE i.
    CLASS-DATA gv_depth_ceiling_hits     TYPE i.
    CLASS-DATA gv_range_fallbacks        TYPE i.
    CLASS-DATA gv_range_fallback_pages   TYPE i.
    CLASS-DATA gv_decode_fallback_pages  TYPE i.
    CLASS-DATA gv_reference_pages        TYPE i.
    CLASS-DATA gv_max_manifest_rows      TYPE i.
    CLASS-DATA gv_max_charged_raw_bytes  TYPE int8.
    CLASS-DATA gv_max_raw_split_depth    TYPE i.

    CLASS-METHODS build_context
      IMPORTING
        !iv_name          TYPE o2applname
        !it_pages         TYPE o2pagelist
      RETURNING
        VALUE(rs_context) TYPE ty_context.

    "! #SER-FINAL: attempts the bounded, whole-WAPA, all-or-nothing raw
    "! O2PAGCON prefetch. On ANY anomaly (row/byte cap, malformed row,
    "! sequence gap, decode failure) leaves
    "! CS_CONTEXT-RAW_PREFETCH_ACTIVE = ABAP_FALSE - never raises, so
    "! every page transparently falls back to the original per-key
    "! IMPORT ... FROM DATABASE path.
    CLASS-METHODS try_raw_prefetch
      IMPORTING
        !it_pages   TYPE o2pagelist
      CHANGING
        !cs_context TYPE ty_context
      RAISING
        zcx_abapgit_exception.

    CLASS-METHODS clear_raw_context
      CHANGING
        !cs_context TYPE ty_context.

    CLASS-METHODS serialize_reference_range
      IMPORTING
        !it_pages      TYPE o2pagelist
        !io_files      TYPE REF TO zcl_abapgit_objects_files
      CHANGING
        !cs_context    TYPE ty_context
        !ct_pages_info TYPE ty_pages_tt
      RAISING
        zcx_abapgit_exception.

    "! Pure, deterministic: which PAGE/EVHNDL/TYPES sub-keys are needed
    "! for IS_CONTEXT's own page set - mirrors the exact existence checks
    "! ADD_FULL_PAGE_DETAILS/READ_PAGE already perform today, so the
    "! requested-key set can never diverge from what the reference path
    "! would itself have looked for.
    CLASS-METHODS build_requested_keys
      IMPORTING
        !it_pages      TYPE o2pagelist
        !is_context    TYPE ty_context
      RETURNING
        VALUE(rt_keys) TYPE ty_raw_key_tt.

    CLASS-METHODS read_raw_manifest
      IMPORTING
        !iv_name              TYPE o2applname
        !it_keys              TYPE ty_raw_key_tt
        !iv_max_rows          TYPE i DEFAULT c_max_raw_manifest_rows
        !iv_max_payload_bytes TYPE int8 DEFAULT c_max_raw_payload_bytes
      EXPORTING
        !et_manifest          TYPE ty_raw_manifest_tt
        !ev_admitted          TYPE abap_bool
        !ev_reject_reason     TYPE ty_raw_reject_reason
      RAISING
        zcx_abapgit_exception.

    "! Bounded bulk read of the real physical O2PAGCON rows for exactly
    "! the requested keys of ONE WAPA. EV_ROW_CAP_HIT = ABAP_TRUE means
    "! the result may be truncated (ambiguous) - the caller must treat
    "! this as a hard failure, never as partial data.
    CLASS-METHODS read_raw_rows
      IMPORTING
        !iv_name     TYPE o2applname
        !it_keys     TYPE ty_raw_key_tt
        !it_manifest TYPE ty_raw_manifest_tt
      EXPORTING
        !et_rows     TYPE ty_raw_row_tt
        !ev_complete TYPE abap_bool
      RAISING
        zcx_abapgit_exception.

    CLASS-METHODS decode_raw_page
      IMPORTING
        !it_keys          TYPE ty_raw_key_tt
        !iv_pagekey       TYPE o2pagdir-pagekey
      EXPORTING
        !es_content       TYPE ty_raw_content
        !es_evhandler     TYPE ty_raw_evhandler
        !es_typesource    TYPE ty_raw_typesource
        !ev_decoded_bytes TYPE int8
      CHANGING
        !ct_rows          TYPE ty_raw_row_tt
      RAISING
        zcx_abapgit_exception.

    CLASS-METHODS raw_prefetch_and_read
      IMPORTING
        !it_pages       TYPE o2pagelist
        !io_files       TYPE REF TO zcl_abapgit_objects_files
        !iv_split_depth TYPE i DEFAULT 0
      CHANGING
        !cs_context     TYPE ty_context
        !ct_pages_info  TYPE ty_pages_tt
      RAISING
        zcx_abapgit_exception.

    "! Groups IT_ROWS by (pagekey, objtype), validates SRTF2 sequence
    "! completeness and CLUSTR truncation, reconstructs one XSTRING per
    "! logical key, and decodes it with IMPORT ... FROM DATA BUFFER using
    "! the exact same field lists as the reference per-key IMPORT path.
    "! Raises ZCX_ABAPGIT_EXCEPTION on ANY anomaly - pure validate-and-
    "! decode, no fallback logic of its own (the caller, TRY_RAW_PREFETCH,
    "! is the only place that decides to fall back).
    CLASS-METHODS assemble_and_decode
      IMPORTING
        !it_keys       TYPE ty_raw_key_tt
        !it_rows       TYPE ty_raw_row_tt
      EXPORTING
        !et_content    TYPE ty_raw_content_tt
        !et_evhandler  TYPE ty_raw_evhandler_tt
        !et_typesource TYPE ty_raw_typesource_tt
      RAISING
        zcx_abapgit_exception.

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


  METHOD get_raw_prefetch_counters.
    ev_hits      = gv_raw_prefetch_hits.
    ev_fallbacks = gv_raw_prefetch_fallbacks.
    ev_manifest_selects      = gv_manifest_selects.
    ev_payload_selects       = gv_payload_selects.
    ev_verify_selects        = gv_verify_selects.
    ev_split_events          = gv_split_events.
    ev_depth_ceiling_hits    = gv_depth_ceiling_hits.
    ev_range_fallbacks       = gv_range_fallbacks.
    ev_range_fallback_pages  = gv_range_fallback_pages.
    ev_decode_fallback_pages = gv_decode_fallback_pages.
    ev_reference_pages       = gv_reference_pages.
    ev_max_manifest_rows     = gv_max_manifest_rows.
    ev_max_charged_raw_bytes = gv_max_charged_raw_bytes.
    ev_max_raw_split_depth   = gv_max_raw_split_depth.
  ENDMETHOD.


  METHOD reset_raw_prefetch_counters.
    CLEAR: gv_raw_prefetch_hits,
           gv_raw_prefetch_fallbacks,
           gv_manifest_selects,
           gv_payload_selects,
           gv_verify_selects,
           gv_split_events,
           gv_depth_ceiling_hits,
           gv_range_fallbacks,
           gv_range_fallback_pages,
           gv_decode_fallback_pages,
           gv_reference_pages,
           gv_max_manifest_rows,
           gv_max_charged_raw_bytes,
           gv_max_raw_split_depth.
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


  METHOD build_requested_keys.

    DATA ls_key   TYPE ty_raw_key.
    DATA ls_pgdir TYPE o2pagdir.

    FIELD-SYMBOLS <ls_page> LIKE LINE OF it_pages.

    LOOP AT it_pages ASSIGNING <ls_page>.
      READ TABLE is_context-page_dirs INTO ls_pgdir
        WITH KEY applname = is_context-name
                 pagekey  = <ls_page>-pagekey.
      IF sy-subrc <> 0.
        " unresolvable page dir - not our concern here, READ_PAGE itself
        " raises the real error; simply do not request raw prefetch for it.
        CONTINUE.
      ENDIF.

      IF ls_pgdir-pagetype = so2_controller.
        CONTINUE.
      ENDIF.

      CLEAR ls_key.
      ls_key-pagekey = <ls_page>-pagekey.

      IF ls_pgdir-pagetype = so2_full_page.
        ls_key-need_types = abap_true.

        READ TABLE is_context-event_handlers TRANSPORTING NO FIELDS
          WITH KEY applname = is_context-name
                   pagekey  = <ls_page>-pagekey
                   version  = c_active.
        ls_key-need_evhndl = boolc( sy-subrc = 0 ).
      ENDIF.

      INSERT ls_key INTO TABLE rt_keys.
    ENDLOOP.

  ENDMETHOD.


  METHOD read_raw_manifest.

    TYPES: BEGIN OF ty_sel_key,
             pagekey TYPE o2pagdir-pagekey,
             objtype TYPE o2pconkey-objtype,
           END OF ty_sel_key.
    DATA lt_sel_keys TYPE SORTED TABLE OF ty_sel_key WITH UNIQUE KEY pagekey objtype.
    DATA ls_sel_key  TYPE ty_sel_key.
    DATA lv_probe_rows TYPE i.
    DATA lv_charged_bytes TYPE int8.
    DATA lv_has_anomaly TYPE abap_bool.
    DATA lv_previous_pagekey TYPE o2pagdir-pagekey.
    DATA lv_previous_objtype TYPE o2pconkey-objtype.
    DATA lv_expected_srtf2 TYPE i.
    DATA lt_page_content TYPE SORTED TABLE OF o2pagdir-pagekey WITH UNIQUE KEY table_line.

    FIELD-SYMBOLS <ls_key> LIKE LINE OF it_keys.
    FIELD-SYMBOLS <ls_manifest> LIKE LINE OF et_manifest.

    CLEAR: et_manifest, ev_admitted, ev_reject_reason.
    IF iv_max_rows <= 0 OR iv_max_rows > c_max_raw_manifest_rows
      OR iv_max_payload_bytes <= 0 OR iv_max_payload_bytes > c_max_raw_payload_bytes.
      zcx_abapgit_exception=>raise( 'WAPA raw manifest: invalid admission limits' ).
    ENDIF.

    LOOP AT it_keys ASSIGNING <ls_key>.
      ls_sel_key-pagekey = <ls_key>-pagekey.
      ls_sel_key-objtype = so2_objtype_page.
      INSERT ls_sel_key INTO TABLE lt_sel_keys.
      IF <ls_key>-need_evhndl = abap_true.
        ls_sel_key-objtype = so2_objtype_evhndl.
        INSERT ls_sel_key INTO TABLE lt_sel_keys.
      ENDIF.
      IF <ls_key>-need_types = abap_true.
        ls_sel_key-objtype = so2_objtype_types.
        INSERT ls_sel_key INTO TABLE lt_sel_keys.
      ENDIF.
    ENDLOOP.

    IF lt_sel_keys IS INITIAL.
      ev_admitted = abap_true.
      RETURN.
    ENDIF.

    lv_probe_rows = iv_max_rows + 1.
    SELECT FROM o2pagcon AS db
      INNER JOIN @lt_sel_keys AS keys
        ON db~pagekey = keys~pagekey
       AND db~objtype = keys~objtype
      FIELDS db~pagekey, db~objtype, db~srtf2, db~clustr
      WHERE db~relid    = 'TR'
        AND db~applname = @iv_name
        AND db~version  = @c_active
      ORDER BY db~pagekey, db~objtype, db~srtf2
      INTO TABLE @et_manifest
      UP TO @lv_probe_rows ROWS.                          "#EC CI_SUBRC

    gv_manifest_selects = gv_manifest_selects + 1.
    gv_max_manifest_rows = nmax( val1 = gv_max_manifest_rows val2 = lines( et_manifest ) ).
    lv_charged_bytes = lines( et_manifest ) * c_raw_payload_row_bytes.
    gv_max_charged_raw_bytes = nmax( val1 = gv_max_charged_raw_bytes val2 = lv_charged_bytes ).

    IF lines( et_manifest ) > iv_max_rows.
      ev_reject_reason = 'R'.
      CLEAR et_manifest.
      RETURN.
    ENDIF.
    IF lv_charged_bytes > iv_max_payload_bytes.
      ev_reject_reason = 'B'.
      CLEAR et_manifest.
      RETURN.
    ENDIF.

    LOOP AT et_manifest ASSIGNING <ls_manifest> USING KEY by_row.
      IF <ls_manifest>-pagekey <> lv_previous_pagekey
        OR <ls_manifest>-objtype <> lv_previous_objtype.
        CLEAR lv_expected_srtf2.
        lv_previous_pagekey = <ls_manifest>-pagekey.
        lv_previous_objtype = <ls_manifest>-objtype.
      ENDIF.
      IF <ls_manifest>-srtf2 <> lv_expected_srtf2
        OR <ls_manifest>-clustr < 0
        OR <ls_manifest>-clustr > c_raw_payload_row_bytes.
        lv_has_anomaly = abap_true.
        EXIT.
      ENDIF.
      lv_expected_srtf2 = lv_expected_srtf2 + 1.
      IF <ls_manifest>-objtype = so2_objtype_page.
        INSERT <ls_manifest>-pagekey INTO TABLE lt_page_content.
      ENDIF.
    ENDLOOP.

    LOOP AT it_keys ASSIGNING <ls_key>.
      READ TABLE lt_page_content TRANSPORTING NO FIELDS
        WITH TABLE KEY table_line = <ls_key>-pagekey.
      IF sy-subrc <> 0.
        lv_has_anomaly = abap_true.
        EXIT.
      ENDIF.
    ENDLOOP.

    IF lv_has_anomaly = abap_true.
      ev_reject_reason = 'A'.
      CLEAR et_manifest.
      RETURN.
    ENDIF.

    ev_admitted = abap_true.

  ENDMETHOD.


  METHOD read_raw_rows.

    DATA lt_verify_manifest TYPE ty_raw_manifest_tt.
    DATA lv_admitted TYPE abap_bool.
    DATA lv_reject_reason TYPE ty_raw_reject_reason.
    DATA lv_charged_bytes TYPE int8.
    DATA lv_payload_limit TYPE i.

    lv_payload_limit = c_max_raw_manifest_rows + 1.

    CLEAR: et_rows, ev_complete.
    IF it_manifest IS INITIAL.
      ev_complete = abap_true.
      RETURN.
    ENDIF.

    SELECT FROM o2pagcon AS db
      INNER JOIN @it_manifest AS manifest
        ON db~pagekey = manifest~pagekey
       AND db~objtype = manifest~objtype
       AND db~srtf2   = manifest~srtf2
       AND db~clustr  = manifest~clustr
      FIELDS db~pagekey, db~objtype, db~srtf2, db~clustr, db~clustd
      WHERE db~relid    = 'TR'
        AND db~applname = @iv_name
        AND db~version  = @c_active
      ORDER BY db~pagekey, db~objtype, db~srtf2
      INTO TABLE @et_rows
      UP TO @lv_payload_limit ROWS.                       "#EC CI_SUBRC

    gv_payload_selects = gv_payload_selects + 1.
    IF lines( et_rows ) <> lines( it_manifest ).
      CLEAR et_rows.
      RETURN.
    ENDIF.

    lv_charged_bytes = lines( et_rows ) * c_raw_payload_row_bytes.
    IF lv_charged_bytes > c_max_raw_payload_bytes.
      CLEAR et_rows.
      RETURN.
    ENDIF.

    read_raw_manifest(
      EXPORTING
        iv_name              = iv_name
        it_keys              = it_keys
        iv_max_rows          = c_max_raw_manifest_rows
        iv_max_payload_bytes = c_max_raw_payload_bytes
      IMPORTING
        et_manifest          = lt_verify_manifest
        ev_admitted          = lv_admitted
        ev_reject_reason     = lv_reject_reason ).
    gv_verify_selects = gv_verify_selects + 1.

    IF lv_admitted = abap_false OR lt_verify_manifest <> it_manifest.
      CLEAR et_rows.
      RETURN.
    ENDIF.

    ev_complete = abap_true.

  ENDMETHOD.


  METHOD assemble_and_decode.

    TYPES: BEGIN OF ty_buffer,
             pagekey    TYPE o2pagdir-pagekey,
             objtype    TYPE o2pconkey-objtype,
             next_srtf2 TYPE i,
             buffer     TYPE xstring,
           END OF ty_buffer.
    DATA lt_buffers TYPE SORTED TABLE OF ty_buffer WITH UNIQUE KEY pagekey objtype.

    DATA lt_sorted      TYPE ty_raw_row_tt.
    DATA lv_total_bytes TYPE i.
    DATA lv_chunk       TYPE xstring.
    DATA ls_content     TYPE ty_raw_content.
    DATA ls_evhandler   TYPE ty_raw_evhandler.
    DATA ls_typesource  TYPE ty_raw_typesource.

    FIELD-SYMBOLS <ls_row>    LIKE LINE OF lt_sorted.
    FIELD-SYMBOLS <ls_buffer> LIKE LINE OF lt_buffers.
    FIELD-SYMBOLS <ls_key>    LIKE LINE OF it_keys.

    CLEAR: et_content, et_evhandler, et_typesource.

    lt_sorted = it_rows.
    SORT lt_sorted BY pagekey objtype srtf2 ASCENDING.

    LOOP AT lt_sorted ASSIGNING <ls_row>.
      READ TABLE lt_buffers ASSIGNING <ls_buffer>
        WITH TABLE KEY pagekey = <ls_row>-pagekey
                       objtype = <ls_row>-objtype.
      IF sy-subrc <> 0.
        INSERT VALUE ty_buffer( pagekey = <ls_row>-pagekey objtype = <ls_row>-objtype )
          INTO TABLE lt_buffers ASSIGNING <ls_buffer>.
      ENDIF.

      IF <ls_row>-srtf2 <> <ls_buffer>-next_srtf2.
        zcx_abapgit_exception=>raise(
          |WAPA raw prefetch: SRTF2 sequence gap/duplicate for { <ls_row>-pagekey }/{ <ls_row>-objtype }| ).
      ENDIF.
      <ls_buffer>-next_srtf2 = <ls_buffer>-next_srtf2 + 1.

      IF <ls_row>-clustr < 0 OR <ls_row>-clustr > xstrlen( <ls_row>-clustd ).
        zcx_abapgit_exception=>raise(
          |WAPA raw prefetch: malformed CLUSTR for { <ls_row>-pagekey }/{ <ls_row>-objtype }| ).
      ENDIF.

      lv_total_bytes = lv_total_bytes + <ls_row>-clustr.
      IF lv_total_bytes > zcl_abapgit_ortec_ser_orch=>c_max_object_output_bytes.
        zcx_abapgit_exception=>raise( 'WAPA raw prefetch: byte cap exceeded' ).
      ENDIF.

      IF <ls_row>-clustr > 0.
        lv_chunk = <ls_row>-clustd(<ls_row>-clustr).
        CONCATENATE <ls_buffer>-buffer lv_chunk INTO <ls_buffer>-buffer IN BYTE MODE.
      ENDIF.
    ENDLOOP.

    LOOP AT lt_buffers ASSIGNING <ls_buffer>.
      CASE <ls_buffer>-objtype.
        WHEN so2_objtype_page.
          CLEAR ls_content.
          ls_content-pagekey = <ls_buffer>-pagekey.
          IMPORT content    TO ls_content-content
                 xml_source TO ls_content-xml_source
                 FROM DATA BUFFER <ls_buffer>-buffer
                 ACCEPTING PADDING
                 IGNORING CONVERSION ERRORS.
          IF sy-subrc <> 0.
            zcx_abapgit_exception=>raise(
              |WAPA raw prefetch: decode failed for { <ls_buffer>-pagekey }/PAGE| ).
          ENDIF.
          INSERT ls_content INTO TABLE et_content.

        WHEN so2_objtype_evhndl.
          CLEAR ls_evhandler.
          ls_evhandler-pagekey = <ls_buffer>-pagekey.
          IMPORT evhandler TO ls_evhandler-evhandler
                 FROM DATA BUFFER <ls_buffer>-buffer
                 ACCEPTING PADDING
                 IGNORING CONVERSION ERRORS.
          " no subrc check - the reference path (ADD_FULL_PAGE_DETAILS)
          " does not check it either; an empty result is tolerated there.
          INSERT ls_evhandler INTO TABLE et_evhandler.

        WHEN so2_objtype_types.
          CLEAR ls_typesource.
          ls_typesource-pagekey = <ls_buffer>-pagekey.
          IMPORT typesource TO ls_typesource-typesource
                 FROM DATA BUFFER <ls_buffer>-buffer
                 ACCEPTING PADDING
                 IGNORING CONVERSION ERRORS.
          " no subrc check - matches the reference path exactly.
          INSERT ls_typesource INTO TABLE et_typesource.

        WHEN OTHERS.
          zcx_abapgit_exception=>raise( |WAPA raw prefetch: unexpected OBJTYPE { <ls_buffer>-objtype }| ).
      ENDCASE.
    ENDLOOP.

    " Every REQUESTED key must end up with a (possibly empty, for the
    " optional EVHNDL/TYPES sub-keys) map entry - a requested PAGE-content
    " key with zero physical rows is a hard anomaly (the reference path
    " raises for a missing PAGE import too); a requested optional
    " EVHNDL/TYPES sub-key with zero physical rows is normal and gets an
    " empty entry inserted here, matching IMPORT's own tolerant behavior
    " for a missing cluster key.
    LOOP AT it_keys ASSIGNING <ls_key>.
      READ TABLE et_content TRANSPORTING NO FIELDS WITH TABLE KEY pagekey = <ls_key>-pagekey.
      IF sy-subrc <> 0.
        zcx_abapgit_exception=>raise(
          |WAPA raw prefetch: no content rows for { <ls_key>-pagekey }| ).
      ENDIF.

      IF <ls_key>-need_evhndl = abap_true.
        READ TABLE et_evhandler TRANSPORTING NO FIELDS WITH TABLE KEY pagekey = <ls_key>-pagekey.
        IF sy-subrc <> 0.
          INSERT VALUE ty_raw_evhandler( pagekey = <ls_key>-pagekey ) INTO TABLE et_evhandler.
        ENDIF.
      ENDIF.

      IF <ls_key>-need_types = abap_true.
        READ TABLE et_typesource TRANSPORTING NO FIELDS WITH TABLE KEY pagekey = <ls_key>-pagekey.
        IF sy-subrc <> 0.
          INSERT VALUE ty_raw_typesource( pagekey = <ls_key>-pagekey ) INTO TABLE et_typesource.
        ENDIF.
      ENDIF.
    ENDLOOP.

  ENDMETHOD.


  METHOD try_raw_prefetch.

    DATA lt_keys        TYPE ty_raw_key_tt.
    DATA lt_manifest    TYPE ty_raw_manifest_tt.
    DATA lt_rows        TYPE ty_raw_row_tt.
    DATA lv_admitted    TYPE abap_bool.
    DATA lv_complete    TYPE abap_bool.
    DATA lv_reject_reason TYPE ty_raw_reject_reason.
    DATA lt_content     TYPE ty_raw_content_tt.
    DATA lt_evhandler   TYPE ty_raw_evhandler_tt.
    DATA lt_typesource  TYPE ty_raw_typesource_tt.

    clear_raw_context( CHANGING cs_context = cs_context ).

    lt_keys = build_requested_keys( it_pages = it_pages is_context = cs_context ).
    IF lt_keys IS INITIAL.
      " nothing needs page content at all (e.g. an all-controller WAPA) -
      " trivially "active" with empty maps; behaviourally identical to
      " the reference path either way, since neither is ever consulted.
      cs_context-raw_prefetch_active = abap_true.
      gv_raw_prefetch_hits = gv_raw_prefetch_hits + 1.
      RETURN.
    ENDIF.

    read_raw_manifest(
      EXPORTING
        iv_name        = cs_context-name
        it_keys        = lt_keys
      IMPORTING
        et_manifest    = lt_manifest
        ev_admitted    = lv_admitted
        ev_reject_reason = lv_reject_reason ).

    IF lv_admitted = abap_false.
      gv_raw_prefetch_fallbacks = gv_raw_prefetch_fallbacks + 1.
      RETURN.
    ENDIF.

    read_raw_rows(
      EXPORTING
        iv_name     = cs_context-name
        it_keys     = lt_keys
        it_manifest = lt_manifest
      IMPORTING
        et_rows     = lt_rows
        ev_complete = lv_complete ).
    IF lv_complete = abap_false.
      gv_raw_prefetch_fallbacks = gv_raw_prefetch_fallbacks + 1.
      RETURN.
    ENDIF.

    TRY.
        assemble_and_decode(
          EXPORTING
            it_keys       = lt_keys
            it_rows       = lt_rows
          IMPORTING
            et_content    = lt_content
            et_evhandler  = lt_evhandler
            et_typesource = lt_typesource ).
      CATCH zcx_abapgit_exception.
        gv_raw_prefetch_fallbacks = gv_raw_prefetch_fallbacks + 1.
        RETURN. " any anomaly - stay on the reference path for the whole WAPA
    ENDTRY.

    cs_context-raw_content         = lt_content.
    cs_context-raw_evhandler       = lt_evhandler.
    cs_context-raw_typesource      = lt_typesource.
    cs_context-raw_prefetch_active = abap_true.
    gv_raw_prefetch_hits = gv_raw_prefetch_hits + 1.

  ENDMETHOD.


  METHOD decode_raw_page.

    DATA ls_key TYPE ty_raw_key.
    DATA lv_objtype TYPE o2pconkey-objtype.
    DATA lv_expected_srtf2 TYPE i.
    DATA lv_fragment TYPE xstring.
    DATA lv_buffer TYPE xstring.
    DATA lt_fragments TYPE STANDARD TABLE OF xstring WITH EMPTY KEY.

    FIELD-SYMBOLS <ls_row> LIKE LINE OF ct_rows.
    FIELD-SYMBOLS <lv_content_line> TYPE any.
    FIELD-SYMBOLS <lv_type_line> TYPE any.
    FIELD-SYMBOLS <ls_evhandler_line> TYPE so2_ev_handler.
    FIELD-SYMBOLS <lv_evhandler_source_line> TYPE any.

    READ TABLE it_keys INTO ls_key WITH TABLE KEY pagekey = iv_pagekey.
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    LOOP AT ct_rows ASSIGNING <ls_row> USING KEY by_page
      WHERE pagekey = iv_pagekey.
      IF lv_objtype IS INITIAL.
        lv_objtype = <ls_row>-objtype.
      ENDIF.
      IF <ls_row>-objtype <> lv_objtype.
        CONCATENATE LINES OF lt_fragments INTO lv_buffer IN BYTE MODE.
        CASE lv_objtype.
          WHEN so2_objtype_page.
            es_content-pagekey = iv_pagekey.
            IMPORT content TO es_content-content
                   xml_source TO es_content-xml_source
                   FROM DATA BUFFER lv_buffer
                   ACCEPTING PADDING
                   IGNORING CONVERSION ERRORS.
            IF sy-subrc <> 0.
              zcx_abapgit_exception=>raise( |WAPA raw prefetch: decode failed for { iv_pagekey }/PAGE| ).
            ENDIF.
          WHEN so2_objtype_evhndl.
            es_evhandler-pagekey = iv_pagekey.
            IMPORT evhandler TO es_evhandler-evhandler
                   FROM DATA BUFFER lv_buffer
                   ACCEPTING PADDING
                   IGNORING CONVERSION ERRORS.
          WHEN so2_objtype_types.
            es_typesource-pagekey = iv_pagekey.
            IMPORT typesource TO es_typesource-typesource
                   FROM DATA BUFFER lv_buffer
                   ACCEPTING PADDING
                   IGNORING CONVERSION ERRORS.
        ENDCASE.
        FREE: lt_fragments, lv_buffer.
        CLEAR lv_expected_srtf2.
        lv_objtype = <ls_row>-objtype.
      ENDIF.

      IF <ls_row>-srtf2 <> lv_expected_srtf2
        OR <ls_row>-clustr < 0 OR <ls_row>-clustr > xstrlen( <ls_row>-clustd ).
        zcx_abapgit_exception=>raise( |WAPA raw prefetch: malformed row for { iv_pagekey }/{ <ls_row>-objtype }| ).
      ENDIF.
      lv_expected_srtf2 = lv_expected_srtf2 + 1.
      IF <ls_row>-clustr > 0.
        lv_fragment = <ls_row>-clustd(<ls_row>-clustr).
        APPEND lv_fragment TO lt_fragments.
      ENDIF.
      DELETE TABLE ct_rows FROM <ls_row>.
    ENDLOOP.

    IF lv_objtype IS INITIAL.
      zcx_abapgit_exception=>raise( |WAPA raw prefetch: no rows for { iv_pagekey }| ).
    ENDIF.
    CONCATENATE LINES OF lt_fragments INTO lv_buffer IN BYTE MODE.
    CASE lv_objtype.
      WHEN so2_objtype_page.
        es_content-pagekey = iv_pagekey.
        IMPORT content TO es_content-content
               xml_source TO es_content-xml_source
               FROM DATA BUFFER lv_buffer
               ACCEPTING PADDING
               IGNORING CONVERSION ERRORS.
        IF sy-subrc <> 0.
          zcx_abapgit_exception=>raise( |WAPA raw prefetch: decode failed for { iv_pagekey }/PAGE| ).
        ENDIF.
      WHEN so2_objtype_evhndl.
        es_evhandler-pagekey = iv_pagekey.
        IMPORT evhandler TO es_evhandler-evhandler
               FROM DATA BUFFER lv_buffer
               ACCEPTING PADDING
               IGNORING CONVERSION ERRORS.
      WHEN so2_objtype_types.
        es_typesource-pagekey = iv_pagekey.
        IMPORT typesource TO es_typesource-typesource
               FROM DATA BUFFER lv_buffer
               ACCEPTING PADDING
               IGNORING CONVERSION ERRORS.
    ENDCASE.

    IF es_content-pagekey IS INITIAL.
      zcx_abapgit_exception=>raise( |WAPA raw prefetch: no decoded content for { iv_pagekey }| ).
    ENDIF.
    IF ls_key-need_evhndl = abap_true AND es_evhandler-pagekey IS INITIAL.
      es_evhandler-pagekey = iv_pagekey.
    ENDIF.
    IF ls_key-need_types = abap_true AND es_typesource-pagekey IS INITIAL.
      es_typesource-pagekey = iv_pagekey.
    ENDIF.

    ev_decoded_bytes = xstrlen( es_content-xml_source ).
    LOOP AT es_content-content ASSIGNING <lv_content_line>.
      ev_decoded_bytes = ev_decoded_bytes + strlen( <lv_content_line> ).
    ENDLOOP.
    LOOP AT es_evhandler-evhandler ASSIGNING <ls_evhandler_line>.
      ev_decoded_bytes = ev_decoded_bytes + strlen( <ls_evhandler_line>-name ).
      LOOP AT <ls_evhandler_line>-source ASSIGNING <lv_evhandler_source_line>.
        ev_decoded_bytes = ev_decoded_bytes + strlen( <lv_evhandler_source_line> ).
      ENDLOOP.
    ENDLOOP.
    LOOP AT es_typesource-typesource ASSIGNING <lv_type_line>.
      ev_decoded_bytes = ev_decoded_bytes + strlen( <lv_type_line> ).
    ENDLOOP.

  ENDMETHOD.


  METHOD raw_prefetch_and_read.

    DATA lt_keys TYPE ty_raw_key_tt.
    DATA lt_manifest TYPE ty_raw_manifest_tt.
    DATA lt_rows TYPE ty_raw_row_tt.
    DATA lt_left TYPE o2pagelist.
    DATA lt_right TYPE o2pagelist.
    DATA lt_suffix TYPE o2pagelist.
    DATA ls_content TYPE ty_raw_content.
    DATA ls_evhandler TYPE ty_raw_evhandler.
    DATA ls_typesource TYPE ty_raw_typesource.
    DATA lv_admitted TYPE abap_bool.
    DATA lv_complete TYPE abap_bool.
    DATA lv_reject_reason TYPE ty_raw_reject_reason.
    DATA lv_decoded_bytes TYPE int8.
    DATA lv_mid TYPE i.
    DATA lv_page_index TYPE i.

    FIELD-SYMBOLS <ls_page> LIKE LINE OF it_pages.
    FIELD-SYMBOLS <ls_remaining> LIKE LINE OF it_pages.

    clear_raw_context( CHANGING cs_context = cs_context ).
    IF iv_split_depth > c_max_raw_split_depth.
      zcx_abapgit_exception=>raise( 'WAPA raw prefetch: split depth exceeded' ).
    ENDIF.
    gv_max_raw_split_depth = nmax( val1 = gv_max_raw_split_depth val2 = iv_split_depth ).

    lt_keys = build_requested_keys( it_pages = it_pages is_context = cs_context ).
    IF lt_keys IS INITIAL.
      LOOP AT it_pages ASSIGNING <ls_page>.
        APPEND read_page( is_context = cs_context is_page = <ls_page> io_files = io_files )
          TO ct_pages_info.
      ENDLOOP.
      RETURN.
    ENDIF.

    read_raw_manifest(
      EXPORTING
        iv_name     = cs_context-name
        it_keys     = lt_keys
      IMPORTING
        et_manifest = lt_manifest
        ev_admitted = lv_admitted
        ev_reject_reason = lv_reject_reason ).
    IF lv_admitted = abap_true.
      read_raw_rows(
        EXPORTING
          iv_name     = cs_context-name
          it_keys     = lt_keys
          it_manifest = lt_manifest
        IMPORTING
          et_rows     = lt_rows
          ev_complete = lv_complete ).
      lv_admitted = lv_complete.
    ENDIF.

    IF lv_admitted = abap_true.
      LOOP AT it_pages ASSIGNING <ls_page>.
        lv_page_index = sy-tabix.
        CLEAR: ls_content, ls_evhandler, ls_typesource, lv_decoded_bytes.
        TRY.
            decode_raw_page(
              EXPORTING it_keys = lt_keys iv_pagekey = <ls_page>-pagekey
              IMPORTING es_content = ls_content es_evhandler = ls_evhandler
                        es_typesource = ls_typesource ev_decoded_bytes = lv_decoded_bytes
              CHANGING ct_rows = lt_rows ).
          CATCH zcx_abapgit_exception.
            CLEAR lv_decoded_bytes.
        ENDTRY.

        IF ls_content-pagekey IS INITIAL OR lv_decoded_bytes > c_max_decoded_page_bytes.
          FREE: lt_rows, lt_manifest.
          clear_raw_context( CHANGING cs_context = cs_context ).
          LOOP AT it_pages ASSIGNING <ls_remaining> FROM lv_page_index.
            APPEND <ls_remaining> TO lt_suffix.
          ENDLOOP.
          gv_decode_fallback_pages = gv_decode_fallback_pages + lines( lt_suffix ).
          serialize_reference_range(
            EXPORTING it_pages = lt_suffix io_files = io_files
            CHANGING cs_context = cs_context ct_pages_info = ct_pages_info ).
          RETURN.
        ENDIF.

        clear_raw_context( CHANGING cs_context = cs_context ).
        INSERT ls_content INTO TABLE cs_context-raw_content.
        IF ls_evhandler-pagekey IS NOT INITIAL.
          INSERT ls_evhandler INTO TABLE cs_context-raw_evhandler.
        ENDIF.
        IF ls_typesource-pagekey IS NOT INITIAL.
          INSERT ls_typesource INTO TABLE cs_context-raw_typesource.
        ENDIF.
        cs_context-raw_prefetch_active = abap_true.
        APPEND read_page( is_context = cs_context is_page = <ls_page> io_files = io_files )
          TO ct_pages_info.
        clear_raw_context( CHANGING cs_context = cs_context ).
      ENDLOOP.
      FREE: lt_rows, lt_manifest.
      RETURN.
    ENDIF.

    FREE: lt_rows, lt_manifest.
    clear_raw_context( CHANGING cs_context = cs_context ).
    IF lines( it_pages ) > 1 AND iv_split_depth < c_max_raw_split_depth.
      lv_mid = lines( it_pages ) / 2.
      LOOP AT it_pages ASSIGNING <ls_page>.
        IF sy-tabix <= lv_mid.
          APPEND <ls_page> TO lt_left.
        ELSE.
          APPEND <ls_page> TO lt_right.
        ENDIF.
      ENDLOOP.
      gv_split_events = gv_split_events + 1.
      raw_prefetch_and_read(
        EXPORTING it_pages = lt_left io_files = io_files iv_split_depth = iv_split_depth + 1
        CHANGING cs_context = cs_context ct_pages_info = ct_pages_info ).
      clear_raw_context( CHANGING cs_context = cs_context ).
      raw_prefetch_and_read(
        EXPORTING it_pages = lt_right io_files = io_files iv_split_depth = iv_split_depth + 1
        CHANGING cs_context = cs_context ct_pages_info = ct_pages_info ).
      RETURN.
    ENDIF.

    IF lines( it_pages ) > 1 AND iv_split_depth = c_max_raw_split_depth.
      gv_depth_ceiling_hits = gv_depth_ceiling_hits + 1.
    ENDIF.
    serialize_reference_range(
      EXPORTING it_pages = it_pages io_files = io_files
      CHANGING cs_context = cs_context ct_pages_info = ct_pages_info ).

  ENDMETHOD.


  METHOD clear_raw_context.

    CLEAR cs_context-raw_prefetch_active.
    FREE: cs_context-raw_content,
          cs_context-raw_evhandler,
          cs_context-raw_typesource.

  ENDMETHOD.


  METHOD serialize_reference_range.

    FIELD-SYMBOLS <ls_page> LIKE LINE OF it_pages.

    gv_range_fallbacks = gv_range_fallbacks + 1.
    gv_range_fallback_pages = gv_range_fallback_pages + lines( it_pages ).

    LOOP AT it_pages ASSIGNING <ls_page>.
      clear_raw_context( CHANGING cs_context = cs_context ).
      APPEND read_page(
               is_context = cs_context
               is_page    = <ls_page>
               io_files   = io_files )
             TO ct_pages_info.
      gv_reference_pages = gv_reference_pages + 1.
      clear_raw_context( CHANGING cs_context = cs_context ).
    ENDLOOP.

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
    DATA lv_have_content      TYPE abap_bool.

    FIELD-SYMBOLS <ls_raw_content> TYPE ty_raw_content.

    IF is_context-raw_prefetch_active = abap_true.
      READ TABLE is_context-raw_content ASSIGNING <ls_raw_content>
        WITH TABLE KEY pagekey = cs_page-attributes-pagekey.
      IF sy-subrc = 0.
        lt_content      = <ls_raw_content>-content.
        lv_xml_source   = <ls_raw_content>-xml_source.
        lv_have_content = abap_true.
      ENDIF.
    ENDIF.

    IF lv_have_content = abap_false.
      " #SER-FINAL fallback: also the unconditional path when raw
      " prefetch is inactive for this WAPA - byte-for-byte the original
      " reference behaviour.
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
    DATA lv_have_evhandler     TYPE abap_bool.
    DATA lv_have_typesource    TYPE abap_bool.

    FIELD-SYMBOLS <ls_ev_handler_source> TYPE so2_ev_handler.
    FIELD-SYMBOLS <ls_raw_evhandler> TYPE ty_raw_evhandler.
    FIELD-SYMBOLS <ls_raw_typesource> TYPE ty_raw_typesource.

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
      IF is_context-raw_prefetch_active = abap_true.
        READ TABLE is_context-raw_evhandler ASSIGNING <ls_raw_evhandler>
          WITH TABLE KEY pagekey = cs_page-attributes-pagekey.
        IF sy-subrc = 0.
          lt_ev_handler_sources = <ls_raw_evhandler>-evhandler.
          lv_have_evhandler = abap_true.
        ENDIF.
      ENDIF.

      IF lv_have_evhandler = abap_false.
        " #SER-FINAL fallback: also the unconditional path when raw
        " prefetch is inactive for this WAPA - byte-for-byte the
        " original reference behaviour.
        ls_pagecon_key-objtype = so2_objtype_evhndl.
        IMPORT evhandler TO lt_ev_handler_sources
               FROM DATABASE o2pagcon(tr) ID ls_pagecon_key
               ACCEPTING PADDING
               IGNORING CONVERSION ERRORS.
      ENDIF.

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

    IF is_context-raw_prefetch_active = abap_true.
      READ TABLE is_context-raw_typesource ASSIGNING <ls_raw_typesource>
        WITH TABLE KEY pagekey = cs_page-attributes-pagekey.
      IF sy-subrc = 0.
        cs_page-types = <ls_raw_typesource>-typesource.
        lv_have_typesource = abap_true.
      ENDIF.
    ENDIF.

    IF lv_have_typesource = abap_false.
      ls_pagecon_key-objtype = so2_objtype_types.
      IMPORT typesource TO cs_page-types
             FROM DATABASE o2pagcon(tr) ID ls_pagecon_key
             ACCEPTING PADDING
             IGNORING CONVERSION ERRORS.
    ENDIF.

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
    DATA lt_page_group TYPE o2pagelist.
    DATA lv_reference_pages_before TYPE i.

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

    lv_reference_pages_before = gv_reference_pages.

    LOOP AT lt_pages ASSIGNING <ls_page>.
      APPEND <ls_page> TO lt_page_group.
      IF lines( lt_page_group ) = c_raw_prefetch_initial_pages.
        raw_prefetch_and_read(
          EXPORTING it_pages = lt_page_group io_files = io_files
          CHANGING cs_context = ls_context ct_pages_info = lt_pages_info ).
        CLEAR lt_page_group.
      ENDIF.
    ENDLOOP.

    IF lt_page_group IS NOT INITIAL.
      raw_prefetch_and_read(
        EXPORTING it_pages = lt_page_group io_files = io_files
        CHANGING cs_context = ls_context ct_pages_info = lt_pages_info ).
    ENDIF.

    IF lt_pages IS NOT INITIAL.
      IF gv_reference_pages > lv_reference_pages_before.
        gv_raw_prefetch_fallbacks = gv_raw_prefetch_fallbacks + 1.
      ELSE.
        gv_raw_prefetch_hits = gv_raw_prefetch_hits + 1.
      ENDIF.
    ENDIF.

    io_xml->add( iv_name = 'PAGES'
                 ig_data = lt_pages_info ).

    clear_raw_context( CHANGING cs_context = ls_context ).
    CLEAR: ls_context, lt_pages_info, lt_pages, lt_navgraph, lt_page_group.

    zcl_abapgit_sotr_handler=>read_sotr(
      iv_pgmid       = 'LIMU'
      iv_object      = 'WAPP'
      iv_obj_name    = is_item-obj_name
      io_i18n_params = io_i18n_params
      io_xml         = io_xml ).

  ENDMETHOD.

ENDCLASS.

