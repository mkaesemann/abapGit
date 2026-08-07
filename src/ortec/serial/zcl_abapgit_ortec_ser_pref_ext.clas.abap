"! <p class="shorttext synchronized">ORTEC serializer prefetch extension</p>
"! Holds additional per-type data loaded before the abapGit serialization loop.
CLASS zcl_abapgit_ortec_ser_pref_ext DEFINITION
  PUBLIC FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.
    TYPES ty_enlfdir_tt TYPE STANDARD TABLE OF enlfdir WITH DEFAULT KEY.
    TYPES ty_tpool_i18n_tt
      TYPE STANDARD TABLE OF zif_abapgit_lang_definitions=>ty_i18n_tpool
      WITH DEFAULT KEY.

    TYPES:
      BEGIN OF ty_smim_phf_data,
        file_name TYPE smimphf-file_name,
        mimetype  TYPE smimphf-mimetype,
      END OF ty_smim_phf_data.

    TYPES:
      BEGIN OF ty_tobj_data,
        tddat TYPE tddat,
        tvdir TYPE tvdir,
        tvimf TYPE STANDARD TABLE OF tvimf WITH DEFAULT KEY,
      END OF ty_tobj_data.

    TYPES:
      BEGIN OF ty_tran_data,
        tstct      TYPE tstct,
        tstcp      TYPE tstcp,
        tstca      TYPE STANDARD TABLE OF tstca WITH DEFAULT KEY,
        tstct_i18n TYPE STANDARD TABLE OF tstct WITH DEFAULT KEY,
      END OF ty_tran_data.

    TYPES:
      BEGIN OF ty_fugr_func_meta,
        funcname          TYPE rs38l_fnam,
        exception_classes TYPE abap_bool,
      END OF ty_fugr_func_meta.

    TYPES:
      BEGIN OF ty_dtel_i18n_text,
        ddlanguage TYPE dd04t-ddlanguage,
        ddtext     TYPE dd04t-ddtext,
        reptext    TYPE dd04t-reptext,
        scrtext_s  TYPE dd04t-scrtext_s,
        scrtext_m  TYPE dd04t-scrtext_m,
        scrtext_l  TYPE dd04t-scrtext_l,
      END OF ty_dtel_i18n_text.
    TYPES ty_dtel_i18n_texts TYPE STANDARD TABLE OF ty_dtel_i18n_text
      WITH DEFAULT KEY.

    "! SER-SLICE-3: DOMA translation-language DD01V rows, one per
    "! (domain, language) - see GET_DOMA_I18N.
    TYPES ty_dd01v_i18n_tt TYPE STANDARD TABLE OF dd01v WITH DEFAULT KEY.

    CLASS-METHODS prepare
      IMPORTING
        it_tadir    TYPE zif_abapgit_definitions=>ty_tadir_tt
        iv_language TYPE spras.

    CLASS-METHODS clear.

    CLASS-METHODS get_dtel_data
      IMPORTING
        iv_rollname     TYPE dd04l-rollname
        iv_language     TYPE spras
      EXPORTING
        es_dd04v        TYPE dd04v
      RETURNING
        VALUE(rv_found) TYPE abap_bool.

    "! Return prefetched DTEL i18n data (translation languages + text fields).
    CLASS-METHODS get_dtel_i18n
      IMPORTING
        iv_rollname      TYPE dd04l-rollname
        iv_language      TYPE spras
      EXPORTING
        et_i18n_langs    TYPE zcl_abapgit_ortec_ser_pref=>ty_langu_tt
        et_dtel_texts    TYPE ty_dtel_i18n_texts
      RETURNING
        VALUE(rv_found)  TYPE abap_bool.

    "! SER-SLICE-3 (serialization_slice_3_provider_contract.md &sect;1):
    "! main-language DOMA header/fixed-values, the exact IMPORTING shape
    "! ZCL_ABAPGIT_OBJECT_DOMA's own DDIF_DOMA_GET call receives today.
    CLASS-METHODS get_doma_data
      IMPORTING
        iv_domname      TYPE dd01l-domname
        iv_language     TYPE spras
      EXPORTING
        es_dd01v        TYPE dd01v
        et_dd07v_tab    TYPE dd07v_tab
      RETURNING
        VALUE(rv_found) TYPE abap_bool.

    "! SER-SLICE-3: prefetched DOMA i18n data (translation languages +
    "! per-language DD01V/DD07V rows), mirroring GET_DTEL_I18N.
    CLASS-METHODS get_doma_i18n
      IMPORTING
        iv_domname      TYPE dd01l-domname
      EXPORTING
        et_i18n_langs   TYPE zcl_abapgit_ortec_ser_pref=>ty_langu_tt
        et_dd01v_i18n   TYPE ty_dd01v_i18n_tt
        et_dd07v_i18n   TYPE dd07v_tab
      RETURNING
        VALUE(rv_found) TYPE abap_bool.

    CLASS-METHODS get_enhs_abap_language_vers
      IMPORTING
        iv_enhspot               TYPE enhspotname
      EXPORTING
        ev_abap_language_version TYPE uccheck
      RETURNING
        VALUE(rv_found)          TYPE abap_bool.

    CLASS-METHODS get_fugr_areat
      IMPORTING
        iv_area         TYPE tlibt-area
        iv_language     TYPE spras
      EXPORTING
        ev_areat        TYPE tlibt-areat
      RETURNING
        VALUE(rv_found) TYPE abap_bool.

    CLASS-METHODS get_fugr_enlfdir
      IMPORTING
        iv_area         TYPE enlfdir-area
      EXPORTING
        et_enlfdir      TYPE ty_enlfdir_tt
      RETURNING
        VALUE(rv_found) TYPE abap_bool.

    CLASS-METHODS get_fugr_func_metadata
      IMPORTING
        iv_funcname     TYPE rs38l_fnam
      EXPORTING
        es_metadata     TYPE ty_fugr_func_meta
      RETURNING
        VALUE(rv_found) TYPE abap_bool.

    CLASS-METHODS get_prog_tpool_languages
      IMPORTING
        iv_program      TYPE d010tinf-prog
        iv_language     TYPE spras
      EXPORTING
        et_tpool_i18n   TYPE ty_tpool_i18n_tt
      RETURNING
        VALUE(rv_found) TYPE abap_bool.

    CLASS-METHODS get_smim_loio
      IMPORTING
        iv_loio_id      TYPE smimloio-loio_id
      EXPORTING
        es_smimloio     TYPE smimloio
      RETURNING
        VALUE(rv_found) TYPE abap_bool.

    CLASS-METHODS get_smim_phf
      IMPORTING
        iv_loio_id      TYPE smimphf-loio_id
        iv_phio_id      TYPE smimphf-phio_id
      EXPORTING
        es_data         TYPE ty_smim_phf_data
      RETURNING
        VALUE(rv_found) TYPE abap_bool.

    CLASS-METHODS get_tobj_data
      IMPORTING
        iv_tabname      TYPE vim_name
      EXPORTING
        es_data         TYPE ty_tobj_data
      RETURNING
        VALUE(rv_found) TYPE abap_bool.

    CLASS-METHODS get_tran_data
      IMPORTING
        iv_tcode        TYPE tstc-tcode
        iv_language     TYPE spras
      EXPORTING
        es_data         TYPE ty_tran_data
      RETURNING
        VALUE(rv_found) TYPE abap_bool.

    "! Extract prefetch data relevant to a single TADIR object into a transferable buffer.
    CLASS-METHODS extract_for_object
      IMPORTING is_tadir         TYPE zif_abapgit_definitions=>ty_tadir
      RETURNING VALUE(rv_buffer) TYPE xstring.

    "! Inject prefetch data from a buffer into the session-local caches.
    CLASS-METHODS inject_from_buffer
      IMPORTING iv_buffer TYPE xstring.

    "! SER-SLICE-3 (serialization_slice_3_provider_contract.md &sect;2/&sect;4):
    "! extract the DOMA/DTEL batch prefetch envelope for an entire
    "! dispatch's TADIR rows in ONE call (as opposed to EXTRACT_FOR_OBJECT's
    "! one-object-at-a-time shape). Filters IT_OBJECT_KEYS to DOMA/DTEL
    "! rows internally; returns an INITIAL buffer with no DB access when
    "! none are present.
    CLASS-METHODS extract_for_batch
      IMPORTING it_object_keys   TYPE zif_abapgit_definitions=>ty_tadir_tt
      RETURNING VALUE(rv_buffer) TYPE xstring.

    "! SER-SLICE-3: inject a DOMA/DTEL batch prefetch envelope (produced by
    "! EXTRACT_FOR_BATCH) into this session's caches. Unknown wire format
    "! version, a failed IMPORT, or a duplicate ENTRIES row all reject the
    "! WHOLE buffer by raising ZCX_ABAPGIT_EXCEPTION - callers must treat
    "! this as a full prefetch MISS for this buffer only, never propagate
    "! it into aborting the batch.
    CLASS-METHODS inject_batch_from_buffer
      IMPORTING iv_buffer TYPE xstring
      RAISING   zcx_abapgit_exception.

    "! SER-SLICE-3 parity incident fix (serialization_slice_3_dtel_doma_
    "! parity.md, AR-3-001): unconditionally clears MT_DOMA/MT_DTEL, unlike
    "! INJECT_BATCH_FROM_BUFFER which only clears as a side effect of a
    "! successful import. A pooled/reused RFC worker session must never
    "! carry DOMA/DTEL data from a PRIOR dispatch into a batch whose OWN
    "! IV_PREFETCH_BUFFER_DD is legitimately empty (e.g. a batch with no
    "! DOMA/DTEL objects at all) - callers must call this FIRST, on EVERY
    "! worker invocation, before conditionally injecting a new buffer.
    CLASS-METHODS clear_dd_cache.

  PRIVATE SECTION.
    TYPES ty_dtel_keys TYPE HASHED TABLE OF dd04l-rollname
      WITH UNIQUE KEY table_line.
    TYPES ty_doma_keys TYPE HASHED TABLE OF dd01l-domname
      WITH UNIQUE KEY table_line.
    TYPES ty_enhs_keys TYPE HASHED TABLE OF enhspotname
      WITH UNIQUE KEY table_line.
    TYPES ty_fugr_keys TYPE HASHED TABLE OF tlibt-area
      WITH UNIQUE KEY table_line.
    TYPES ty_prog_keys TYPE HASHED TABLE OF d010tinf-prog
      WITH UNIQUE KEY table_line.
    TYPES ty_smim_keys TYPE HASHED TABLE OF smimloio-loio_id
      WITH UNIQUE KEY table_line.
    TYPES ty_tobj_keys TYPE HASHED TABLE OF vim_name
      WITH UNIQUE KEY table_line.
    TYPES ty_tran_keys TYPE HASHED TABLE OF tstc-tcode
      WITH UNIQUE KEY table_line.

    TYPES:
      BEGIN OF ty_dtel_cache,
        rollname   TYPE dd04l-rollname,
        dd04v      TYPE dd04v,
        dd04t_i18n TYPE STANDARD TABLE OF dd04t WITH DEFAULT KEY,
      END OF ty_dtel_cache.
    TYPES ty_dtel_cache_tt TYPE HASHED TABLE OF ty_dtel_cache
      WITH UNIQUE KEY rollname.

    "! SER-SLICE-3 (serialization_slice_3_provider_contract.md &sect;1,
    "! DR-001): MERGED, DD01V/DD07V-shaped cache rows - the exact same
    "! shape ZCL_ABAPGIT_OBJECT_DOMA's own DDIF_DOMA_GET call receives
    "! today, never raw DD01L/DD07L rows.
    TYPES:
      BEGIN OF ty_doma_cache,
        domname        TYPE dd01l-domname,
        dd01v          TYPE dd01v,
        dd01v_i18n     TYPE ty_dd01v_i18n_tt,
        dd07v_tab      TYPE dd07v_tab,
        dd07v_tab_i18n TYPE STANDARD TABLE OF dd07v WITH DEFAULT KEY,
      END OF ty_doma_cache.
    TYPES ty_doma_cache_tt TYPE HASHED TABLE OF ty_doma_cache
      WITH UNIQUE KEY domname.

    TYPES:
      BEGIN OF ty_enhs_cache,
        enhspot               TYPE enhspotname,
        abap_language_version TYPE uccheck,
      END OF ty_enhs_cache.
    TYPES ty_enhs_cache_tt TYPE HASHED TABLE OF ty_enhs_cache
      WITH UNIQUE KEY enhspot.

    TYPES:
      BEGIN OF ty_fugr_areat_cache,
        area  TYPE tlibt-area,
        areat TYPE tlibt-areat,
      END OF ty_fugr_areat_cache.
    TYPES ty_fugr_areat_cache_tt TYPE HASHED TABLE OF ty_fugr_areat_cache
      WITH UNIQUE KEY area.

    TYPES:
      BEGIN OF ty_fugr_enlfdir_cache,
        area    TYPE enlfdir-area,
        enlfdir TYPE ty_enlfdir_tt,
      END OF ty_fugr_enlfdir_cache.
    TYPES ty_fugr_enlfdir_cache_tt TYPE HASHED TABLE OF ty_fugr_enlfdir_cache
      WITH UNIQUE KEY area.

    TYPES ty_fugr_func_meta_tt TYPE HASHED TABLE OF ty_fugr_func_meta
      WITH UNIQUE KEY funcname.

    TYPES:
      BEGIN OF ty_prog_lang_cache,
        program    TYPE d010tinf-prog,
        tpool_i18n TYPE ty_tpool_i18n_tt,
      END OF ty_prog_lang_cache.
    TYPES ty_prog_lang_cache_tt TYPE HASHED TABLE OF ty_prog_lang_cache
      WITH UNIQUE KEY program.

    TYPES:
      BEGIN OF ty_smim_loio_cache,
        loio_id  TYPE smimloio-loio_id,
        smimloio TYPE smimloio,
      END OF ty_smim_loio_cache.
    TYPES ty_smim_loio_cache_tt TYPE HASHED TABLE OF ty_smim_loio_cache
      WITH UNIQUE KEY loio_id.

    TYPES:
      BEGIN OF ty_smim_phf_cache,
        loio_id TYPE smimphf-loio_id,
        phio_id TYPE smimphf-phio_id,
        data    TYPE ty_smim_phf_data,
      END OF ty_smim_phf_cache.
    TYPES ty_smim_phf_cache_tt TYPE HASHED TABLE OF ty_smim_phf_cache
      WITH UNIQUE KEY loio_id phio_id.

    TYPES:
      BEGIN OF ty_tobj_cache,
        tabname TYPE vim_name,
        data    TYPE ty_tobj_data,
      END OF ty_tobj_cache.
    TYPES ty_tobj_cache_tt TYPE HASHED TABLE OF ty_tobj_cache
      WITH UNIQUE KEY tabname.

    TYPES:
      BEGIN OF ty_tran_cache,
        tcode TYPE tstc-tcode,
        data  TYPE ty_tran_data,
      END OF ty_tran_cache.
    TYPES ty_tran_cache_tt TYPE HASHED TABLE OF ty_tran_cache
      WITH UNIQUE KEY tcode.

    TYPES:
      BEGIN OF ty_d010tinf_lang,
        prog     TYPE d010tinf-prog,
        language TYPE d010tinf-language,
      END OF ty_d010tinf_lang.
    TYPES ty_d010tinf_lang_tt TYPE STANDARD TABLE OF ty_d010tinf_lang
      WITH DEFAULT KEY.

    CLASS-DATA mt_dtel TYPE ty_dtel_cache_tt.
    CLASS-DATA mt_doma TYPE ty_doma_cache_tt.
    CLASS-DATA mt_enhs TYPE ty_enhs_cache_tt.
    CLASS-DATA mt_fugr_areat TYPE ty_fugr_areat_cache_tt.
    CLASS-DATA mt_fugr_enlfdir TYPE ty_fugr_enlfdir_cache_tt.
    CLASS-DATA mt_fugr_func_meta TYPE ty_fugr_func_meta_tt.
    CLASS-DATA mt_prog_langs TYPE ty_prog_lang_cache_tt.
    CLASS-DATA mt_smim_loio TYPE ty_smim_loio_cache_tt.
    CLASS-DATA mt_smim_phf TYPE ty_smim_phf_cache_tt.
    CLASS-DATA mt_tobj TYPE ty_tobj_cache_tt.
    CLASS-DATA mt_tran TYPE ty_tran_cache_tt.
    CLASS-DATA mv_language TYPE spras.

    CLASS-METHODS collect_keys
      IMPORTING
        it_tadir TYPE zif_abapgit_definitions=>ty_tadir_tt
      EXPORTING
        et_dtel  TYPE ty_dtel_keys
        et_doma  TYPE ty_doma_keys
        et_enhs  TYPE ty_enhs_keys
        et_fugr  TYPE ty_fugr_keys
        et_prog  TYPE ty_prog_keys
        et_smim  TYPE ty_smim_keys
        et_tobj  TYPE ty_tobj_keys
        et_tran  TYPE ty_tran_keys.

    CLASS-METHODS get_fugr_main_program
      IMPORTING
        iv_area           TYPE rs38l-area
      RETURNING
        VALUE(rv_program) TYPE d010tinf-prog.

    CLASS-METHODS prepare_dtel
      IMPORTING
        it_names TYPE ty_dtel_keys.
    "! SER-SLICE-3 (serialization_slice_3_provider_contract.md &sect;1):
    "! decision-free bulk-read mirror of what DDIF_DOMA_GET itself would do
    "! for every domain in IT_NAMES, main language and every translation
    "! language, as one bulk read instead of N function-module calls.
    CLASS-METHODS prepare_doma
      IMPORTING
        it_names         TYPE ty_doma_keys
        iv_main_language TYPE spras.
    CLASS-METHODS prepare_enhs
      IMPORTING
        it_names TYPE ty_enhs_keys.
    CLASS-METHODS prepare_fugr
      IMPORTING
        it_areas TYPE ty_fugr_keys.
    CLASS-METHODS prepare_prog_langs
      IMPORTING
        it_programs TYPE ty_prog_keys
        iv_language TYPE spras.
    CLASS-METHODS prepare_smim
      IMPORTING
        it_loio_ids TYPE ty_smim_keys.
    CLASS-METHODS prepare_tobj
      IMPORTING
        it_tabnames TYPE ty_tobj_keys.
    CLASS-METHODS prepare_tran
      IMPORTING
        it_tcodes   TYPE ty_tran_keys
        iv_language TYPE spras.
ENDCLASS.

CLASS zcl_abapgit_ortec_ser_pref_ext IMPLEMENTATION.
  METHOD clear.
    CLEAR mt_dtel.
    CLEAR mt_doma.
    CLEAR mt_enhs.
    CLEAR mt_fugr_areat.
    CLEAR mt_fugr_enlfdir.
    CLEAR mt_fugr_func_meta.
    CLEAR mt_prog_langs.
    CLEAR mt_smim_loio.
    CLEAR mt_smim_phf.
    CLEAR mt_tobj.
    CLEAR mt_tran.
    CLEAR mv_language.
  ENDMETHOD.

  METHOD collect_keys.
    DATA lv_area TYPE rs38l-area.
    DATA lv_loio TYPE smimloio-loio_id.
    DATA lv_program TYPE d010tinf-prog.
    DATA lv_rollname TYPE dd04l-rollname.
    DATA lv_tabname TYPE vim_name.
    DATA lv_tcode TYPE tstc-tcode.
    DATA lv_length TYPE i.

    LOOP AT it_tadir INTO DATA(ls_tadir).
      CASE ls_tadir-object.
        WHEN 'DTEL'.
          lv_rollname = ls_tadir-obj_name.
          INSERT lv_rollname INTO TABLE et_dtel.
        WHEN 'DOMA'.
          INSERT CONV dd01l-domname( ls_tadir-obj_name ) INTO TABLE et_doma.
        WHEN 'ENHS'.
          INSERT CONV enhspotname( ls_tadir-obj_name ) INTO TABLE et_enhs.
        WHEN 'FUGR'.
          lv_area = ls_tadir-obj_name.
          INSERT lv_area INTO TABLE et_fugr.
          lv_program = get_fugr_main_program( lv_area ).
          IF lv_program IS NOT INITIAL.
            INSERT lv_program INTO TABLE et_prog.
          ENDIF.
        WHEN 'PROG'.
          lv_program = ls_tadir-obj_name.
          INSERT lv_program INTO TABLE et_prog.
        WHEN 'SMIM'.
          lv_loio = ls_tadir-obj_name.
          INSERT lv_loio INTO TABLE et_smim.
        WHEN 'TOBJ'.
          lv_length = strlen( ls_tadir-obj_name ) - 1.
          IF lv_length > 0.
            lv_tabname = ls_tadir-obj_name(lv_length).
            INSERT lv_tabname INTO TABLE et_tobj.
          ENDIF.
        WHEN 'TRAN'.
          lv_tcode = ls_tadir-obj_name.
          INSERT lv_tcode INTO TABLE et_tran.
      ENDCASE.
    ENDLOOP.
  ENDMETHOD.

  METHOD get_dtel_data.
    CLEAR es_dd04v.
    IF iv_language <> mv_language.
      RETURN.
    ENDIF.

    READ TABLE mt_dtel INTO DATA(ls_dtel) WITH TABLE KEY rollname = iv_rollname.
    IF sy-subrc = 0.
      es_dd04v = ls_dtel-dd04v.
      rv_found = abap_true.
    ENDIF.
  ENDMETHOD.

  METHOD get_dtel_i18n.
    CLEAR: et_i18n_langs, et_dtel_texts.

    READ TABLE mt_dtel INTO DATA(ls_dtel) WITH TABLE KEY rollname = iv_rollname.
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    " Build language list and text entries from stored dd04t translations
    LOOP AT ls_dtel-dd04t_i18n INTO DATA(ls_dd04t)
      WHERE ddlanguage <> iv_language.
      APPEND VALUE ty_dtel_i18n_text(
        ddlanguage = ls_dd04t-ddlanguage
        ddtext     = ls_dd04t-ddtext
        reptext    = ls_dd04t-reptext
        scrtext_s  = ls_dd04t-scrtext_s
        scrtext_m  = ls_dd04t-scrtext_m
        scrtext_l  = ls_dd04t-scrtext_l ) TO et_dtel_texts.
      APPEND ls_dd04t-ddlanguage TO et_i18n_langs.
    ENDLOOP.

    SORT et_i18n_langs ASCENDING.
    DELETE ADJACENT DUPLICATES FROM et_i18n_langs.
    SORT et_dtel_texts BY ddlanguage ASCENDING.

    rv_found = abap_true.
  ENDMETHOD.

  METHOD get_doma_data.
    CLEAR: es_dd01v, et_dd07v_tab.
    IF iv_language <> mv_language.
      RETURN.
    ENDIF.

    READ TABLE mt_doma INTO DATA(ls_doma) WITH TABLE KEY domname = iv_domname.
    IF sy-subrc = 0.
      es_dd01v     = ls_doma-dd01v.
      et_dd07v_tab = ls_doma-dd07v_tab.
      rv_found     = abap_true.
    ENDIF.
  ENDMETHOD.

  METHOD get_doma_i18n.
    CLEAR: et_i18n_langs, et_dd01v_i18n, et_dd07v_i18n.

    READ TABLE mt_doma INTO DATA(ls_doma) WITH TABLE KEY domname = iv_domname.
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    et_dd01v_i18n = ls_doma-dd01v_i18n.
    et_dd07v_i18n = ls_doma-dd07v_tab_i18n.

    LOOP AT ls_doma-dd01v_i18n INTO DATA(ls_dd01v_i18n).
      APPEND ls_dd01v_i18n-ddlanguage TO et_i18n_langs.
    ENDLOOP.
    SORT et_i18n_langs ASCENDING.
    DELETE ADJACENT DUPLICATES FROM et_i18n_langs.

    rv_found = abap_true.
  ENDMETHOD.

  METHOD get_enhs_abap_language_vers.
    CLEAR ev_abap_language_version.
    READ TABLE mt_enhs INTO DATA(ls_enhs) WITH TABLE KEY enhspot = iv_enhspot.
    IF sy-subrc = 0.
      ev_abap_language_version = ls_enhs-abap_language_version.
      rv_found = abap_true.
    ENDIF.
  ENDMETHOD.

  METHOD get_fugr_areat.
    CLEAR ev_areat.
    IF iv_language <> mv_language.
      RETURN.
    ENDIF.

    READ TABLE mt_fugr_areat INTO DATA(ls_areat) WITH TABLE KEY area = iv_area.
    IF sy-subrc = 0.
      ev_areat = ls_areat-areat.
      rv_found = abap_true.
    ENDIF.
  ENDMETHOD.

  METHOD get_fugr_enlfdir.
    CLEAR et_enlfdir.
    READ TABLE mt_fugr_enlfdir INTO DATA(ls_enlfdir) WITH TABLE KEY area = iv_area.
    IF sy-subrc = 0.
      et_enlfdir = ls_enlfdir-enlfdir.
      rv_found = abap_true.
    ENDIF.
  ENDMETHOD.

  METHOD get_fugr_func_metadata.
    CLEAR es_metadata.
    READ TABLE mt_fugr_func_meta INTO es_metadata WITH TABLE KEY funcname = iv_funcname.
    IF sy-subrc = 0.
      rv_found = abap_true.
    ENDIF.
  ENDMETHOD.

  METHOD get_fugr_main_program.
    DATA lv_group TYPE rs38l-area.
    DATA lv_namespace TYPE rs38l-namespace.

    CALL FUNCTION 'FUNCTION_INCLUDE_SPLIT'
      EXPORTING
        complete_area = iv_area
      IMPORTING
        namespace     = lv_namespace
        group         = lv_group
      EXCEPTIONS
        OTHERS        = 12.
    IF sy-subrc = 0.
      CONCATENATE lv_namespace 'SAPL' lv_group INTO rv_program.
    ENDIF.
  ENDMETHOD.

  METHOD get_prog_tpool_languages.
    CLEAR et_tpool_i18n.
    IF iv_language <> mv_language.
      RETURN.
    ENDIF.

    READ TABLE mt_prog_langs INTO DATA(ls_prog) WITH TABLE KEY program = iv_program.
    IF sy-subrc = 0.
      et_tpool_i18n = ls_prog-tpool_i18n.
      rv_found = abap_true.
    ENDIF.
  ENDMETHOD.

  METHOD get_smim_loio.
    CLEAR es_smimloio.
    READ TABLE mt_smim_loio INTO DATA(ls_loio) WITH TABLE KEY loio_id = iv_loio_id.
    IF sy-subrc = 0.
      es_smimloio = ls_loio-smimloio.
      rv_found = abap_true.
    ENDIF.
  ENDMETHOD.

  METHOD get_smim_phf.
    CLEAR es_data.
    READ TABLE mt_smim_phf INTO DATA(ls_phf)
      WITH TABLE KEY loio_id = iv_loio_id phio_id = iv_phio_id.
    IF sy-subrc = 0.
      es_data = ls_phf-data.
      rv_found = abap_true.
    ENDIF.
  ENDMETHOD.

  METHOD get_tobj_data.
    CLEAR es_data.
    READ TABLE mt_tobj INTO DATA(ls_tobj) WITH TABLE KEY tabname = iv_tabname.
    IF sy-subrc = 0.
      es_data = ls_tobj-data.
      rv_found = abap_true.
    ENDIF.
  ENDMETHOD.

  METHOD get_tran_data.
    CLEAR es_data.
    IF iv_language <> mv_language.
      RETURN.
    ENDIF.

    READ TABLE mt_tran INTO DATA(ls_tran) WITH TABLE KEY tcode = iv_tcode.
    IF sy-subrc = 0.
      es_data = ls_tran-data.
      rv_found = abap_true.
    ENDIF.
  ENDMETHOD.

  METHOD prepare.
    DATA lt_dtel TYPE ty_dtel_keys.
    DATA lt_doma TYPE ty_doma_keys.
    DATA lt_enhs TYPE ty_enhs_keys.
    DATA lt_fugr TYPE ty_fugr_keys.
    DATA lt_prog TYPE ty_prog_keys.
    DATA lt_smim TYPE ty_smim_keys.
    DATA lt_tobj TYPE ty_tobj_keys.
    DATA lt_tran TYPE ty_tran_keys.

    clear( ).
    mv_language = iv_language.

    collect_keys(
      EXPORTING
        it_tadir = it_tadir
      IMPORTING
        et_dtel  = lt_dtel
        et_doma  = lt_doma
        et_enhs  = lt_enhs
        et_fugr  = lt_fugr
        et_prog  = lt_prog
        et_smim  = lt_smim
        et_tobj  = lt_tobj
        et_tran  = lt_tran ).

    TRY.
        prepare_dtel( lt_dtel ).
        prepare_doma(
          it_names         = lt_doma
          iv_main_language = iv_language ).
        prepare_enhs( lt_enhs ).
        prepare_fugr( lt_fugr ).
        prepare_prog_langs(
          it_programs = lt_prog
          iv_language = iv_language ).
        prepare_smim( lt_smim ).
        prepare_tobj( lt_tobj ).
        prepare_tran(
          it_tcodes   = lt_tran
          iv_language = iv_language ).
      CATCH cx_root.
        clear( ).
    ENDTRY.
  ENDMETHOD.

  METHOD prepare_dtel.
    DATA lt_dd04v TYPE STANDARD TABLE OF dd04v WITH DEFAULT KEY.
    DATA lt_dd04t TYPE STANDARD TABLE OF dd04t WITH DEFAULT KEY.

    IF it_names IS INITIAL.
      RETURN.
    ENDIF.

    SELECT *
      FROM dd04l
      INTO CORRESPONDING FIELDS OF TABLE @lt_dd04v
      FOR ALL ENTRIES IN @it_names
      WHERE rollname = @it_names-table_line
        AND as4local = 'A'
        AND as4vers = '0000'.

    LOOP AT lt_dd04v INTO DATA(ls_dd04v).
      INSERT VALUE ty_dtel_cache(
        rollname = ls_dd04v-rollname
        dd04v    = ls_dd04v ) INTO TABLE mt_dtel.
    ENDLOOP.

    IF mt_dtel IS INITIAL.
      RETURN.
    ENDIF.

    " Load DD04T for ALL languages (not just main)
    SELECT *
      FROM dd04t
      INTO TABLE @lt_dd04t
      FOR ALL ENTRIES IN @it_names
      WHERE rollname = @it_names-table_line
        AND as4local = 'A'
        AND as4vers = '0000'.

    LOOP AT lt_dd04t INTO DATA(ls_dd04t).
      READ TABLE mt_dtel ASSIGNING FIELD-SYMBOL(<ls_dtel>)
        WITH TABLE KEY rollname = ls_dd04t-rollname.
      IF sy-subrc = 0.
        IF ls_dd04t-ddlanguage = mv_language.
          " Main language: merge into dd04v as before
          MOVE-CORRESPONDING ls_dd04t TO <ls_dtel>-dd04v.
        ELSE.
          " Translation: store separately
          APPEND ls_dd04t TO <ls_dtel>-dd04t_i18n.
        ENDIF.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.

  METHOD prepare_doma.
    DATA lt_dd01l TYPE STANDARD TABLE OF dd01l WITH DEFAULT KEY.
    DATA lt_dd01t TYPE STANDARD TABLE OF dd01t WITH DEFAULT KEY.
    DATA lt_dd07l TYPE STANDARD TABLE OF dd07l WITH DEFAULT KEY.
    DATA lt_dd07t TYPE STANDARD TABLE OF dd07t WITH DEFAULT KEY.

    IF it_names IS INITIAL.
      RETURN.
    ENDIF.

    SELECT *
      FROM dd01l
      INTO TABLE @lt_dd01l
      FOR ALL ENTRIES IN @it_names
      WHERE domname = @it_names-table_line
        AND as4local = 'A'
        AND as4vers = '0000'.
    IF lt_dd01l IS INITIAL.
      RETURN.
    ENDIF.

    SELECT *
      FROM dd01t
      INTO TABLE @lt_dd01t
      FOR ALL ENTRIES IN @lt_dd01l
      WHERE domname = @lt_dd01l-domname
        AND as4local = 'A'
        AND as4vers = '0000'.

    SELECT *
      FROM dd07l
      INTO TABLE @lt_dd07l
      FOR ALL ENTRIES IN @lt_dd01l
      WHERE domname = @lt_dd01l-domname
        AND as4local = 'A'
        AND as4vers = '0000'.

    SELECT *
      FROM dd07t
      INTO TABLE @lt_dd07t
      FOR ALL ENTRIES IN @lt_dd01l
      WHERE domname = @lt_dd01l-domname
        AND as4local = 'A'
        AND as4vers = '0000'.

    LOOP AT lt_dd01l INTO DATA(ls_dd01l).
      DATA(lt_langs) = VALUE zcl_abapgit_ortec_ser_pref=>ty_langu_tt( ( iv_main_language ) ).

      " every language this domain has EITHER a DD01T OR a DD07T text row
      " for (mirrors ZCL_ABAPGIT_OBJECT_DOMA's own serialize_texts language
      " discovery, but from the already-fetched bulk tables). IV_MAIN_
      " LANGUAGE is seeded above unconditionally - DDIF_DOMA_GET always
      " returns the DD01L-derived header for the main language even when
      " no DD01T/DD07T text row exists for it (correctness review DR-001:
      " without this seed, a domain with no main-language text would get
      " a fully INITIAL main-language DD01V and be silently dropped by
      " the seam's "ls_dd01v IS INITIAL -> RETURN" guard).
      LOOP AT lt_dd01t INTO DATA(ls_dd01t_lang) WHERE domname = ls_dd01l-domname.
        APPEND ls_dd01t_lang-ddlanguage TO lt_langs.
      ENDLOOP.
      LOOP AT lt_dd07t INTO DATA(ls_dd07t_lang) WHERE domname = ls_dd01l-domname.
        APPEND ls_dd07t_lang-ddlanguage TO lt_langs.
      ENDLOOP.
      SORT lt_langs ASCENDING.
      DELETE ADJACENT DUPLICATES FROM lt_langs.

      DATA(ls_cache) = VALUE ty_doma_cache( domname = ls_dd01l-domname ).

      LOOP AT lt_langs INTO DATA(lv_lang).
        DATA(ls_dd01v) = CORRESPONDING dd01v( ls_dd01l ).
        READ TABLE lt_dd01t INTO DATA(ls_text) WITH KEY domname = ls_dd01l-domname ddlanguage = lv_lang.
        IF sy-subrc = 0.
          ls_dd01v-ddlanguage = ls_text-ddlanguage.
          ls_dd01v-ddtext     = ls_text-ddtext.
        ELSE.
          " no DD01T text row - DDIF_DOMA_GET still sets ddlanguage
          ls_dd01v-ddlanguage = lv_lang.
        ENDIF.

        DATA(lt_dd07v) = VALUE dd07v_tab( ).
        LOOP AT lt_dd07l INTO DATA(ls_dd07l) WHERE domname = ls_dd01l-domname.
          DATA(ls_dd07v) = CORRESPONDING dd07v( ls_dd07l ).
          READ TABLE lt_dd07t INTO DATA(ls_val_text)
            WITH KEY domname = ls_dd01l-domname ddlanguage = lv_lang valpos = ls_dd07l-valpos.
          IF sy-subrc = 0.
            ls_dd07v-ddlanguage = ls_val_text-ddlanguage.
            ls_dd07v-ddtext     = ls_val_text-ddtext.
            ls_dd07v-domval_ld  = ls_val_text-domval_ld.
            ls_dd07v-domval_hd  = ls_val_text-domval_hd.
          ELSE.
            " no translation for this value - keep entry, texts stay initial
            ls_dd07v-ddlanguage = lv_lang.
          ENDIF.
          APPEND ls_dd07v TO lt_dd07v.
        ENDLOOP.

        IF lv_lang = iv_main_language.
          ls_cache-dd01v     = ls_dd01v.
          ls_cache-dd07v_tab = lt_dd07v.
        ELSE.
          APPEND ls_dd01v TO ls_cache-dd01v_i18n.
          APPEND LINES OF lt_dd07v TO ls_cache-dd07v_tab_i18n.
        ENDIF.
      ENDLOOP.

      INSERT ls_cache INTO TABLE mt_doma.
    ENDLOOP.
  ENDMETHOD.

  METHOD prepare_enhs.
    DATA lt_raw TYPE STANDARD TABLE OF ty_enhs_cache WITH DEFAULT KEY.

    IF it_names IS INITIAL.
      RETURN.
    ENDIF.

    SELECT enhspot, abap_language_version
      FROM enhspotheader
      INTO TABLE @lt_raw
      FOR ALL ENTRIES IN @it_names
      WHERE enhspot = @it_names-table_line
        AND version = 'A'.

    LOOP AT lt_raw INTO DATA(ls_raw_enhs).
      INSERT ls_raw_enhs INTO TABLE mt_enhs.
    ENDLOOP.
  ENDMETHOD.

  METHOD prepare_fugr.
    DATA lt_enlfdir TYPE ty_enlfdir_tt.
    DATA lt_tlibt TYPE STANDARD TABLE OF tlibt WITH DEFAULT KEY.

    IF it_areas IS INITIAL.
      RETURN.
    ENDIF.

    LOOP AT it_areas INTO DATA(lv_area).
      INSERT VALUE ty_fugr_areat_cache( area = lv_area ) INTO TABLE mt_fugr_areat.
      INSERT VALUE ty_fugr_enlfdir_cache( area = lv_area ) INTO TABLE mt_fugr_enlfdir.
    ENDLOOP.

    SELECT *
      FROM tlibt
      INTO TABLE @lt_tlibt
      FOR ALL ENTRIES IN @it_areas
      WHERE spras = @mv_language
        AND area = @it_areas-table_line.

    LOOP AT lt_tlibt INTO DATA(ls_tlibt).
      READ TABLE mt_fugr_areat ASSIGNING FIELD-SYMBOL(<ls_areat>)
        WITH TABLE KEY area = ls_tlibt-area.
      IF sy-subrc = 0.
        <ls_areat>-areat = ls_tlibt-areat.
      ENDIF.
    ENDLOOP.

    SELECT *
      FROM enlfdir
      INTO TABLE @lt_enlfdir
      FOR ALL ENTRIES IN @it_areas
      WHERE area = @it_areas-table_line
        AND active = @abap_true.

    SORT lt_enlfdir BY area funcname.

    LOOP AT lt_enlfdir ASSIGNING FIELD-SYMBOL(<ls_enlfdir>).
      TRANSLATE <ls_enlfdir>-funcname TO UPPER CASE.
      READ TABLE mt_fugr_enlfdir ASSIGNING FIELD-SYMBOL(<ls_fugr>)
        WITH TABLE KEY area = <ls_enlfdir>-area.
      IF sy-subrc = 0.
        APPEND <ls_enlfdir> TO <ls_fugr>-enlfdir.
      ENDIF.
      INSERT VALUE ty_fugr_func_meta(
        funcname          = <ls_enlfdir>-funcname
        exception_classes = <ls_enlfdir>-exten3 ) INTO TABLE mt_fugr_func_meta.
    ENDLOOP.
  ENDMETHOD.

  METHOD prepare_prog_langs.
    DATA lt_raw TYPE ty_d010tinf_lang_tt.

    IF it_programs IS INITIAL.
      RETURN.
    ENDIF.

    LOOP AT it_programs INTO DATA(lv_program).
      INSERT VALUE ty_prog_lang_cache( program = lv_program ) INTO TABLE mt_prog_langs.
    ENDLOOP.

    SELECT DISTINCT prog, language
      FROM d010tinf
      INTO TABLE @lt_raw
      FOR ALL ENTRIES IN @it_programs
      WHERE r3state = 'A'
        AND prog = @it_programs-table_line
        AND language <> @iv_language.

    SORT lt_raw BY prog language.

    LOOP AT lt_raw INTO DATA(ls_raw_lang).
      READ TABLE mt_prog_langs ASSIGNING FIELD-SYMBOL(<ls_prog>)
        WITH TABLE KEY program = ls_raw_lang-prog.
      IF sy-subrc = 0.
        APPEND VALUE #( language = ls_raw_lang-language ) TO <ls_prog>-tpool_i18n.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.

  METHOD prepare_smim.
    DATA lt_smimphf TYPE STANDARD TABLE OF smimphf WITH DEFAULT KEY.

    IF it_loio_ids IS INITIAL.
      RETURN.
    ENDIF.

    SELECT *
      FROM smimloio
      INTO TABLE @DATA(lt_smimloio)
      FOR ALL ENTRIES IN @it_loio_ids
      WHERE loio_id = @it_loio_ids-table_line.

    LOOP AT lt_smimloio INTO DATA(ls_smimloio).
      INSERT VALUE ty_smim_loio_cache(
        loio_id  = ls_smimloio-loio_id
        smimloio = ls_smimloio ) INTO TABLE mt_smim_loio.
    ENDLOOP.

    SELECT *
      FROM smimphf
      INTO TABLE @lt_smimphf
      FOR ALL ENTRIES IN @it_loio_ids
      WHERE langu = @sy-langu
        AND loio_id = @it_loio_ids-table_line.

    LOOP AT lt_smimphf INTO DATA(ls_smimphf).
      INSERT VALUE ty_smim_phf_cache(
        loio_id = ls_smimphf-loio_id
        phio_id = ls_smimphf-phio_id
        data    = VALUE #(
          file_name = ls_smimphf-file_name
          mimetype  = ls_smimphf-mimetype ) ) INTO TABLE mt_smim_phf.
    ENDLOOP.
  ENDMETHOD.

  METHOD prepare_tobj.
    IF it_tabnames IS INITIAL.
      RETURN.
    ENDIF.

    LOOP AT it_tabnames INTO DATA(lv_tabname).
      INSERT VALUE ty_tobj_cache( tabname = lv_tabname ) INTO TABLE mt_tobj.
    ENDLOOP.

    SELECT *
      FROM tddat
      INTO TABLE @DATA(lt_tddat)
      FOR ALL ENTRIES IN @it_tabnames
      WHERE tabname = @it_tabnames-table_line
      ORDER BY PRIMARY KEY.

    LOOP AT lt_tddat INTO DATA(ls_tddat).
      READ TABLE mt_tobj ASSIGNING FIELD-SYMBOL(<ls_tobj>)
        WITH TABLE KEY tabname = ls_tddat-tabname.
      IF sy-subrc = 0.
        <ls_tobj>-data-tddat = ls_tddat.
      ENDIF.
    ENDLOOP.

    SELECT *
      FROM tvdir
      INTO TABLE @DATA(lt_tvdir)
      FOR ALL ENTRIES IN @it_tabnames
      WHERE tabname = @it_tabnames-table_line.

    LOOP AT lt_tvdir INTO DATA(ls_tvdir).
      READ TABLE mt_tobj ASSIGNING <ls_tobj>
        WITH TABLE KEY tabname = ls_tvdir-tabname.
      IF sy-subrc = 0.
        CLEAR: ls_tvdir-gendate,
               ls_tvdir-gentime,
               ls_tvdir-devclass.
        <ls_tobj>-data-tvdir = ls_tvdir.
      ENDIF.
    ENDLOOP.

    SELECT *
      FROM tvimf
      INTO TABLE @DATA(lt_tvimf)
      FOR ALL ENTRIES IN @it_tabnames
      WHERE tabname = @it_tabnames-table_line
      ORDER BY PRIMARY KEY.

    LOOP AT lt_tvimf INTO DATA(ls_tvimf).
      READ TABLE mt_tobj ASSIGNING <ls_tobj>
        WITH TABLE KEY tabname = ls_tvimf-tabname.
      IF sy-subrc = 0.
        APPEND ls_tvimf TO <ls_tobj>-data-tvimf.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.

  METHOD prepare_tran.
    DATA ls_tstct_i18n TYPE tstct.

    IF it_tcodes IS INITIAL.
      RETURN.
    ENDIF.

    LOOP AT it_tcodes INTO DATA(lv_tcode).
      INSERT VALUE ty_tran_cache( tcode = lv_tcode ) INTO TABLE mt_tran.
    ENDLOOP.

    SELECT *
      FROM tstct
      INTO TABLE @DATA(lt_tstct)
      FOR ALL ENTRIES IN @it_tcodes
      WHERE tcode = @it_tcodes-table_line.

    LOOP AT lt_tstct INTO DATA(ls_tstct).
      READ TABLE mt_tran ASSIGNING FIELD-SYMBOL(<ls_tran>)
        WITH TABLE KEY tcode = ls_tstct-tcode.
      IF sy-subrc = 0.
        IF ls_tstct-sprsl = iv_language.
          <ls_tran>-data-tstct = ls_tstct.
        ELSE.
          CLEAR ls_tstct_i18n.
          ls_tstct_i18n-sprsl = ls_tstct-sprsl.
          ls_tstct_i18n-ttext = ls_tstct-ttext.
          APPEND ls_tstct_i18n TO <ls_tran>-data-tstct_i18n.
        ENDIF.
      ENDIF.
    ENDLOOP.

    SELECT *
      FROM tstcp
      INTO TABLE @DATA(lt_tstcp)
      FOR ALL ENTRIES IN @it_tcodes
      WHERE tcode = @it_tcodes-table_line.

    LOOP AT lt_tstcp INTO DATA(ls_tstcp).
      READ TABLE mt_tran ASSIGNING <ls_tran> WITH TABLE KEY tcode = ls_tstcp-tcode.
      IF sy-subrc = 0.
        <ls_tran>-data-tstcp = ls_tstcp.
      ENDIF.
    ENDLOOP.

    SELECT *
      FROM tstca
      INTO TABLE @DATA(lt_tstca)
      FOR ALL ENTRIES IN @it_tcodes
      WHERE tcode = @it_tcodes-table_line
      ORDER BY PRIMARY KEY.

    LOOP AT lt_tstca INTO DATA(ls_tstca).
      READ TABLE mt_tran ASSIGNING <ls_tran> WITH TABLE KEY tcode = ls_tstca-tcode.
      IF sy-subrc = 0.
        APPEND ls_tstca TO <ls_tran>-data-tstca.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.


  METHOD extract_for_object.
    DATA lt_dtel TYPE ty_dtel_cache_tt.
    DATA lt_enhs TYPE ty_enhs_cache_tt.
    DATA lt_fugr_areat TYPE ty_fugr_areat_cache_tt.
    DATA lt_fugr_enlfdir TYPE ty_fugr_enlfdir_cache_tt.
    DATA lt_fugr_func_meta TYPE ty_fugr_func_meta_tt.
    DATA lt_prog_langs TYPE ty_prog_lang_cache_tt.
    DATA lt_smim_loio TYPE ty_smim_loio_cache_tt.
    DATA lt_smim_phf TYPE ty_smim_phf_cache_tt.
    DATA lt_tobj TYPE ty_tobj_cache_tt.
    DATA lt_tran TYPE ty_tran_cache_tt.
    DATA lv_has_data TYPE abap_bool.

    CASE is_tadir-object.
      WHEN 'DTEL'.
        READ TABLE mt_dtel INTO DATA(ls_dtel)
          WITH TABLE KEY rollname = CONV dd04l-rollname( is_tadir-obj_name ).
        IF sy-subrc = 0.
          INSERT ls_dtel INTO TABLE lt_dtel.
          lv_has_data = abap_true.
        ENDIF.

      WHEN 'ENHS'.
        READ TABLE mt_enhs INTO DATA(ls_enhs)
          WITH TABLE KEY enhspot = CONV enhspotname( is_tadir-obj_name ).
        IF sy-subrc = 0.
          INSERT ls_enhs INTO TABLE lt_enhs.
          lv_has_data = abap_true.
        ENDIF.

      WHEN 'FUGR'.
        DATA(lv_area) = CONV tlibt-area( is_tadir-obj_name ).
        READ TABLE mt_fugr_areat INTO DATA(ls_areat)
          WITH TABLE KEY area = lv_area.
        IF sy-subrc = 0.
          INSERT ls_areat INTO TABLE lt_fugr_areat.
          lv_has_data = abap_true.
        ENDIF.
        READ TABLE mt_fugr_enlfdir INTO DATA(ls_enlfdir)
          WITH TABLE KEY area = lv_area.
        IF sy-subrc = 0.
          INSERT ls_enlfdir INTO TABLE lt_fugr_enlfdir.
          lv_has_data = abap_true.
          " Also extract func_meta for each function module in this group
          LOOP AT ls_enlfdir-enlfdir INTO DATA(ls_fm).
            READ TABLE mt_fugr_func_meta INTO DATA(ls_func_meta)
              WITH TABLE KEY funcname = ls_fm-funcname.
            IF sy-subrc = 0.
              INSERT ls_func_meta INTO TABLE lt_fugr_func_meta.
            ENDIF.
          ENDLOOP.
        ENDIF.
        " Also extract prog_langs for the FUGR main program
        DATA(lv_program) = get_fugr_main_program( CONV rs38l-area( is_tadir-obj_name ) ).
        IF lv_program IS NOT INITIAL.
          READ TABLE mt_prog_langs INTO DATA(ls_prog)
            WITH TABLE KEY program = lv_program.
          IF sy-subrc = 0.
            INSERT ls_prog INTO TABLE lt_prog_langs.
            lv_has_data = abap_true.
          ENDIF.
        ENDIF.

      WHEN 'PROG'.
        READ TABLE mt_prog_langs INTO ls_prog
          WITH TABLE KEY program = is_tadir-obj_name.
        IF sy-subrc = 0.
          INSERT ls_prog INTO TABLE lt_prog_langs.
          lv_has_data = abap_true.
        ENDIF.

      WHEN 'SMIM'.
        DATA(lv_loio) = CONV smimloio-loio_id( is_tadir-obj_name ).
        READ TABLE mt_smim_loio INTO DATA(ls_loio)
          WITH TABLE KEY loio_id = lv_loio.
        IF sy-subrc = 0.
          INSERT ls_loio INTO TABLE lt_smim_loio.
          lv_has_data = abap_true.
        ENDIF.
        LOOP AT mt_smim_phf INTO DATA(ls_phf)
          WHERE loio_id = lv_loio.
          INSERT ls_phf INTO TABLE lt_smim_phf.
          lv_has_data = abap_true.
        ENDLOOP.

      WHEN 'TOBJ'.
        DATA(lv_tobj_name) = condense( CONV string( is_tadir-obj_name ) ).
        DATA(lv_tobj_len) = strlen( lv_tobj_name ) - 1.
        IF lv_tobj_len > 0.
          READ TABLE mt_tobj INTO DATA(ls_tobj)
            WITH TABLE KEY tabname = CONV vim_name( lv_tobj_name(lv_tobj_len) ).
          IF sy-subrc = 0.
            INSERT ls_tobj INTO TABLE lt_tobj.
            lv_has_data = abap_true.
          ENDIF.
        ENDIF.

      WHEN 'TRAN'.
        READ TABLE mt_tran INTO DATA(ls_tran)
          WITH TABLE KEY tcode = CONV tstc-tcode( is_tadir-obj_name ).
        IF sy-subrc = 0.
          INSERT ls_tran INTO TABLE lt_tran.
          lv_has_data = abap_true.
        ENDIF.
    ENDCASE.

    IF lv_has_data = abap_false.
      RETURN.
    ENDIF.

    EXPORT dtel = lt_dtel
           enhs = lt_enhs
           fugr_areat = lt_fugr_areat
           fugr_enlfdir = lt_fugr_enlfdir
           fugr_func_meta = lt_fugr_func_meta
           prog_langs = lt_prog_langs
           smim_loio = lt_smim_loio
           smim_phf = lt_smim_phf
           tobj = lt_tobj
           tran = lt_tran
           language = mv_language
      TO DATA BUFFER rv_buffer COMPRESSION ON.
  ENDMETHOD.


  METHOD inject_from_buffer.
    DATA lt_dtel TYPE ty_dtel_cache_tt.
    DATA lt_enhs TYPE ty_enhs_cache_tt.
    DATA lt_fugr_areat TYPE ty_fugr_areat_cache_tt.
    DATA lt_fugr_enlfdir TYPE ty_fugr_enlfdir_cache_tt.
    DATA lt_fugr_func_meta TYPE ty_fugr_func_meta_tt.
    DATA lt_prog_langs TYPE ty_prog_lang_cache_tt.
    DATA lt_smim_loio TYPE ty_smim_loio_cache_tt.
    DATA lt_smim_phf TYPE ty_smim_phf_cache_tt.
    DATA lt_tobj TYPE ty_tobj_cache_tt.
    DATA lt_tran TYPE ty_tran_cache_tt.
    DATA lv_language TYPE spras.

    CHECK iv_buffer IS NOT INITIAL.

    IMPORT dtel = lt_dtel
           enhs = lt_enhs
           fugr_areat = lt_fugr_areat
           fugr_enlfdir = lt_fugr_enlfdir
           fugr_func_meta = lt_fugr_func_meta
           prog_langs = lt_prog_langs
           smim_loio = lt_smim_loio
           smim_phf = lt_smim_phf
           tobj = lt_tobj
           tran = lt_tran
           language = lv_language
      FROM DATA BUFFER iv_buffer.                       "#EC CI_SUBRC
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    " CLEAR first: a parallel RFC worker session can be reused across many
    " unrelated dispatches over its lifetime, and INSERT INTO a UNIQUE-keyed
    " table silently no-ops if a prior invocation already cached that same
    " key - without clearing, a worker would serve stale data (e.g. an
    " outdated DTEL short text/domain) forever, regardless of what the
    " main process re-sends on later runs.
    CLEAR: mt_dtel, mt_enhs, mt_fugr_areat, mt_fugr_enlfdir,
           mt_fugr_func_meta, mt_prog_langs, mt_smim_loio, mt_smim_phf,
           mt_tobj, mt_tran.

    LOOP AT lt_dtel INTO DATA(ls_dtel).
      INSERT ls_dtel INTO TABLE mt_dtel.
    ENDLOOP.
    LOOP AT lt_enhs INTO DATA(ls_enhs).
      INSERT ls_enhs INTO TABLE mt_enhs.
    ENDLOOP.
    LOOP AT lt_fugr_areat INTO DATA(ls_areat).
      INSERT ls_areat INTO TABLE mt_fugr_areat.
    ENDLOOP.
    LOOP AT lt_fugr_enlfdir INTO DATA(ls_enlfdir).
      INSERT ls_enlfdir INTO TABLE mt_fugr_enlfdir.
    ENDLOOP.
    LOOP AT lt_fugr_func_meta INTO DATA(ls_func_meta).
      INSERT ls_func_meta INTO TABLE mt_fugr_func_meta.
    ENDLOOP.
    LOOP AT lt_prog_langs INTO DATA(ls_prog).
      INSERT ls_prog INTO TABLE mt_prog_langs.
    ENDLOOP.
    LOOP AT lt_smim_loio INTO DATA(ls_loio).
      INSERT ls_loio INTO TABLE mt_smim_loio.
    ENDLOOP.
    LOOP AT lt_smim_phf INTO DATA(ls_phf).
      INSERT ls_phf INTO TABLE mt_smim_phf.
    ENDLOOP.
    LOOP AT lt_tobj INTO DATA(ls_tobj).
      INSERT ls_tobj INTO TABLE mt_tobj.
    ENDLOOP.
    LOOP AT lt_tran INTO DATA(ls_tran).
      INSERT ls_tran INTO TABLE mt_tran.
    ENDLOOP.

    IF lv_language IS NOT INITIAL.
      mv_language = lv_language.
    ENDIF.
  ENDMETHOD.

  METHOD extract_for_batch.
    DATA lt_entries TYPE zaog_ser_dd_bentry_tt.
    DATA lt_doma    TYPE ty_doma_cache_tt.
    DATA lt_dtel    TYPE ty_dtel_cache_tt.
    DATA ls_hdr     TYPE zaog_ser_dd_bhdr.

    LOOP AT it_object_keys INTO DATA(ls_tadir) WHERE object = 'DOMA' OR object = 'DTEL'.
      DATA(ls_entry) = VALUE zaog_ser_dd_bentry(
        obj_type = ls_tadir-object
        obj_name = ls_tadir-obj_name ).

      CASE ls_tadir-object.
        WHEN 'DOMA'.
          READ TABLE mt_doma INTO DATA(ls_doma)
            WITH TABLE KEY domname = CONV dd01l-domname( ls_tadir-obj_name ).
          IF sy-subrc = 0.
            ls_entry-present = abap_true.
            INSERT ls_doma INTO TABLE lt_doma.
          ENDIF.
        WHEN 'DTEL'.
          READ TABLE mt_dtel INTO DATA(ls_dtel)
            WITH TABLE KEY rollname = CONV dd04l-rollname( ls_tadir-obj_name ).
          IF sy-subrc = 0.
            ls_entry-present = abap_true.
            INSERT ls_dtel INTO TABLE lt_dtel.
          ENDIF.
      ENDCASE.

      APPEND ls_entry TO lt_entries.
    ENDLOOP.

    " SER-SLICE-3 parity incident fix (serialization_slice_3_dtel_doma_
    " parity.md): a batch with DOMA/DTEL objects but NOTHING actually
    " cached (e.g. PREPARE was never called, or none of these objects
    " were found) must still return an INITIAL buffer, per this method's
    " own documented contract - LT_ENTRIES alone being non-empty (every
    " row PRESENT = ABAP_FALSE) is not sufficient reason to build and
    " transmit a real envelope.
    IF lt_entries IS INITIAL OR ( lt_doma IS INITIAL AND lt_dtel IS INITIAL ).
      CLEAR rv_buffer.
      RETURN.
    ENDIF.

    ls_hdr-wire_format_version = 1.
    ls_hdr-provider_id         = 'SER_DD01'.
    ls_hdr-object_count        = lines( lt_entries ).

    EXPORT hdr      = ls_hdr
           entries  = lt_entries
           doma     = lt_doma
           dtel     = lt_dtel
           language = mv_language
      TO DATA BUFFER rv_buffer COMPRESSION ON.
  ENDMETHOD.


  METHOD inject_batch_from_buffer.
    DATA ls_hdr            TYPE zaog_ser_dd_bhdr.
    DATA lt_entries        TYPE zaog_ser_dd_bentry_tt.
    DATA lt_doma           TYPE ty_doma_cache_tt.
    DATA lt_dtel           TYPE ty_dtel_cache_tt.
    DATA lt_entries_sorted TYPE STANDARD TABLE OF zaog_ser_dd_bentry WITH DEFAULT KEY.
    DATA lv_lines_before   TYPE i.
    DATA lv_language       TYPE spras.

    CHECK iv_buffer IS NOT INITIAL.

    TRY.
        IMPORT hdr      = ls_hdr
               entries  = lt_entries
               doma     = lt_doma
               dtel     = lt_dtel
               language = lv_language
          FROM DATA BUFFER iv_buffer.
      CATCH cx_root INTO DATA(lx_import).
        zcx_abapgit_exception=>raise(
          |ORTEC DOMA/DTEL batch prefetch buffer is corrupt: { lx_import->get_text( ) }| ).
    ENDTRY.
    IF sy-subrc <> 0.
      zcx_abapgit_exception=>raise( 'ORTEC DOMA/DTEL batch prefetch buffer: IMPORT failed' ).
    ENDIF.

    IF ls_hdr-wire_format_version <> 1.
      zcx_abapgit_exception=>raise(
        |ORTEC DOMA/DTEL batch prefetch buffer: unknown wire_format_version { ls_hdr-wire_format_version }| ).
    ENDIF.

    IF ls_hdr-object_count <> lines( lt_entries ).
      zcx_abapgit_exception=>raise(
        'ORTEC DOMA/DTEL batch prefetch buffer: object_count does not match ENTRIES' ).
    ENDIF.

    " duplicate check MUST happen before any INSERT INTO mt_doma/mt_dtel -
    " a HASHED TABLE INSERT would otherwise silently collapse a duplicate
    " instead of rejecting the whole buffer as corrupt.
    lt_entries_sorted = CORRESPONDING #( lt_entries ).
    SORT lt_entries_sorted BY obj_type obj_name.
    lv_lines_before = lines( lt_entries_sorted ).
    DELETE ADJACENT DUPLICATES FROM lt_entries_sorted COMPARING obj_type obj_name.
    IF lines( lt_entries_sorted ) <> lv_lines_before.
      zcx_abapgit_exception=>raise(
        'ORTEC DOMA/DTEL batch prefetch buffer: duplicate entry in ENTRIES' ).
    ENDIF.

    " CLEAR first: a parallel RFC worker session can be reused across many
    " unrelated dispatches over its lifetime - see INJECT_FROM_BUFFER's own
    " identical clear-before-insert rationale.
    CLEAR mt_doma.
    CLEAR mt_dtel.

    LOOP AT lt_doma INTO DATA(ls_doma).
      INSERT ls_doma INTO TABLE mt_doma.
    ENDLOOP.
    LOOP AT lt_dtel INTO DATA(ls_dtel).
      INSERT ls_dtel INTO TABLE mt_dtel.
    ENDLOOP.

    " a worker session that never called PREPARE() has MV_LANGUAGE initial -
    " without this, GET_DOMA_DATA/GET_DTEL_DATA's own language guard would
    " reject every lookup after an otherwise-successful inject.
    IF lv_language IS NOT INITIAL.
      mv_language = lv_language.
    ENDIF.
  ENDMETHOD.

  METHOD clear_dd_cache.
    CLEAR mt_doma.
    CLEAR mt_dtel.
  ENDMETHOD.

ENDCLASS.

