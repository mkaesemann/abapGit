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

  PRIVATE SECTION.
    TYPES ty_dtel_keys TYPE HASHED TABLE OF dd04l-rollname
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
        rollname TYPE dd04l-rollname,
        dd04v    TYPE dd04v,
      END OF ty_dtel_cache.
    TYPES ty_dtel_cache_tt TYPE HASHED TABLE OF ty_dtel_cache
      WITH UNIQUE KEY rollname.

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
        et_enhs  = lt_enhs
        et_fugr  = lt_fugr
        et_prog  = lt_prog
        et_smim  = lt_smim
        et_tobj  = lt_tobj
        et_tran  = lt_tran ).

    TRY.
        prepare_dtel( lt_dtel ).
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

    SELECT *
      FROM dd04t
      INTO TABLE @lt_dd04t
      FOR ALL ENTRIES IN @it_names
      WHERE rollname = @it_names-table_line
        AND ddlanguage = @mv_language
        AND as4local = 'A'
        AND as4vers = '0000'.

    LOOP AT lt_dd04t INTO DATA(ls_dd04t).
      READ TABLE mt_dtel ASSIGNING FIELD-SYMBOL(<ls_dtel>)
        WITH TABLE KEY rollname = ls_dd04t-rollname.
      IF sy-subrc = 0.
        MOVE-CORRESPONDING ls_dd04t TO <ls_dtel>-dd04v.
      ENDIF.
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
ENDCLASS.
