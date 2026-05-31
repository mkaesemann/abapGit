"! <p class="shorttext synchronized">ORTEC serializer prefetch buffer</p>
"! Holds per-type data loaded before the abapGit serialization loop so
"! selected serializers can avoid repeated single-object table reads.
CLASS zcl_abapgit_ortec_ser_pref DEFINITION
  PUBLIC FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.
    TYPES:
      BEGIN OF ty_msag_data,
        t100a     TYPE t100a,
        t100      TYPE STANDARD TABLE OF t100 WITH DEFAULT KEY,
        t100t     TYPE STANDARD TABLE OF t100t WITH DEFAULT KEY,
        t100_i18n TYPE STANDARD TABLE OF t100 WITH DEFAULT KEY,
      END OF ty_msag_data.
    TYPES ty_langu_tt TYPE STANDARD TABLE OF langu WITH DEFAULT KEY.
    TYPES ty_t100t_tt TYPE STANDARD TABLE OF t100t WITH DEFAULT KEY.
    TYPES ty_t100_tt TYPE STANDARD TABLE OF t100 WITH DEFAULT KEY.

    "! Prepare prefetch buffers for the current serialization run.
    CLASS-METHODS prepare
      IMPORTING it_tadir    TYPE zif_abapgit_definitions=>ty_tadir_tt
                iv_language TYPE spras.

    "! Clear all prefetch buffers for the current internal session.
    CLASS-METHODS clear.

    "! Return prefetched message class data when available.
    CLASS-METHODS get_msag_data
      IMPORTING iv_msg_id        TYPE rglif-message_id
                iv_language      TYPE spras
      EXPORTING es_data          TYPE ty_msag_data
      RETURNING VALUE(rv_found)  TYPE abap_bool.

    "! Return prefetched MSAG i18n data (translation languages, t100t, t100 texts).
    CLASS-METHODS get_msag_i18n_data
      IMPORTING iv_msg_id        TYPE rglif-message_id
                iv_language      TYPE spras
      EXPORTING et_i18n_langs    TYPE ty_langu_tt
                et_t100t         TYPE ty_t100t_tt
                et_t100_i18n     TYPE ty_t100_tt
      RETURNING VALUE(rv_found)  TYPE abap_bool.

    "! Return prefetched DOKIL entries for a given longtext ID and object name.
    "! Returns empty table if prefetch was not prepared or no entries found.
    CLASS-METHODS get_dokil
      IMPORTING iv_longtext_id  TYPE dokil-id
                iv_object_name  TYPE clike
      RETURNING VALUE(rt_dokil) TYPE zif_abapgit_definitions=>ty_dokil_tt.

    "! Extract prefetch data relevant to a single TADIR object into a transferable buffer.
    "! Used to forward per-object cache slices to parallel worker sessions.
    CLASS-METHODS extract_for_object
      IMPORTING is_tadir         TYPE zif_abapgit_definitions=>ty_tadir
      RETURNING VALUE(rv_buffer) TYPE xstring.

    "! Inject prefetch data from a buffer into the session-local caches.
    "! Called by parallel workers to restore per-object cache slices.
    CLASS-METHODS inject_from_buffer
      IMPORTING iv_buffer TYPE xstring.

  PRIVATE SECTION.
    TYPES:
      BEGIN OF ty_msag_cache,
        msg_id TYPE rglif-message_id,
        data   TYPE ty_msag_data,
      END OF ty_msag_cache.
    TYPES ty_msag_cache_tt TYPE HASHED TABLE OF ty_msag_cache WITH UNIQUE KEY msg_id.
    TYPES ty_msg_ids TYPE HASHED TABLE OF rglif-message_id WITH UNIQUE KEY table_line.

    CLASS-DATA mt_msag TYPE ty_msag_cache_tt.
    CLASS-DATA mt_dokil TYPE SORTED TABLE OF dokil WITH NON-UNIQUE KEY id object.
    CLASS-DATA mv_dokil_prepared TYPE abap_bool.
    CLASS-DATA mv_language TYPE spras.

    CLASS-METHODS prepare_dokil
      IMPORTING it_tadir TYPE zif_abapgit_definitions=>ty_tadir_tt.

ENDCLASS.

CLASS zcl_abapgit_ortec_ser_pref IMPLEMENTATION.
  METHOD clear.
    CLEAR mt_msag.
    CLEAR mt_dokil.
    CLEAR mv_dokil_prepared.
    CLEAR mv_language.
  ENDMETHOD.

  METHOD get_dokil.
    DATA lv_object TYPE dokil-object.

    IF mv_dokil_prepared = abap_false.
      RETURN.
    ENDIF.

    lv_object = iv_object_name.

    IF 'CA,CE,CO,CT,IA,IE,IO,WC,FU,FX,DI,IS,PS' CS iv_longtext_id.
      " Sub-object types: match first 30 chars of object
      LOOP AT mt_dokil INTO DATA(ls_dokil)
        WHERE id = iv_longtext_id
          AND object(30) = lv_object(30).
        APPEND ls_dokil TO rt_dokil.
      ENDLOOP.
    ELSEIF iv_longtext_id = 'OD'.
      " OD type: match first 10 chars
      LOOP AT mt_dokil INTO ls_dokil
        WHERE id = iv_longtext_id
          AND object(10) = lv_object(10).
        APPEND ls_dokil TO rt_dokil.
      ENDLOOP.
    ELSE.
      " Exact match on object name (e.g. RE, DE, DT)
      LOOP AT mt_dokil INTO ls_dokil
        WHERE id = iv_longtext_id
          AND object = lv_object.
        APPEND ls_dokil TO rt_dokil.
      ENDLOOP.
    ENDIF.
  ENDMETHOD.

  METHOD get_msag_data.
    CLEAR es_data.

    IF iv_language <> mv_language.
      RETURN.
    ENDIF.

    READ TABLE mt_msag INTO DATA(ls_msag) WITH TABLE KEY msg_id = iv_msg_id.
    IF sy-subrc = 0.
      es_data = ls_msag-data.
      rv_found = abap_true.
    ENDIF.
  ENDMETHOD.

  METHOD prepare.
    DATA lt_msg_ids TYPE ty_msg_ids.
    DATA lv_msg_id TYPE rglif-message_id.

    clear( ).
    mv_language = iv_language.

    prepare_dokil( it_tadir ).

    LOOP AT it_tadir INTO DATA(ls_tadir) WHERE object = 'MSAG'.
      lv_msg_id = ls_tadir-obj_name.
      INSERT lv_msg_id INTO TABLE lt_msg_ids.
    ENDLOOP.

    IF lt_msg_ids IS INITIAL.
      RETURN.
    ENDIF.

    TRY.
        SELECT *
          FROM t100a
          INTO TABLE @DATA(lt_t100a)
          FOR ALL ENTRIES IN @lt_msg_ids
          WHERE arbgb = @lt_msg_ids-table_line
          ORDER BY PRIMARY KEY.

        LOOP AT lt_t100a INTO DATA(ls_t100a).
          INSERT VALUE ty_msag_cache(
            msg_id = ls_t100a-arbgb
            data   = VALUE #( t100a = ls_t100a ) ) INTO TABLE mt_msag.
        ENDLOOP.

        IF mt_msag IS INITIAL.
          RETURN.
        ENDIF.

        " Load T100 for ALL languages (main + translations)
        SELECT *
          FROM t100
          INTO TABLE @DATA(lt_t100)
          FOR ALL ENTRIES IN @lt_msg_ids
          WHERE arbgb = @lt_msg_ids-table_line
          ORDER BY PRIMARY KEY.

        LOOP AT lt_t100 INTO DATA(ls_t100).
          READ TABLE mt_msag ASSIGNING FIELD-SYMBOL(<ls_msag>) WITH TABLE KEY msg_id = ls_t100-arbgb.
          IF sy-subrc = 0.
            IF ls_t100-sprsl = iv_language.
              APPEND ls_t100 TO <ls_msag>-data-t100.
            ELSE.
              APPEND ls_t100 TO <ls_msag>-data-t100_i18n.
            ENDIF.
          ENDIF.
        ENDLOOP.

        " Load T100T for ALL languages
        SELECT *
          FROM t100t
          INTO TABLE @DATA(lt_t100t)
          FOR ALL ENTRIES IN @lt_msg_ids
          WHERE arbgb = @lt_msg_ids-table_line
          ORDER BY PRIMARY KEY.

        LOOP AT lt_t100t INTO DATA(ls_t100t).
          READ TABLE mt_msag ASSIGNING <ls_msag> WITH TABLE KEY msg_id = ls_t100t-arbgb.
          IF sy-subrc = 0.
            APPEND ls_t100t TO <ls_msag>-data-t100t.
          ENDIF.
        ENDLOOP.
      CATCH cx_root.
        clear( ).
    ENDTRY.
  ENDMETHOD.

  METHOD get_msag_i18n_data.
    CLEAR: et_i18n_langs, et_t100t, et_t100_i18n.

    READ TABLE mt_msag INTO DATA(ls_msag) WITH TABLE KEY msg_id = iv_msg_id.
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    " Collect distinct non-main languages from t100t
    LOOP AT ls_msag-data-t100t INTO DATA(ls_t100t)
      WHERE sprsl <> iv_language.
      APPEND ls_t100t TO et_t100t.
      APPEND ls_t100t-sprsl TO et_i18n_langs.
    ENDLOOP.

    SORT et_i18n_langs ASCENDING.
    DELETE ADJACENT DUPLICATES FROM et_i18n_langs.

    " Return t100 entries for non-main languages
    et_t100_i18n = ls_msag-data-t100_i18n.

    rv_found = abap_true.
  ENDMETHOD.


  METHOD extract_for_object.
    DATA lt_msag TYPE ty_msag_cache_tt.
    DATA lt_dokil LIKE mt_dokil.
    DATA lv_object TYPE dokil-object.

    " Extract MSAG data for this object
    IF is_tadir-object = 'MSAG'.
      READ TABLE mt_msag INTO DATA(ls_msag)
        WITH TABLE KEY msg_id = CONV rglif-message_id( is_tadir-obj_name ).
      IF sy-subrc = 0.
        INSERT ls_msag INTO TABLE lt_msag.
      ENDIF.
    ENDIF.

    " Extract DOKIL entries for this object (prefix match on obj_name)
    IF mv_dokil_prepared = abap_true.
      lv_object = is_tadir-obj_name.
      DATA(lv_objname_str) = condense( CONV string( is_tadir-obj_name ) ).
      DATA(lv_len) = strlen( lv_objname_str ).
      IF lv_len > 0.
        LOOP AT mt_dokil INTO DATA(ls_dokil).
          IF ls_dokil-object(lv_len) = lv_object(lv_len).
            INSERT ls_dokil INTO TABLE lt_dokil.
          ENDIF.
        ENDLOOP.
      ENDIF.
    ENDIF.

    IF lt_msag IS INITIAL AND lt_dokil IS INITIAL.
      RETURN.
    ENDIF.

    EXPORT msag = lt_msag
           dokil = lt_dokil
           language = mv_language
           dokil_prepared = mv_dokil_prepared
      TO DATA BUFFER rv_buffer COMPRESSION ON.
  ENDMETHOD.


  METHOD inject_from_buffer.
    DATA lt_msag TYPE ty_msag_cache_tt.
    DATA lt_dokil LIKE mt_dokil.
    DATA lv_language TYPE spras.
    DATA lv_dokil_prepared TYPE abap_bool.

    CHECK iv_buffer IS NOT INITIAL.

    IMPORT msag = lt_msag
           dokil = lt_dokil
           language = lv_language
           dokil_prepared = lv_dokil_prepared
      FROM DATA BUFFER iv_buffer.                       "#EC CI_SUBRC
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    " Merge into session-local caches
    LOOP AT lt_msag INTO DATA(ls_msag).
      INSERT ls_msag INTO TABLE mt_msag.
    ENDLOOP.

    LOOP AT lt_dokil INTO DATA(ls_dokil).
      INSERT ls_dokil INTO TABLE mt_dokil.
    ENDLOOP.

    IF lv_dokil_prepared = abap_true.
      mv_dokil_prepared = abap_true.
    ENDIF.
    IF lv_language IS NOT INITIAL.
      mv_language = lv_language.
    ENDIF.
  ENDMETHOD.


  METHOD prepare_dokil.
    DATA lr_object TYPE RANGE OF dokil-object.
    DATA ls_range LIKE LINE OF lr_object.

    ls_range-sign = 'I'.
    ls_range-option = 'CP'.

    LOOP AT it_tadir INTO DATA(ls_tadir).
      ls_range-low = ls_tadir-obj_name.
      REPLACE ALL OCCURRENCES OF '*' IN ls_range-low WITH '#*'.
      CONCATENATE ls_range-low '*' INTO ls_range-low.
      APPEND ls_range TO lr_object.
    ENDLOOP.

    IF lr_object IS INITIAL.
      RETURN.
    ENDIF.

    SORT lr_object BY low.
    DELETE ADJACENT DUPLICATES FROM lr_object COMPARING low.

    TRY.
        SELECT *
          FROM dokil
          INTO TABLE @mt_dokil
          WHERE object IN @lr_object
          ORDER BY PRIMARY KEY.

        mv_dokil_prepared = abap_true.
      CATCH cx_root.
        CLEAR mt_dokil.
    ENDTRY.
  ENDMETHOD.
ENDCLASS.
