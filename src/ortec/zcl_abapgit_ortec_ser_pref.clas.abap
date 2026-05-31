"! <p class="shorttext synchronized">ORTEC serializer prefetch buffer</p>
"! Holds per-type data loaded before the abapGit serialization loop so
"! selected serializers can avoid repeated single-object table reads.
CLASS zcl_abapgit_ortec_ser_pref DEFINITION
  PUBLIC FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.
    TYPES:
      BEGIN OF ty_msag_data,
        t100a TYPE t100a,
        t100  TYPE STANDARD TABLE OF t100 WITH DEFAULT KEY,
      END OF ty_msag_data.

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

    "! Return prefetched DOKIL entries for a given longtext ID and object name.
    "! Returns empty table if prefetch was not prepared or no entries found.
    CLASS-METHODS get_dokil
      IMPORTING iv_longtext_id  TYPE dokil-id
                iv_object_name  TYPE clike
      RETURNING VALUE(rt_dokil) TYPE zif_abapgit_definitions=>ty_dokil_tt.

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

        SELECT *
          FROM t100
          INTO TABLE @DATA(lt_t100)
          FOR ALL ENTRIES IN @lt_msg_ids
          WHERE sprsl = @iv_language
            AND arbgb = @lt_msg_ids-table_line
          ORDER BY PRIMARY KEY.

        LOOP AT lt_t100 INTO DATA(ls_t100).
          READ TABLE mt_msag ASSIGNING FIELD-SYMBOL(<ls_msag>) WITH TABLE KEY msg_id = ls_t100-arbgb.
          IF sy-subrc = 0.
            APPEND ls_t100 TO <ls_msag>-data-t100.
          ENDIF.
        ENDLOOP.
      CATCH cx_root.
        clear( ).
    ENDTRY.
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
