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
    "! Currently this loads message class header and main-language rows.
    "! @parameter it_tadir |
    "! TADIR rows that will be serialized.
    "! @parameter iv_language |
    "! Main language used by the serializer.
    CLASS-METHODS prepare
      IMPORTING it_tadir    TYPE zif_abapgit_definitions=>ty_tadir_tt
                iv_language TYPE spras.

    "! Clear all prefetch buffers for the current internal session.
    CLASS-METHODS clear.

    "! Return prefetched message class data when available.
    "! The method returns false if the buffer was not prepared for the
    "! requested language or if the message class header was not found.
    "! @parameter iv_msg_id |
    "! Message class ID.
    "! @parameter iv_language |
    "! Main language requested by the serializer.
    "! @parameter es_data |
    "! Prefetched message class header and messages.
    "! @parameter rv_found |
    "! ABAP_TRUE if prefetched data is complete enough to use.
    CLASS-METHODS get_msag_data
      IMPORTING iv_msg_id        TYPE rglif-message_id
                iv_language      TYPE spras
      EXPORTING es_data          TYPE ty_msag_data
      RETURNING VALUE(rv_found)  TYPE abap_bool.

  PRIVATE SECTION.
    TYPES:
      BEGIN OF ty_msag_cache,
        msg_id TYPE rglif-message_id,
        data   TYPE ty_msag_data,
      END OF ty_msag_cache.
    TYPES ty_msag_cache_tt TYPE HASHED TABLE OF ty_msag_cache WITH UNIQUE KEY msg_id.
    TYPES ty_msg_ids TYPE HASHED TABLE OF rglif-message_id WITH UNIQUE KEY table_line.

    CLASS-DATA mt_msag TYPE ty_msag_cache_tt.
    CLASS-DATA mv_language TYPE spras.

ENDCLASS.

CLASS zcl_abapgit_ortec_ser_pref IMPLEMENTATION.
  METHOD clear.
    CLEAR mt_msag.
    CLEAR mv_language.
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
ENDCLASS.
