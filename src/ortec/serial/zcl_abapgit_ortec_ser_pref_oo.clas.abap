"! <p class="shorttext synchronized">ORTEC serializer prefetch for CLAS/INTF</p>
"! Holds OO description data loaded before the abapGit serialization loop so
"! CLAS and INTF serializers can avoid repeated single-object SELECTs.
CLASS zcl_abapgit_ortec_ser_pref_oo DEFINITION
  PUBLIC FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.
    CLASS-METHODS prepare
      IMPORTING it_tadir    TYPE zif_abapgit_definitions=>ty_tadir_tt
                iv_language TYPE spras.

    CLASS-METHODS clear.

    "! Return prefetched SEOCLASSTX entries for a class or interface.
    CLASS-METHODS get_descriptions_class
      IMPORTING iv_clsname       TYPE seoclsname
                iv_language      TYPE spras
      EXPORTING et_descriptions  TYPE zif_abapgit_oo_object_fnc=>ty_seoclasstx_tt
      RETURNING VALUE(rv_found)  TYPE abap_bool.

    "! Return prefetched SEOCOMPOTX entries for a class or interface.
    CLASS-METHODS get_descriptions_compo
      IMPORTING iv_clsname       TYPE seoclsname
                iv_language      TYPE spras
      EXPORTING et_descriptions  TYPE zif_abapgit_oo_object_fnc=>ty_seocompotx_tt
      RETURNING VALUE(rv_found)  TYPE abap_bool.

    "! Return prefetched SEOSUBCOTX entries for a class or interface.
    CLASS-METHODS get_descriptions_subco
      IMPORTING iv_clsname       TYPE seoclsname
                iv_language      TYPE spras
      EXPORTING et_descriptions  TYPE zif_abapgit_oo_object_fnc=>ty_seosubcotx_tt
      RETURNING VALUE(rv_found)  TYPE abap_bool.

    "! Extract prefetch data relevant to a single TADIR object into a transferable buffer.
    CLASS-METHODS extract_for_object
      IMPORTING is_tadir         TYPE zif_abapgit_definitions=>ty_tadir
      RETURNING VALUE(rv_buffer) TYPE xstring.

    "! Inject prefetch data from a buffer into the session-local caches.
    CLASS-METHODS inject_from_buffer
      IMPORTING iv_buffer TYPE xstring.

  PRIVATE SECTION.
    TYPES ty_clsname_keys TYPE HASHED TABLE OF seoclsname
      WITH UNIQUE KEY table_line.

    TYPES:
      BEGIN OF ty_classtx_cache,
        clsname      TYPE seoclsname,
        descriptions TYPE zif_abapgit_oo_object_fnc=>ty_seoclasstx_tt,
      END OF ty_classtx_cache.
    TYPES ty_classtx_cache_tt TYPE HASHED TABLE OF ty_classtx_cache
      WITH UNIQUE KEY clsname.

    TYPES:
      BEGIN OF ty_compotx_cache,
        clsname      TYPE seoclsname,
        descriptions TYPE zif_abapgit_oo_object_fnc=>ty_seocompotx_tt,
      END OF ty_compotx_cache.
    TYPES ty_compotx_cache_tt TYPE HASHED TABLE OF ty_compotx_cache
      WITH UNIQUE KEY clsname.

    TYPES:
      BEGIN OF ty_subcotx_cache,
        clsname      TYPE seoclsname,
        descriptions TYPE zif_abapgit_oo_object_fnc=>ty_seosubcotx_tt,
      END OF ty_subcotx_cache.
    TYPES ty_subcotx_cache_tt TYPE HASHED TABLE OF ty_subcotx_cache
      WITH UNIQUE KEY clsname.

    CLASS-DATA mt_classtx TYPE ty_classtx_cache_tt.
    CLASS-DATA mt_compotx TYPE ty_compotx_cache_tt.
    CLASS-DATA mt_subcotx TYPE ty_subcotx_cache_tt.
    CLASS-DATA mv_language TYPE spras.
    CLASS-DATA mv_prepared TYPE abap_bool.

    CLASS-METHODS prepare_classtx
      IMPORTING it_names    TYPE ty_clsname_keys
                iv_language TYPE spras.
    CLASS-METHODS prepare_compotx
      IMPORTING it_names    TYPE ty_clsname_keys
                iv_language TYPE spras.
    CLASS-METHODS prepare_subcotx
      IMPORTING it_names    TYPE ty_clsname_keys
                iv_language TYPE spras.
ENDCLASS.

CLASS zcl_abapgit_ortec_ser_pref_oo IMPLEMENTATION.
  METHOD clear.
    CLEAR mt_classtx.
    CLEAR mt_compotx.
    CLEAR mt_subcotx.
    CLEAR mv_language.
    CLEAR mv_prepared.
  ENDMETHOD.

  METHOD prepare.
    DATA lt_names TYPE ty_clsname_keys.
    DATA lv_clsname TYPE seoclsname.

    clear( ).
    mv_language = iv_language.

    LOOP AT it_tadir INTO DATA(ls_tadir) WHERE object = 'CLAS' OR object = 'INTF'.
      lv_clsname = ls_tadir-obj_name.
      INSERT lv_clsname INTO TABLE lt_names.
    ENDLOOP.

    IF lt_names IS INITIAL.
      RETURN.
    ENDIF.

    TRY.
        prepare_classtx( it_names = lt_names iv_language = iv_language ).
        prepare_compotx( it_names = lt_names iv_language = iv_language ).
        prepare_subcotx( it_names = lt_names iv_language = iv_language ).
        mv_prepared = abap_true.
      CATCH cx_root.
        clear( ).
    ENDTRY.
  ENDMETHOD.

  METHOD prepare_classtx.
    " Load translations (not main language) for class descriptions
    LOOP AT it_names INTO DATA(lv_clsname).
      INSERT VALUE ty_classtx_cache( clsname = lv_clsname ) INTO TABLE mt_classtx.
    ENDLOOP.

    SELECT * FROM seoclasstx
      INTO TABLE @DATA(lt_raw)
      FOR ALL ENTRIES IN @it_names
      WHERE clsname = @it_names-table_line
        AND langu <> @iv_language
        AND descript <> ''
      ORDER BY PRIMARY KEY.                               "#EC CI_SUBRC

    LOOP AT lt_raw ASSIGNING FIELD-SYMBOL(<ls_raw>).
      READ TABLE mt_classtx ASSIGNING FIELD-SYMBOL(<ls_cache>)
        WITH TABLE KEY clsname = <ls_raw>-clsname.
      IF sy-subrc = 0.
        DATA(ls_descr) = <ls_raw>.
        CLEAR ls_descr-clsname.
        APPEND ls_descr TO <ls_cache>-descriptions.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.

  METHOD prepare_compotx.
    " Load component descriptions — main language and translations separately
    LOOP AT it_names INTO DATA(lv_clsname).
      INSERT VALUE ty_compotx_cache( clsname = lv_clsname ) INTO TABLE mt_compotx.
    ENDLOOP.

    SELECT * FROM seocompotx
      INTO TABLE @DATA(lt_raw)
      FOR ALL ENTRIES IN @it_names
      WHERE clsname = @it_names-table_line
        AND descript <> ''
      ORDER BY PRIMARY KEY.                               "#EC CI_SUBRC

    LOOP AT lt_raw ASSIGNING FIELD-SYMBOL(<ls_raw>).
      READ TABLE mt_compotx ASSIGNING FIELD-SYMBOL(<ls_cache>)
        WITH TABLE KEY clsname = <ls_raw>-clsname.
      IF sy-subrc = 0.
        DATA(ls_descr) = <ls_raw>.
        CLEAR ls_descr-clsname.
        APPEND ls_descr TO <ls_cache>-descriptions.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.

  METHOD prepare_subcotx.
    " Load sub-component descriptions — main language and translations
    LOOP AT it_names INTO DATA(lv_clsname).
      INSERT VALUE ty_subcotx_cache( clsname = lv_clsname ) INTO TABLE mt_subcotx.
    ENDLOOP.

    SELECT * FROM seosubcotx
      INTO TABLE @DATA(lt_raw)
      FOR ALL ENTRIES IN @it_names
      WHERE clsname = @it_names-table_line
        AND descript <> ''
      ORDER BY PRIMARY KEY.                               "#EC CI_SUBRC

    LOOP AT lt_raw ASSIGNING FIELD-SYMBOL(<ls_raw>).
      READ TABLE mt_subcotx ASSIGNING FIELD-SYMBOL(<ls_cache>)
        WITH TABLE KEY clsname = <ls_raw>-clsname.
      IF sy-subrc = 0.
        DATA(ls_descr) = <ls_raw>.
        CLEAR ls_descr-clsname.
        APPEND ls_descr TO <ls_cache>-descriptions.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.

  METHOD get_descriptions_class.
    CLEAR et_descriptions.

    IF mv_prepared = abap_false.
      RETURN.
    ENDIF.

    READ TABLE mt_classtx INTO DATA(ls_cache)
      WITH TABLE KEY clsname = iv_clsname.
    IF sy-subrc = 0.
      et_descriptions = ls_cache-descriptions.
      rv_found = abap_true.
    ENDIF.
  ENDMETHOD.

  METHOD get_descriptions_compo.
    CLEAR et_descriptions.

    IF mv_prepared = abap_false.
      RETURN.
    ENDIF.

    READ TABLE mt_compotx INTO DATA(ls_cache)
      WITH TABLE KEY clsname = iv_clsname.
    IF sy-subrc = 0.
      " Filter by language if specified
      IF iv_language IS NOT INITIAL.
        LOOP AT ls_cache-descriptions INTO DATA(ls_descr)
          WHERE langu = iv_language.
          APPEND ls_descr TO et_descriptions.
        ENDLOOP.
      ELSE.
        et_descriptions = ls_cache-descriptions.
      ENDIF.
      rv_found = abap_true.
    ENDIF.
  ENDMETHOD.

  METHOD get_descriptions_subco.
    CLEAR et_descriptions.

    IF mv_prepared = abap_false.
      RETURN.
    ENDIF.

    READ TABLE mt_subcotx INTO DATA(ls_cache)
      WITH TABLE KEY clsname = iv_clsname.
    IF sy-subrc = 0.
      " Filter by language if specified
      IF iv_language IS NOT INITIAL.
        LOOP AT ls_cache-descriptions INTO DATA(ls_descr)
          WHERE langu = iv_language.
          APPEND ls_descr TO et_descriptions.
        ENDLOOP.
      ELSE.
        et_descriptions = ls_cache-descriptions.
      ENDIF.
      rv_found = abap_true.
    ENDIF.
  ENDMETHOD.

  METHOD extract_for_object.
    DATA lt_classtx TYPE ty_classtx_cache_tt.
    DATA lt_compotx TYPE ty_compotx_cache_tt.
    DATA lt_subcotx TYPE ty_subcotx_cache_tt.
    DATA lv_has_data TYPE abap_bool.

    CHECK is_tadir-object = 'CLAS' OR is_tadir-object = 'INTF'.

    DATA(lv_clsname) = CONV seoclsname( is_tadir-obj_name ).

    READ TABLE mt_classtx INTO DATA(ls_classtx)
      WITH TABLE KEY clsname = lv_clsname.
    IF sy-subrc = 0.
      INSERT ls_classtx INTO TABLE lt_classtx.
      lv_has_data = abap_true.
    ENDIF.

    READ TABLE mt_compotx INTO DATA(ls_compotx)
      WITH TABLE KEY clsname = lv_clsname.
    IF sy-subrc = 0.
      INSERT ls_compotx INTO TABLE lt_compotx.
      lv_has_data = abap_true.
    ENDIF.

    READ TABLE mt_subcotx INTO DATA(ls_subcotx)
      WITH TABLE KEY clsname = lv_clsname.
    IF sy-subrc = 0.
      INSERT ls_subcotx INTO TABLE lt_subcotx.
      lv_has_data = abap_true.
    ENDIF.

    IF lv_has_data = abap_false.
      RETURN.
    ENDIF.

    EXPORT classtx = lt_classtx
           compotx = lt_compotx
           subcotx = lt_subcotx
           language = mv_language
      TO DATA BUFFER rv_buffer COMPRESSION ON.
  ENDMETHOD.

  METHOD inject_from_buffer.
    DATA lt_classtx TYPE ty_classtx_cache_tt.
    DATA lt_compotx TYPE ty_compotx_cache_tt.
    DATA lt_subcotx TYPE ty_subcotx_cache_tt.
    DATA lv_language TYPE spras.

    CHECK iv_buffer IS NOT INITIAL.

    IMPORT classtx = lt_classtx
           compotx = lt_compotx
           subcotx = lt_subcotx
           language = lv_language
      FROM DATA BUFFER iv_buffer.                       "#EC CI_SUBRC
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    " CLEAR first: a parallel RFC worker session can be reused across many
    " unrelated dispatches over its lifetime, and INSERT INTO a UNIQUE-keyed
    " table silently no-ops if a prior invocation already cached that same
    " key - without clearing, a worker would serve stale data forever.
    CLEAR: mt_classtx, mt_compotx, mt_subcotx.
    LOOP AT lt_classtx INTO DATA(ls_classtx).
      INSERT ls_classtx INTO TABLE mt_classtx.
    ENDLOOP.
    LOOP AT lt_compotx INTO DATA(ls_compotx).
      INSERT ls_compotx INTO TABLE mt_compotx.
    ENDLOOP.
    LOOP AT lt_subcotx INTO DATA(ls_subcotx).
      INSERT ls_subcotx INTO TABLE mt_subcotx.
    ENDLOOP.

    IF lv_language IS NOT INITIAL.
      mv_language = lv_language.
    ENDIF.
    mv_prepared = abap_true.
  ENDMETHOD.
ENDCLASS.
