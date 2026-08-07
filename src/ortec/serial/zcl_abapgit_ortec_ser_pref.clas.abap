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

    "! SER-SLICE-3 Phase 6 (serialization_slice_3_msag.md): extract the
    "! MSAG batch prefetch envelope for an entire dispatch's TADIR rows in
    "! ONE call, mirroring ZCL_ABAPGIT_ORTEC_SER_PREF_OO's
    "! EXTRACT_FOR_BATCH. Filters IT_OBJECT_KEYS to MSAG internally;
    "! returns an INITIAL buffer with no DB access when none are present,
    "! when PREPARE() was never called, or when every entry would be a
    "! MISS (nothing genuinely useful to send). MT_DOKIL long-text
    "! documentation is explicitly OUT OF SCOPE for this envelope
    "! (disclosed scope boundary, see serialization_slice_3_msag.md) -
    "! ACTUAL_BYTES measurement reuses EXTRACT_FOR_OBJECT's own wire shape
    "! for sizing only, which may itself include DOKIL bytes for this
    "! object; that does not leak DOKIL data into this envelope's payload.
    CLASS-METHODS extract_for_batch
      IMPORTING it_object_keys   TYPE zif_abapgit_definitions=>ty_tadir_tt
      RETURNING VALUE(rv_buffer) TYPE xstring.

    "! SER-SLICE-3 Phase 6: inject an MSAG batch prefetch envelope
    "! (produced by EXTRACT_FOR_BATCH) into this session's MT_MSAG cache.
    "! Unknown wire format version, a failed IMPORT, or a duplicate
    "! ENTRIES row all reject the WHOLE buffer by raising
    "! ZCX_ABAPGIT_EXCEPTION - callers must treat this as a full prefetch
    "! MISS for this buffer only, never propagate. Only MT_MSAG is
    "! touched - MT_DOKIL/MV_DOKIL_PREPARED are out of scope for this
    "! envelope and are left completely untouched.
    CLASS-METHODS inject_batch_from_buffer
      IMPORTING iv_buffer TYPE xstring
      RAISING   zcx_abapgit_exception.

    "! SER-SLICE-3 Phase 6 (parity pattern, AR-3-001 / CLEAR_OO_CACHE):
    "! unconditional CLEAR of MT_MSAG only - narrower than CLEAR, which
    "! also clears MT_DOKIL/MV_DOKIL_PREPARED/MV_LANGUAGE. The RFC worker
    "! calls this FIRST, on EVERY invocation, before conditionally
    "! injecting a new buffer.
    CLASS-METHODS clear_msag_cache.

  PRIVATE SECTION.
    TYPES:
      BEGIN OF ty_msag_cache,
        msg_id TYPE rglif-message_id,
        data   TYPE ty_msag_data,
      END OF ty_msag_cache.
    TYPES ty_msag_cache_tt TYPE HASHED TABLE OF ty_msag_cache WITH UNIQUE KEY msg_id.
    TYPES ty_msg_ids TYPE HASHED TABLE OF rglif-message_id WITH UNIQUE KEY table_line.

    CLASS-DATA mt_msag TYPE ty_msag_cache_tt.
    CLASS-DATA mt_dokil TYPE SORTED TABLE OF dokil
      WITH NON-UNIQUE KEY id object
      WITH NON-UNIQUE SORTED KEY object_prefix COMPONENTS object.
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
    DATA lv_object_high TYPE dokil-object.
    DATA lv_object_len TYPE i.
    DATA lv_object_type_len TYPE i.

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
      lv_object_len = strlen( lv_object ).
      IF lv_object_len > 0.
        DESCRIBE FIELD lv_object_high LENGTH lv_object_type_len IN CHARACTER MODE.

        IF lv_object_len < lv_object_type_len.
          lv_object_high = lv_object.
          " Build an exclusive upper bound for object prefix range selection
          lv_object_high+lv_object_len(1) = cl_abap_char_utilities=>maxchar.

          LOOP AT mt_dokil INTO DATA(ls_dokil)
               USING KEY object_prefix
               WHERE object >= lv_object
                 AND object <  lv_object_high.
            INSERT ls_dokil INTO TABLE lt_dokil.
          ENDLOOP.
        ELSE.
          LOOP AT mt_dokil INTO ls_dokil
               USING KEY object_prefix
               WHERE object = lv_object.
            INSERT ls_dokil INTO TABLE lt_dokil.
          ENDLOOP.
        ENDIF.
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

    " Merge into session-local caches. CLEAR first: a parallel RFC worker
    " session can be reused across many unrelated dispatches over its
    " lifetime, and INSERT INTO a UNIQUE-keyed table silently no-ops if a
    " prior invocation already cached that same key - without clearing,
    " a worker would serve stale data for that object forever.
    CLEAR: mt_msag, mt_dokil.
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


  METHOD extract_for_batch.
    DATA lt_entries TYPE zaog_ser_env_bentry_tt.
    DATA lt_msag    TYPE ty_msag_cache_tt.
    DATA ls_hdr     TYPE zaog_ser_env_bhdr.
    DATA lv_any_hit TYPE abap_bool.

    LOOP AT it_object_keys INTO DATA(ls_tadir) WHERE object = 'MSAG'.
      DATA(ls_entry) = VALUE zaog_ser_env_bentry(
        obj_type = ls_tadir-object
        obj_name = ls_tadir-obj_name ).

      READ TABLE mt_msag INTO DATA(ls_msag)
        WITH TABLE KEY msg_id = CONV rglif-message_id( ls_tadir-obj_name ).
      IF sy-subrc = 0.
        ls_entry-state        = 'P'.
        " Cheapest correct per-object byte measure available: reuse
        " EXTRACT_FOR_OBJECT's own single-object wire shape purely to
        " measure this object's serialized size (see this method's own
        " class-level doc comment for the DOKIL sizing caveat).
        ls_entry-actual_bytes = xstrlen( extract_for_object( ls_tadir ) ).
        INSERT ls_msag INTO TABLE lt_msag.
        lv_any_hit = abap_true.
      ELSE.
        ls_entry-state        = 'M'.
        ls_entry-actual_bytes = 0.
      ENDIF.

      APPEND ls_entry TO lt_entries.
    ENDLOOP.

    " A batch with ZERO MSAG objects, or where EVERY entry would be a
    " MISS (including PREPARE() never having been called at all - MT_MSAG
    " would then be empty and every lookup above would MISS), has nothing
    " genuinely useful to send - mirrors ZCL_ABAPGIT_ORTEC_SER_PREF_OO's
    " and ZCL_ABAPGIT_ORTEC_SER_PREF_EXT's identical EXTRACT_FOR_BATCH
    " guard (SER-SLICE-3 parity incident Fix B).
    IF lt_entries IS INITIAL OR lv_any_hit = abap_false.
      CLEAR rv_buffer.
      RETURN.
    ENDIF.

    ls_hdr-wire_format_version = 1.
    ls_hdr-provider_id         = 'SER_MSAG'.
    ls_hdr-object_count        = lines( lt_entries ).

    EXPORT hdr      = ls_hdr
           entries  = lt_entries
           msag     = lt_msag
           language = mv_language
      TO DATA BUFFER rv_buffer COMPRESSION ON.
  ENDMETHOD.


  METHOD inject_batch_from_buffer.
    DATA ls_hdr            TYPE zaog_ser_env_bhdr.
    DATA lt_entries        TYPE zaog_ser_env_bentry_tt.
    DATA lt_msag           TYPE ty_msag_cache_tt.
    DATA lt_entries_sorted TYPE STANDARD TABLE OF zaog_ser_env_bentry WITH DEFAULT KEY.
    DATA lv_lines_before   TYPE i.
    DATA lv_language       TYPE spras.

    CHECK iv_buffer IS NOT INITIAL.

    TRY.
        IMPORT hdr      = ls_hdr
               entries  = lt_entries
               msag     = lt_msag
               language = lv_language
          FROM DATA BUFFER iv_buffer.
      CATCH cx_root INTO DATA(lx_import).
        zcx_abapgit_exception=>raise(
          |ORTEC MSAG batch prefetch buffer is corrupt: { lx_import->get_text( ) }| ).
    ENDTRY.
    IF sy-subrc <> 0.
      zcx_abapgit_exception=>raise( 'ORTEC MSAG batch prefetch buffer: IMPORT failed' ).
    ENDIF.

    IF ls_hdr-wire_format_version <> 1.
      zcx_abapgit_exception=>raise(
        |ORTEC MSAG batch prefetch buffer: unknown wire_format_version { ls_hdr-wire_format_version }| ).
    ENDIF.

    IF ls_hdr-object_count <> lines( lt_entries ).
      zcx_abapgit_exception=>raise(
        'ORTEC MSAG batch prefetch buffer: object_count does not match ENTRIES' ).
    ENDIF.

    " Duplicate check MUST happen before any INSERT INTO mt_msag - a
    " HASHED TABLE INSERT would otherwise silently collapse a duplicate
    " instead of rejecting the whole buffer.
    lt_entries_sorted = CORRESPONDING #( lt_entries ).
    SORT lt_entries_sorted BY obj_type obj_name.
    lv_lines_before = lines( lt_entries_sorted ).
    DELETE ADJACENT DUPLICATES FROM lt_entries_sorted COMPARING obj_type obj_name.
    IF lines( lt_entries_sorted ) <> lv_lines_before.
      zcx_abapgit_exception=>raise(
        'ORTEC MSAG batch prefetch buffer: duplicate entry in ENTRIES' ).
    ENDIF.

    " Unconditional clear of MT_MSAG only - MT_DOKIL/MV_DOKIL_PREPARED are
    " out of scope for this envelope (serialization_slice_3_msag.md) and
    " must survive untouched, unlike CLEAR( )'s full reset. A parallel RFC
    " worker session can also be reused across many unrelated dispatches
    " over its lifetime, so MT_MSAG must not keep stale rows either.
    CLEAR mt_msag.

    LOOP AT lt_msag INTO DATA(ls_msag).
      INSERT ls_msag INTO TABLE mt_msag.
    ENDLOOP.

    IF lv_language IS NOT INITIAL.
      mv_language = lv_language.
    ENDIF.
  ENDMETHOD.


  METHOD clear_msag_cache.
    CLEAR mt_msag.
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
