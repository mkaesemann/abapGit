CLASS zcl_abapgit_ortec_ser_pref DEFINITION LOCAL FRIENDS ltcl_msag_batch_wire.

CLASS ltcl_msag_batch_wire DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  " SER-SLICE-3 Phase 6 (serialization_slice_3_msag.md):
  " EXTRACT_FOR_BATCH / INJECT_BATCH_FROM_BUFFER wire-envelope contract for
  " the MSAG (T100/T100A/T100T) provider, mirroring
  " ZCL_ABAPGIT_ORTEC_SER_PREF_OO's LTCL_OO_BATCH_WIRE test class exactly.
  " No live MSAG objects are read from the database - fixtures are seeded
  " directly into the private MT_MSAG cache via friend access, with
  " MV_LANGUAGE set the same way PREPARE() itself would leave it, so every
  " test is deterministic and independent of live SAP Basis DDIC content.
  " MT_DOKIL/MV_DOKIL_PREPARED are explicitly OUT OF SCOPE for this batch
  " envelope - see INJECT_DOES_NOT_TOUCH_DOKIL for the regression proving
  " that boundary.

  PRIVATE SECTION.
    METHODS setup.
    METHODS teardown.

    METHODS seed_msag
      IMPORTING iv_msg_id TYPE rglif-message_id
                iv_msgnr  TYPE t100-msgnr DEFAULT '000'
                iv_langu  TYPE spras DEFAULT 'D'
                iv_text   TYPE t100-text DEFAULT 'Message text'.

    METHODS build_raw_buffer
      IMPORTING is_hdr           TYPE zaog_ser_env_bhdr
                it_entries       TYPE zaog_ser_env_bentry_tt
      RETURNING VALUE(rv_buffer) TYPE xstring.
    METHODS read_envelope
      IMPORTING iv_buffer  TYPE xstring
      EXPORTING es_hdr     TYPE zaog_ser_env_bhdr
                et_entries TYPE zaog_ser_env_bentry_tt.

    METHODS small_message_class_one_row     FOR TESTING RAISING zcx_abapgit_exception.
    METHODS message_class_multiple_msgnrs   FOR TESTING RAISING zcx_abapgit_exception.
    METHODS multiple_languages_round_trip   FOR TESTING RAISING zcx_abapgit_exception.
    METHODS namespaced_name_round_trip      FOR TESTING RAISING zcx_abapgit_exception.
    METHODS mixed_msag_batch_partial        FOR TESTING RAISING zcx_abapgit_exception.
    METHODS all_miss_batch_is_initial       FOR TESTING.
    METHODS reject_unknown_version          FOR TESTING.
    METHODS reject_duplicate_entries        FOR TESTING.
    METHODS reject_count_mismatch           FOR TESTING.
    METHODS reject_corrupt_import           FOR TESTING.
    METHODS cross_batch_isolation           FOR TESTING RAISING zcx_abapgit_exception.
    METHODS full_round_trip_byte_ident      FOR TESTING RAISING zcx_abapgit_exception.
    METHODS extract_no_msag_objects_empty   FOR TESTING.
    METHODS extract_not_prepared_empty      FOR TESTING.
    METHODS inject_does_not_touch_dokil     FOR TESTING RAISING zcx_abapgit_exception.

ENDCLASS.


CLASS ltcl_msag_batch_wire IMPLEMENTATION.

  METHOD setup.
    zcl_abapgit_ortec_ser_pref=>clear( ).
  ENDMETHOD.

  METHOD teardown.
    zcl_abapgit_ortec_ser_pref=>clear( ).
  ENDMETHOD.

  METHOD seed_msag.
    DATA ls_t100  TYPE t100.
    DATA ls_t100t TYPE t100t.

    IF NOT line_exists( zcl_abapgit_ortec_ser_pref=>mt_msag[ msg_id = iv_msg_id ] ).
      INSERT VALUE #( msg_id = iv_msg_id
                       data   = VALUE #( t100a = VALUE #( arbgb = iv_msg_id ) ) )
        INTO TABLE zcl_abapgit_ortec_ser_pref=>mt_msag.
    ENDIF.

    ls_t100-arbgb = iv_msg_id.
    ls_t100-msgnr = iv_msgnr.
    ls_t100-sprsl = iv_langu.
    ls_t100-text  = iv_text.

    ls_t100t-arbgb = iv_msg_id.
    ls_t100t-sprsl = iv_langu.
    ls_t100t-stext = iv_text.

    READ TABLE zcl_abapgit_ortec_ser_pref=>mt_msag ASSIGNING FIELD-SYMBOL(<ls_cache>)
      WITH TABLE KEY msg_id = iv_msg_id.

    IF iv_langu = zcl_abapgit_ortec_ser_pref=>mv_language.
      APPEND ls_t100 TO <ls_cache>-data-t100.
    ELSE.
      APPEND ls_t100 TO <ls_cache>-data-t100_i18n.
    ENDIF.
    APPEND ls_t100t TO <ls_cache>-data-t100t.
  ENDMETHOD.

  METHOD build_raw_buffer.
    DATA lt_msag TYPE zcl_abapgit_ortec_ser_pref=>ty_msag_cache_tt.

    EXPORT hdr      = is_hdr
           entries  = it_entries
           msag     = lt_msag
           language = 'E'
      TO DATA BUFFER rv_buffer COMPRESSION ON.
  ENDMETHOD.

  METHOD read_envelope.
    DATA lt_msag TYPE zcl_abapgit_ortec_ser_pref=>ty_msag_cache_tt.
    DATA lv_language TYPE spras.

    IMPORT hdr      = es_hdr
           entries  = et_entries
           msag     = lt_msag
           language = lv_language
      FROM DATA BUFFER iv_buffer.
  ENDMETHOD.

  METHOD small_message_class_one_row.
    zcl_abapgit_ortec_ser_pref=>mv_language = 'E'.
    seed_msag( iv_msg_id = 'ZTEST_SMALL' iv_msgnr = '000' iv_langu = 'D' iv_text = 'Kleine Nachricht' ).

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt( ( object = 'MSAG' obj_name = 'ZTEST_SMALL' ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref=>extract_for_batch( lt_tadir ).
    cl_abap_unit_assert=>assert_not_initial( lv_buffer ).

    zcl_abapgit_ortec_ser_pref=>clear( ).
    zcl_abapgit_ortec_ser_pref=>inject_batch_from_buffer( lv_buffer ).

    DATA es_data TYPE zcl_abapgit_ortec_ser_pref=>ty_msag_data.
    DATA(rv_found) = zcl_abapgit_ortec_ser_pref=>get_msag_data(
      EXPORTING iv_msg_id = 'ZTEST_SMALL' iv_language = 'E'
      IMPORTING es_data = es_data ).

    cl_abap_unit_assert=>assert_true( rv_found ).
    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( es_data-t100t ) ).
    READ TABLE es_data-t100t INDEX 1 INTO DATA(ls_t100t).
    cl_abap_unit_assert=>assert_equals( exp = 'Kleine Nachricht' act = ls_t100t-stext ).
  ENDMETHOD.

  METHOD message_class_multiple_msgnrs.
    zcl_abapgit_ortec_ser_pref=>mv_language = 'E'.

    DO 5 TIMES.
      seed_msag( iv_msg_id = 'ZTEST_MULTI' iv_msgnr = CONV #( sy-index ) iv_langu = 'E'
                 iv_text = |Message { sy-index }| ).
    ENDDO.

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt( ( object = 'MSAG' obj_name = 'ZTEST_MULTI' ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref=>extract_for_batch( lt_tadir ).
    cl_abap_unit_assert=>assert_not_initial( lv_buffer ).

    zcl_abapgit_ortec_ser_pref=>clear( ).
    zcl_abapgit_ortec_ser_pref=>inject_batch_from_buffer( lv_buffer ).

    DATA es_data TYPE zcl_abapgit_ortec_ser_pref=>ty_msag_data.
    zcl_abapgit_ortec_ser_pref=>get_msag_data(
      EXPORTING iv_msg_id = 'ZTEST_MULTI' iv_language = 'E'
      IMPORTING es_data = es_data ).

    cl_abap_unit_assert=>assert_equals( exp = 5 act = lines( es_data-t100 ) ).
    cl_abap_unit_assert=>assert_equals( exp = 5 act = lines( es_data-t100t ) ).
  ENDMETHOD.

  METHOD multiple_languages_round_trip.
    zcl_abapgit_ortec_ser_pref=>mv_language = 'E'.
    seed_msag( iv_msg_id = 'ZTEST_LANG' iv_msgnr = '000' iv_langu = 'E' iv_text = 'English text' ).
    seed_msag( iv_msg_id = 'ZTEST_LANG' iv_msgnr = '000' iv_langu = 'D' iv_text = 'German text' ).
    seed_msag( iv_msg_id = 'ZTEST_LANG' iv_msgnr = '000' iv_langu = 'F' iv_text = 'French text' ).

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt( ( object = 'MSAG' obj_name = 'ZTEST_LANG' ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref=>extract_for_batch( lt_tadir ).
    cl_abap_unit_assert=>assert_not_initial( lv_buffer ).

    zcl_abapgit_ortec_ser_pref=>clear( ).
    zcl_abapgit_ortec_ser_pref=>inject_batch_from_buffer( lv_buffer ).

    DATA et_i18n_langs TYPE zcl_abapgit_ortec_ser_pref=>ty_langu_tt.
    DATA et_t100t      TYPE zcl_abapgit_ortec_ser_pref=>ty_t100t_tt.
    DATA et_t100_i18n  TYPE zcl_abapgit_ortec_ser_pref=>ty_t100_tt.
    DATA(rv_found) = zcl_abapgit_ortec_ser_pref=>get_msag_i18n_data(
      EXPORTING iv_msg_id = 'ZTEST_LANG' iv_language = 'E'
      IMPORTING et_i18n_langs = et_i18n_langs et_t100t = et_t100t et_t100_i18n = et_t100_i18n ).

    cl_abap_unit_assert=>assert_true( rv_found ).
    cl_abap_unit_assert=>assert_equals( exp = 2 act = lines( et_i18n_langs ) ).
    cl_abap_unit_assert=>assert_equals( exp = 2 act = lines( et_t100_i18n ) ).
  ENDMETHOD.

  METHOD namespaced_name_round_trip.
    " CHAR40-safe round trip, mirroring the OO/DD provider's identical
    " check for the OBJ_NAME (SOBJ_NAME) field.
    DATA(lv_msg_id) = CONV rglif-message_id( '/NS/ZTEST_MSG' ).
    zcl_abapgit_ortec_ser_pref=>mv_language = 'E'.
    seed_msag( iv_msg_id = lv_msg_id iv_langu = 'D' iv_text = 'Namespaced message' ).

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt( ( object = 'MSAG' obj_name = CONV #( lv_msg_id ) ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref=>extract_for_batch( lt_tadir ).
    cl_abap_unit_assert=>assert_not_initial( lv_buffer ).

    zcl_abapgit_ortec_ser_pref=>clear( ).
    zcl_abapgit_ortec_ser_pref=>inject_batch_from_buffer( lv_buffer ).

    DATA(rv_found) = zcl_abapgit_ortec_ser_pref=>get_msag_data( iv_msg_id = lv_msg_id iv_language = 'E' ).
    cl_abap_unit_assert=>assert_true( rv_found ).
  ENDMETHOD.

  METHOD mixed_msag_batch_partial.
    zcl_abapgit_ortec_ser_pref=>mv_language = 'E'.
    seed_msag( iv_msg_id = 'ZTEST_PRESENT' iv_langu = 'D' iv_text = 'Present message' ).

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( object = 'MSAG' obj_name = 'ZTEST_PRESENT' )
      ( object = 'MSAG' obj_name = 'ZTEST_ABSENT' ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref=>extract_for_batch( lt_tadir ).
    cl_abap_unit_assert=>assert_not_initial( lv_buffer ).

    DATA ls_hdr TYPE zaog_ser_env_bhdr.
    DATA lt_entries TYPE zaog_ser_env_bentry_tt.
    read_envelope( EXPORTING iv_buffer = lv_buffer IMPORTING es_hdr = ls_hdr et_entries = lt_entries ).

    READ TABLE lt_entries INTO DATA(ls_present) WITH KEY obj_name = 'ZTEST_PRESENT'.
    cl_abap_unit_assert=>assert_subrc( exp = 0 act = sy-subrc ).
    cl_abap_unit_assert=>assert_equals( exp = 'P' act = ls_present-state ).

    READ TABLE lt_entries INTO DATA(ls_absent) WITH KEY obj_name = 'ZTEST_ABSENT'.
    cl_abap_unit_assert=>assert_subrc( exp = 0 act = sy-subrc ).
    cl_abap_unit_assert=>assert_equals( exp = 'M' act = ls_absent-state ).
    cl_abap_unit_assert=>assert_equals( exp = 0 act = ls_absent-actual_bytes ).

    " No cross-contamination: injecting must not expose ZTEST_ABSENT data.
    zcl_abapgit_ortec_ser_pref=>clear( ).
    zcl_abapgit_ortec_ser_pref=>inject_batch_from_buffer( lv_buffer ).

    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref=>get_msag_data( iv_msg_id = 'ZTEST_PRESENT' iv_language = 'E' ) ).
    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_pref=>get_msag_data( iv_msg_id = 'ZTEST_ABSENT' iv_language = 'E' ) ).
  ENDMETHOD.

  METHOD all_miss_batch_is_initial.
    " No PREPARE()-equivalent flag exists on this class - MT_MSAG simply
    " stays empty until PREPARE() populates it, so every lookup below
    " misses and no envelope must be built, mirroring the OO/DD providers'
    " identical "nothing genuinely useful to send" guard.
    zcl_abapgit_ortec_ser_pref=>mv_language = 'E'.

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( object = 'MSAG' obj_name = 'ZTEST_NEVER_CACHED' )
      ( object = 'MSAG' obj_name = 'ZTEST_NEVER_CACHED2' ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref=>extract_for_batch( lt_tadir ).

    cl_abap_unit_assert=>assert_initial( lv_buffer ).
  ENDMETHOD.

  METHOD reject_unknown_version.
    DATA(ls_hdr) = VALUE zaog_ser_env_bhdr(
      wire_format_version = 99
      provider_id         = 'SER_MSAG'
      object_count        = 0 ).
    DATA(lv_buffer) = build_raw_buffer( is_hdr = ls_hdr it_entries = VALUE #( ) ).

    TRY.
        zcl_abapgit_ortec_ser_pref=>inject_batch_from_buffer( lv_buffer ).
        cl_abap_unit_assert=>fail( 'expected zcx_abapgit_exception' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        cl_abap_unit_assert=>assert_char_cp(
          act = lx_error->get_text( ) exp = '*wire_format_version*' ).
    ENDTRY.
  ENDMETHOD.

  METHOD reject_duplicate_entries.
    DATA(ls_hdr) = VALUE zaog_ser_env_bhdr(
      wire_format_version = 1
      provider_id         = 'SER_MSAG'
      object_count        = 2 ).
    DATA(lt_entries) = VALUE zaog_ser_env_bentry_tt(
      ( obj_type = 'MSAG' obj_name = 'ZTEST_DUP' state = 'P' )
      ( obj_type = 'MSAG' obj_name = 'ZTEST_DUP' state = 'P' ) ).
    DATA(lv_buffer) = build_raw_buffer( is_hdr = ls_hdr it_entries = lt_entries ).

    TRY.
        zcl_abapgit_ortec_ser_pref=>inject_batch_from_buffer( lv_buffer ).
        cl_abap_unit_assert=>fail( 'expected zcx_abapgit_exception' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        cl_abap_unit_assert=>assert_char_cp(
          act = lx_error->get_text( ) exp = '*duplicate*' ).
    ENDTRY.
  ENDMETHOD.

  METHOD reject_count_mismatch.
    DATA(ls_hdr) = VALUE zaog_ser_env_bhdr(
      wire_format_version = 1
      provider_id         = 'SER_MSAG'
      object_count        = 5 ).
    DATA(lt_entries) = VALUE zaog_ser_env_bentry_tt(
      ( obj_type = 'MSAG' obj_name = 'ZTEST_ONE' state = 'P' ) ).
    DATA(lv_buffer) = build_raw_buffer( is_hdr = ls_hdr it_entries = lt_entries ).

    TRY.
        zcl_abapgit_ortec_ser_pref=>inject_batch_from_buffer( lv_buffer ).
        cl_abap_unit_assert=>fail( 'expected zcx_abapgit_exception' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        cl_abap_unit_assert=>assert_char_cp(
          act = lx_error->get_text( ) exp = '*object_count*' ).
    ENDTRY.
  ENDMETHOD.

  METHOD reject_corrupt_import.
    DATA(lv_buffer) = build_raw_buffer(
      is_hdr     = VALUE #( wire_format_version = 1 provider_id = 'SER_MSAG' object_count = 0 )
      it_entries = VALUE #( ) ).
    DATA(lv_corrupt) = lv_buffer(3).

    TRY.
        zcl_abapgit_ortec_ser_pref=>inject_batch_from_buffer( lv_corrupt ).
        cl_abap_unit_assert=>fail( 'expected zcx_abapgit_exception for a corrupt buffer' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        cl_abap_unit_assert=>assert_char_cp(
          act = lx_error->get_text( ) exp = '*corrupt*' ).
    ENDTRY.
  ENDMETHOD.

  METHOD cross_batch_isolation.
    " Simulates two sequential worker-style INJECT_BATCH_FROM_BUFFER calls
    " for two independently PREPARE()'d batches - the second call must not
    " retain any of the first batch's cached rows.
    zcl_abapgit_ortec_ser_pref=>mv_language = 'E'.
    seed_msag( iv_msg_id = 'ZTEST_BATCH_A' iv_langu = 'D' iv_text = 'Batch A' ).
    DATA(lv_buffer_a) = zcl_abapgit_ortec_ser_pref=>extract_for_batch(
      VALUE #( ( object = 'MSAG' obj_name = 'ZTEST_BATCH_A' ) ) ).

    zcl_abapgit_ortec_ser_pref=>clear( ).
    zcl_abapgit_ortec_ser_pref=>mv_language = 'E'.
    seed_msag( iv_msg_id = 'ZTEST_BATCH_B' iv_langu = 'D' iv_text = 'Batch B' ).
    DATA(lv_buffer_b) = zcl_abapgit_ortec_ser_pref=>extract_for_batch(
      VALUE #( ( object = 'MSAG' obj_name = 'ZTEST_BATCH_B' ) ) ).

    zcl_abapgit_ortec_ser_pref=>clear( ).
    zcl_abapgit_ortec_ser_pref=>inject_batch_from_buffer( lv_buffer_a ).
    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref=>get_msag_data( iv_msg_id = 'ZTEST_BATCH_A' iv_language = 'E' ) ).
    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_pref=>get_msag_data( iv_msg_id = 'ZTEST_BATCH_B' iv_language = 'E' ) ).

    " A pooled worker session is reused for the SECOND dispatch WITHOUT an
    " intervening CLEAR( ) - INJECT_BATCH_FROM_BUFFER itself must clear
    " MT_MSAG, not rely on the caller to do so.
    zcl_abapgit_ortec_ser_pref=>inject_batch_from_buffer( lv_buffer_b ).
    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref=>get_msag_data( iv_msg_id = 'ZTEST_BATCH_B' iv_language = 'E' ) ).
    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_pref=>get_msag_data( iv_msg_id = 'ZTEST_BATCH_A' iv_language = 'E' ) ).
  ENDMETHOD.

  METHOD full_round_trip_byte_ident.
    " Full EXTRACT_FOR_BATCH -> (simulated worker) INJECT_BATCH_FROM_BUFFER
    " -> GET_MSAG_DATA round trip returns byte-identical data to what was
    " originally prepared, for a fixture with several message classes,
    " message numbers and languages. Live serialized-file-set parity
    " against a real MSAG object remains an IT8-only step (no live SAP
    " connectivity this session) - same disclosed boundary DOMA/DTEL/OO
    " had before their own IT8 validation.
    zcl_abapgit_ortec_ser_pref=>mv_language = 'E'.
    seed_msag( iv_msg_id = 'ZTEST_FULL_A' iv_msgnr = '000' iv_langu = 'E' iv_text = 'Full A EN' ).
    seed_msag( iv_msg_id = 'ZTEST_FULL_A' iv_msgnr = '000' iv_langu = 'D' iv_text = 'Full A DE' ).
    seed_msag( iv_msg_id = 'ZTEST_FULL_A' iv_msgnr = '001' iv_langu = 'E' iv_text = 'Full A EN 2' ).
    seed_msag( iv_msg_id = 'ZTEST_FULL_B' iv_msgnr = '000' iv_langu = 'D' iv_text = 'Full B DE' ).

    DATA es_data_a_before TYPE zcl_abapgit_ortec_ser_pref=>ty_msag_data.
    DATA es_data_b_before TYPE zcl_abapgit_ortec_ser_pref=>ty_msag_data.
    zcl_abapgit_ortec_ser_pref=>get_msag_data(
      EXPORTING iv_msg_id = 'ZTEST_FULL_A' iv_language = 'E' IMPORTING es_data = es_data_a_before ).
    zcl_abapgit_ortec_ser_pref=>get_msag_data(
      EXPORTING iv_msg_id = 'ZTEST_FULL_B' iv_language = 'E' IMPORTING es_data = es_data_b_before ).

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( object = 'MSAG' obj_name = 'ZTEST_FULL_A' )
      ( object = 'MSAG' obj_name = 'ZTEST_FULL_B' ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref=>extract_for_batch( lt_tadir ).
    cl_abap_unit_assert=>assert_not_initial( lv_buffer ).

    zcl_abapgit_ortec_ser_pref=>clear( ).
    zcl_abapgit_ortec_ser_pref=>inject_batch_from_buffer( lv_buffer ).

    DATA es_data_a_after TYPE zcl_abapgit_ortec_ser_pref=>ty_msag_data.
    DATA es_data_b_after TYPE zcl_abapgit_ortec_ser_pref=>ty_msag_data.
    zcl_abapgit_ortec_ser_pref=>get_msag_data(
      EXPORTING iv_msg_id = 'ZTEST_FULL_A' iv_language = 'E' IMPORTING es_data = es_data_a_after ).
    zcl_abapgit_ortec_ser_pref=>get_msag_data(
      EXPORTING iv_msg_id = 'ZTEST_FULL_B' iv_language = 'E' IMPORTING es_data = es_data_b_after ).

    cl_abap_unit_assert=>assert_equals( exp = es_data_a_before act = es_data_a_after ).
    cl_abap_unit_assert=>assert_equals( exp = es_data_b_before act = es_data_b_after ).
  ENDMETHOD.

  METHOD extract_no_msag_objects_empty.
    zcl_abapgit_ortec_ser_pref=>mv_language = 'E'.

    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref=>extract_for_batch(
      VALUE #( ( object = 'PROG' obj_name = 'SAPMZ_TEST' ) ) ).

    cl_abap_unit_assert=>assert_initial( lv_buffer ).
  ENDMETHOD.

  METHOD extract_not_prepared_empty.
    " PREPARE() was never called - MT_MSAG is empty, so every lookup below
    " misses and EXTRACT_FOR_BATCH must return INITIAL with no DB access,
    " even though the object keys are genuine MSAG rows.
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref=>extract_for_batch(
      VALUE #( ( object = 'MSAG' obj_name = 'ZTEST_ANYTHING' ) ) ).

    cl_abap_unit_assert=>assert_initial( lv_buffer ).
  ENDMETHOD.

  METHOD inject_does_not_touch_dokil.
    " Regression proving the disclosed scope boundary
    " (serialization_slice_3_msag.md): DOKIL is explicitly OUT OF SCOPE
    " for the MSAG batch envelope. MT_DOKIL/MV_DOKIL_PREPARED are seeded
    " directly via friend access (no live DB read needed) and must be
    " left completely untouched by INJECT_BATCH_FROM_BUFFER, even though
    " that call unconditionally clears MT_MSAG.
    INSERT VALUE #( id = 'DE' object = 'ZTEST_SENTINEL' ) INTO TABLE zcl_abapgit_ortec_ser_pref=>mt_dokil.
    zcl_abapgit_ortec_ser_pref=>mv_dokil_prepared = abap_true.

    zcl_abapgit_ortec_ser_pref=>mv_language = 'E'.
    seed_msag( iv_msg_id = 'ZTEST_DOKIL_SCOPE' iv_langu = 'D' iv_text = 'Message text' ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref=>extract_for_batch(
      VALUE #( ( object = 'MSAG' obj_name = 'ZTEST_DOKIL_SCOPE' ) ) ).
    cl_abap_unit_assert=>assert_not_initial( lv_buffer ).

    zcl_abapgit_ortec_ser_pref=>inject_batch_from_buffer( lv_buffer ).

    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( zcl_abapgit_ortec_ser_pref=>mt_dokil ) ).
    cl_abap_unit_assert=>assert_true( zcl_abapgit_ortec_ser_pref=>mv_dokil_prepared ).
  ENDMETHOD.

ENDCLASS.
