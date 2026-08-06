CLASS zcl_abapgit_ortec_ser_pref_ext DEFINITION LOCAL FRIENDS ltcl_dd_batch_wire.

CLASS ltcl_doma_parity DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  " SER-SLICE-1 parity harness (serialization_slice_1_doma_semantics.md).
  " Calls the REAL, unmodified ZCL_ABAPGIT_OBJECT_DOMA~serialize() as a
  " black box and pins its rendered XML output against real, stable SAP
  " Basis domains - no productive serialization logic is reimplemented
  " here. This is a TEMPORARY home (ZCL_ABAPGIT_ORTEC_SER_PREF_EXT already
  " owns the sibling DTEL prefetch cache this DOMA work will eventually
  " join); relocate to ZCL_ABAPGIT_ORTEC_SER_PROV_DD once SER-SLICE-3
  " creates it - do not duplicate these tests there, move them.
  "
  " Fixtures are real, universal SAP Basis domains (confirmed present via
  " live DDIC query, never created/modified by this test):
  "   XFELD  - active, 2 fixed values with texts ('X'->Yes, ''->No)
  "   CHAR30 - active, no fixed values (empty optional collection)
  "   a fabricated, never-existing name - proves BOTH "nonexistent object"
  "     and "inactive-only version" collapse to the identical observable
  "     behavior (empty payload, no exception) - see evidence log §2.

  PRIVATE SECTION.
    CONSTANTS c_never_exists TYPE ddobjname VALUE 'ZZZZ_SLICE1_NOT_A_REAL_DOMAIN'.

    METHODS setup.
    METHODS teardown.

    METHODS serialize_doma
      IMPORTING iv_domname    TYPE ddobjname
      RETURNING VALUE(rv_xml) TYPE string
      RAISING   zcx_abapgit_exception.

    METHODS active_with_fixed_values FOR TESTING RAISING zcx_abapgit_exception.
    METHODS active_without_fixed_values FOR TESTING RAISING zcx_abapgit_exception.
    METHODS nonexistent_or_inactive FOR TESTING RAISING zcx_abapgit_exception.
    METHODS exists_matches_serialize FOR TESTING RAISING zcx_abapgit_exception.
    METHODS output_is_stable FOR TESTING RAISING zcx_abapgit_exception.

    METHODS provider_hit_matches_baseline FOR TESTING RAISING zcx_abapgit_exception.
    METHODS provider_hit_no_fixed_values  FOR TESTING RAISING zcx_abapgit_exception.

ENDCLASS.


CLASS ltcl_doma_parity IMPLEMENTATION.

  METHOD setup.
    " Defensive: this class's tests must not depend on execution order
    " relative to LTCL_DD_BATCH_WIRE, since MT_DOMA is CLASS-DATA shared
    " across every local test class in this session.
    zcl_abapgit_ortec_ser_pref_ext=>clear( ).
  ENDMETHOD.

  METHOD teardown.
    " SER-SLICE-3: PROVIDER_HIT_* tests populate the shared, class-wide
    " MT_DOMA cache via PREPARE() - clear it after every test method so a
    " provider-hit test can never leak cached data into a sibling
    " feature-OFF-equivalent baseline test in this same class.
    zcl_abapgit_ortec_ser_pref_ext=>clear( ).
  ENDMETHOD.

  METHOD serialize_doma.
    DATA(lo_doma) = NEW zcl_abapgit_object_doma(
      is_item     = VALUE #( obj_type = 'DOMA' obj_name = iv_domname )
      iv_language = 'E' ).
    DATA(li_xml) = CAST zif_abapgit_xml_output( NEW zcl_abapgit_xml_output( ) ).

    lo_doma->zif_abapgit_object~serialize( li_xml ).

    rv_xml = li_xml->render( ).
  ENDMETHOD.

  METHOD active_with_fixed_values.
    DATA(lv_xml) = serialize_doma( 'XFELD' ).

    cl_abap_unit_assert=>assert_char_cp( act = lv_xml exp = '*<DATATYPE>CHAR</DATATYPE>*' ).
    cl_abap_unit_assert=>assert_char_cp( act = lv_xml exp = '*<LENG>000001</LENG>*' ).
    cl_abap_unit_assert=>assert_char_cp( act = lv_xml exp = '*<DOMVALUE_L>X</DOMVALUE_L>*' ).
    cl_abap_unit_assert=>assert_char_cp( act = lv_xml exp = '*Yes*' ).
    cl_abap_unit_assert=>assert_char_cp( act = lv_xml exp = '*No*' ).
  ENDMETHOD.

  METHOD active_without_fixed_values.
    DATA(lv_xml) = serialize_doma( 'CHAR30' ).

    cl_abap_unit_assert=>assert_char_cp( act = lv_xml exp = '*<DATATYPE>CHAR</DATATYPE>*' ).
    cl_abap_unit_assert=>assert_char_cp( act = lv_xml exp = '*<LENG>000030</LENG>*' ).
    " empty optional collection: no fixed-value rows at all
    cl_abap_unit_assert=>assert_equals( act = find( val = lv_xml sub = 'DOMVALUE_L' ) exp = -1 ).
  ENDMETHOD.

  METHOD nonexistent_or_inactive.
    " A domain with no active version is OBSERVATIONALLY IDENTICAL to one
    " that never existed at all (both hit ZCL_ABAPGIT_OBJECT_DOMA's own
    " `IF ls_dd01v IS INITIAL OR lv_state <> 'A'. RETURN.` branch) - no
    " exception, no DD01V node, confirmed from source (evidence log §2).
    DATA(lv_xml) = serialize_doma( c_never_exists ).

    cl_abap_unit_assert=>assert_equals( act = find( val = lv_xml sub = 'DD01V' ) exp = -1 ).
  ENDMETHOD.

  METHOD exists_matches_serialize.
    DATA(lo_exists) = NEW zcl_abapgit_object_doma(
      is_item     = VALUE #( obj_type = 'DOMA' obj_name = 'XFELD' )
      iv_language = 'E' ).
    cl_abap_unit_assert=>assert_true( lo_exists->zif_abapgit_object~exists( ) ).

    DATA(lo_absent) = NEW zcl_abapgit_object_doma(
      is_item     = VALUE #( obj_type = 'DOMA' obj_name = c_never_exists )
      iv_language = 'E' ).
    cl_abap_unit_assert=>assert_false( lo_absent->zif_abapgit_object~exists( ) ).
  ENDMETHOD.

  METHOD output_is_stable.
    " Standard-path reference result: two independent serialize() calls
    " for the same active domain must render byte-identical XML - any
    " future provider must reproduce this exact determinism.
    DATA(lv_xml_1) = serialize_doma( 'XFELD' ).
    DATA(lv_xml_2) = serialize_doma( 'XFELD' ).

    cl_abap_unit_assert=>assert_equals( act = lv_xml_1 exp = lv_xml_2 ).
  ENDMETHOD.

  METHOD provider_hit_matches_baseline.
    " SER-SLICE-3 (design §7, correctness review DR-002): the required
    " byte-identical-XML parity proof for a real provider HIT, not just
    " feature-OFF/empty-cache MISS as the other tests in this class prove.
    " IS_SERIAL_PREFETCH_ACTIVE defaults ABAP_TRUE already - the ONLY thing
    " that changes between a MISS and a HIT here is whether MT_DOMA has
    " been populated via PREPARE() first.
    DATA(lv_baseline) = serialize_doma( 'XFELD' ).           " MT_DOMA empty -> MISS -> standard DDIF_DOMA_GET path

    zcl_abapgit_ortec_ser_pref_ext=>prepare(
      it_tadir    = VALUE #( ( object = 'DOMA' obj_name = 'XFELD' ) )
      iv_language = 'E' ).
    DATA(lv_hit) = serialize_doma( 'XFELD' ).                 " MT_DOMA populated -> HIT -> provider path

    cl_abap_unit_assert=>assert_equals( act = lv_hit exp = lv_baseline ).
  ENDMETHOD.

  METHOD provider_hit_no_fixed_values.
    " Same parity proof for a domain with NO fixed values (empty optional
    " collection must stay empty under the provider path too).
    DATA(lv_baseline) = serialize_doma( 'CHAR30' ).

    zcl_abapgit_ortec_ser_pref_ext=>prepare(
      it_tadir    = VALUE #( ( object = 'DOMA' obj_name = 'CHAR30' ) )
      iv_language = 'E' ).
    DATA(lv_hit) = serialize_doma( 'CHAR30' ).

    cl_abap_unit_assert=>assert_equals( act = lv_hit exp = lv_baseline ).
  ENDMETHOD.

ENDCLASS.


CLASS ltcl_dd_batch_wire DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  " SER-SLICE-3 P2A (serialization_slice_3_provider_contract.md &sect;1/&sect;2):
  " EXTRACT_FOR_BATCH / INJECT_BATCH_FROM_BUFFER wire-envelope contract.
  " Reuses LTCL_DOMA_PARITY's real, universal DOMA fixtures (XFELD,
  " CHAR30, a fabricated never-existing domain name) plus MANDT as a
  " universal DTEL fixture. Friend access to the class's private cache
  " table types is used only to hand-craft corrupt envelopes for the
  " negative-path tests (build_raw_buffer).

  PRIVATE SECTION.
    CONSTANTS c_never_exists TYPE ddobjname VALUE 'ZZZZ_SLICE1_NOT_A_REAL_DOMAIN'.

    METHODS setup.
    METHODS teardown.

    METHODS build_raw_buffer
      IMPORTING is_hdr           TYPE zaog_ser_dd_bhdr
                it_entries       TYPE zaog_ser_dd_bentry_tt
      RETURNING VALUE(rv_buffer) TYPE xstring.

    METHODS extract_no_dd_objects_empty FOR TESTING.
    METHODS batch_round_trip_finds_data FOR TESTING RAISING zcx_abapgit_exception.
    METHODS reject_unknown_version      FOR TESTING.
    METHODS reject_count_mismatch       FOR TESTING.
    METHODS reject_duplicate_entries    FOR TESTING.
    METHODS no_cross_batch_leakage      FOR TESTING RAISING zcx_abapgit_exception.
    METHODS doma_miss_when_not_prepared FOR TESTING.
    METHODS reject_corrupt_import       FOR TESTING.
    METHODS unexpected_entry_ignored    FOR TESTING RAISING zcx_abapgit_exception.
    METHODS empty_payload_is_hit        FOR TESTING RAISING zcx_abapgit_exception.

ENDCLASS.


CLASS ltcl_dd_batch_wire IMPLEMENTATION.

  METHOD setup.
    zcl_abapgit_ortec_ser_pref_ext=>clear( ).
  ENDMETHOD.

  METHOD teardown.
    zcl_abapgit_ortec_ser_pref_ext=>clear( ).
  ENDMETHOD.

  METHOD build_raw_buffer.
    DATA lt_doma TYPE zcl_abapgit_ortec_ser_pref_ext=>ty_doma_cache_tt.
    DATA lt_dtel TYPE zcl_abapgit_ortec_ser_pref_ext=>ty_dtel_cache_tt.

    EXPORT hdr      = is_hdr
           entries  = it_entries
           doma     = lt_doma
           dtel     = lt_dtel
           language = 'E'
      TO DATA BUFFER rv_buffer COMPRESSION ON.
  ENDMETHOD.

  METHOD extract_no_dd_objects_empty.
    zcl_abapgit_ortec_ser_pref_ext=>clear( ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch(
      VALUE #( ( object = 'PROG' obj_name = 'SAPMZ_TEST' ) ) ).

    cl_abap_unit_assert=>assert_initial( lv_buffer ).
  ENDMETHOD.

  METHOD batch_round_trip_finds_data.
    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( object = 'DOMA' obj_name = 'XFELD' )
      ( object = 'DTEL' obj_name = 'MANDT' ) ).

    zcl_abapgit_ortec_ser_pref_ext=>prepare( it_tadir = lt_tadir iv_language = 'E' ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch( lt_tadir ).
    cl_abap_unit_assert=>assert_not_initial( lv_buffer ).

    zcl_abapgit_ortec_ser_pref_ext=>clear( ).
    zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer( lv_buffer ).

    DATA es_dd01v TYPE dd01v.
    DATA et_dd07v TYPE dd07v_tab.
    DATA(rv_doma_found) = zcl_abapgit_ortec_ser_pref_ext=>get_doma_data(
      EXPORTING iv_domname = 'XFELD' iv_language = 'E'
      IMPORTING es_dd01v = es_dd01v et_dd07v_tab = et_dd07v ).
    cl_abap_unit_assert=>assert_true( rv_doma_found ).
    cl_abap_unit_assert=>assert_equals( act = es_dd01v-domname exp = 'XFELD' ).

    DATA es_dd04v TYPE dd04v.
    DATA(rv_dtel_found) = zcl_abapgit_ortec_ser_pref_ext=>get_dtel_data(
      EXPORTING iv_rollname = 'MANDT' iv_language = 'E'
      IMPORTING es_dd04v = es_dd04v ).
    cl_abap_unit_assert=>assert_true( rv_dtel_found ).
    cl_abap_unit_assert=>assert_equals( act = es_dd04v-rollname exp = 'MANDT' ).
  ENDMETHOD.

  METHOD reject_unknown_version.
    DATA(ls_hdr) = VALUE zaog_ser_dd_bhdr(
      wire_format_version = 99
      provider_id         = 'SER_DD01'
      object_count        = 0 ).
    DATA(lv_buffer) = build_raw_buffer( is_hdr = ls_hdr it_entries = VALUE #( ) ).

    TRY.
        zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer( lv_buffer ).
        cl_abap_unit_assert=>fail( 'expected zcx_abapgit_exception' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        cl_abap_unit_assert=>assert_char_cp(
          act = lx_error->get_text( ) exp = '*wire_format_version*' ).
    ENDTRY.
  ENDMETHOD.

  METHOD reject_count_mismatch.
    DATA(ls_hdr) = VALUE zaog_ser_dd_bhdr(
      wire_format_version = 1
      provider_id         = 'SER_DD01'
      object_count        = 5 ).
    DATA(lt_entries) = VALUE zaog_ser_dd_bentry_tt(
      ( obj_type = 'DOMA' obj_name = 'XFELD' present = abap_true ) ).
    DATA(lv_buffer) = build_raw_buffer( is_hdr = ls_hdr it_entries = lt_entries ).

    TRY.
        zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer( lv_buffer ).
        cl_abap_unit_assert=>fail( 'expected zcx_abapgit_exception' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        cl_abap_unit_assert=>assert_char_cp(
          act = lx_error->get_text( ) exp = '*object_count*' ).
    ENDTRY.
  ENDMETHOD.

  METHOD reject_duplicate_entries.
    DATA(ls_hdr) = VALUE zaog_ser_dd_bhdr(
      wire_format_version = 1
      provider_id         = 'SER_DD01'
      object_count        = 2 ).
    DATA(lt_entries) = VALUE zaog_ser_dd_bentry_tt(
      ( obj_type = 'DOMA' obj_name = 'XFELD' present = abap_true )
      ( obj_type = 'DOMA' obj_name = 'XFELD' present = abap_true ) ).
    DATA(lv_buffer) = build_raw_buffer( is_hdr = ls_hdr it_entries = lt_entries ).

    TRY.
        zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer( lv_buffer ).
        cl_abap_unit_assert=>fail( 'expected zcx_abapgit_exception' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        cl_abap_unit_assert=>assert_char_cp(
          act = lx_error->get_text( ) exp = '*duplicate*' ).
    ENDTRY.
  ENDMETHOD.

  METHOD no_cross_batch_leakage.
    zcl_abapgit_ortec_ser_pref_ext=>prepare(
      it_tadir    = VALUE #( ( object = 'DOMA' obj_name = 'XFELD' ) )
      iv_language = 'E' ).
    DATA(lv_buffer_a) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch(
      VALUE #( ( object = 'DOMA' obj_name = 'XFELD' ) ) ).

    zcl_abapgit_ortec_ser_pref_ext=>prepare(
      it_tadir    = VALUE #( ( object = 'DOMA' obj_name = 'CHAR30' ) )
      iv_language = 'E' ).
    DATA(lv_buffer_b) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch(
      VALUE #( ( object = 'DOMA' obj_name = 'CHAR30' ) ) ).

    zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer( lv_buffer_a ).
    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref_ext=>get_doma_data( iv_domname = 'XFELD' iv_language = 'E' ) ).
    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_pref_ext=>get_doma_data( iv_domname = 'CHAR30' iv_language = 'E' ) ).

    zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer( lv_buffer_b ).
    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref_ext=>get_doma_data( iv_domname = 'CHAR30' iv_language = 'E' ) ).
    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_pref_ext=>get_doma_data( iv_domname = 'XFELD' iv_language = 'E' ) ).
  ENDMETHOD.

  METHOD doma_miss_when_not_prepared.
    zcl_abapgit_ortec_ser_pref_ext=>prepare(
      it_tadir    = VALUE #( ( object = 'DOMA' obj_name = 'XFELD' ) )
      iv_language = 'E' ).

    DATA(rv_found) = zcl_abapgit_ortec_ser_pref_ext=>get_doma_data(
      iv_domname = c_never_exists iv_language = 'E' ).

    cl_abap_unit_assert=>assert_false( rv_found ).
  ENDMETHOD.

  METHOD reject_corrupt_import.
    " SER-SLICE-3 correctness review DR-003: a genuinely truncated/
    " corrupt buffer (not merely a well-formed envelope with a bad
    " semantic value) must hit INJECT_BATCH_FROM_BUFFER's own
    " CATCH cx_root branch, not one of the semantic-validation checks.
    DATA(lv_buffer) = build_raw_buffer(
      is_hdr     = VALUE #( wire_format_version = 1 provider_id = 'SER_DD01' object_count = 0 )
      it_entries = VALUE #( ) ).
    " Truncate to a handful of leading bytes - IMPORT ... FROM DATA BUFFER
    " on a partial/malformed compressed stream must raise a catchable
    " exception, never silently succeed with empty/garbage data.
    DATA(lv_corrupt) = lv_buffer(3).

    TRY.
        zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer( lv_corrupt ).
        cl_abap_unit_assert=>fail( 'expected zcx_abapgit_exception for a corrupt buffer' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        cl_abap_unit_assert=>assert_char_cp(
          act = lx_error->get_text( ) exp = '*corrupt*' ).
    ENDTRY.
  ENDMETHOD.

  METHOD unexpected_entry_ignored.
    " SER-SLICE-3 correctness review DR-004: a buffer carrying MULTIPLE
    " objects must never let one entry's data leak into a lookup for a
    " DIFFERENT entry - correlation is strictly by (obj_type, obj_name),
    " never by row position.
    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( object = 'DOMA' obj_name = 'XFELD' )
      ( object = 'DOMA' obj_name = 'CHAR30' ) ).
    zcl_abapgit_ortec_ser_pref_ext=>prepare( it_tadir = lt_tadir iv_language = 'E' ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch( lt_tadir ).

    zcl_abapgit_ortec_ser_pref_ext=>clear( ).
    zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer( lv_buffer ).

    DATA es_dd01v_xfeld TYPE dd01v.
    zcl_abapgit_ortec_ser_pref_ext=>get_doma_data(
      EXPORTING iv_domname = 'XFELD' iv_language = 'E'
      IMPORTING es_dd01v = es_dd01v_xfeld ).
    cl_abap_unit_assert=>assert_equals( act = es_dd01v_xfeld-domname exp = 'XFELD' ).

    DATA es_dd01v_char30 TYPE dd01v.
    zcl_abapgit_ortec_ser_pref_ext=>get_doma_data(
      EXPORTING iv_domname = 'CHAR30' iv_language = 'E'
      IMPORTING es_dd01v = es_dd01v_char30 ).
    cl_abap_unit_assert=>assert_equals( act = es_dd01v_char30-domname exp = 'CHAR30' ).
  ENDMETHOD.

  METHOD empty_payload_is_hit.
    " SER-SLICE-3 correctness review DR-004: CHAR30 is a real, active
    " domain with NO fixed values - present=TRUE with an empty
    " ET_DD07V_TAB must still be a real HIT (RV_FOUND = TRUE), never
    " conflated with a MISS.
    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt( ( object = 'DOMA' obj_name = 'CHAR30' ) ).
    zcl_abapgit_ortec_ser_pref_ext=>prepare( it_tadir = lt_tadir iv_language = 'E' ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch( lt_tadir ).

    zcl_abapgit_ortec_ser_pref_ext=>clear( ).
    zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer( lv_buffer ).

    DATA et_dd07v TYPE dd07v_tab.
    DATA(rv_found) = zcl_abapgit_ortec_ser_pref_ext=>get_doma_data(
      EXPORTING iv_domname = 'CHAR30' iv_language = 'E'
      IMPORTING et_dd07v_tab = et_dd07v ).

    cl_abap_unit_assert=>assert_true( rv_found ).
    cl_abap_unit_assert=>assert_initial( et_dd07v ).
  ENDMETHOD.

ENDCLASS.
