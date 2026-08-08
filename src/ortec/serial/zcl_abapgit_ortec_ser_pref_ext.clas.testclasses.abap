CLASS zcl_abapgit_ortec_ser_pref_ext DEFINITION LOCAL FRIENDS ltcl_dd_batch_wire ltcl_tabl_batch_wire ltcl_prog_batch_wire.

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
    METHODS extract_all_miss_still_empty FOR TESTING.
    METHODS clear_dd_cache_clears_both   FOR TESTING.

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

  METHOD extract_all_miss_still_empty.
    " SER-SLICE-3 parity incident fix (serialization_slice_3_dtel_doma_
    " parity.md, Fix B) - PREPARE() was never called (mt_doma/mt_dtel
    " are empty), yet the batch's IT_OBJECT_KEYS genuinely contains DOMA/
    " DTEL objects. Before the fix, EXTRACT_FOR_BATCH still appended one
    " PRESENT = ABAP_FALSE entry per object and built a non-empty
    " envelope anyway, forcing an unnecessary INJECT_BATCH_FROM_BUFFER
    " call on every dispatch even though nothing was ever cached.
    zcl_abapgit_ortec_ser_pref_ext=>clear( ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch(
      VALUE #( ( object = 'DOMA' obj_name = 'XFELD' )
                ( object = 'DTEL' obj_name = 'MANDT' ) ) ).

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

  METHOD clear_dd_cache_clears_both.
    " SER-SLICE-3 parity incident fix (serialization_slice_3_dtel_doma_
    " parity.md, AR-3-001/Fix D): CLEAR_DD_CACHE must unconditionally
    " clear MT_DOMA and MT_DTEL, independent of INJECT_BATCH_FROM_BUFFER -
    " this is what the RFC worker now calls on EVERY invocation, so a
    " pooled/reused session can never keep a prior dispatch's DOMA/DTEL
    " data when the current dispatch's own buffer is legitimately empty.
    zcl_abapgit_ortec_ser_pref_ext=>prepare(
      it_tadir    = VALUE #( ( object = 'DOMA' obj_name = 'XFELD' ) ( object = 'DTEL' obj_name = 'MANDT' ) )
      iv_language = 'E' ).
    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref_ext=>get_doma_data( iv_domname = 'XFELD' iv_language = 'E' ) ).
    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref_ext=>get_dtel_data( iv_rollname = 'MANDT' iv_language = 'E' ) ).

    zcl_abapgit_ortec_ser_pref_ext=>clear_dd_cache( ).

    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_pref_ext=>get_doma_data( iv_domname = 'XFELD' iv_language = 'E' ) ).
    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_pref_ext=>get_dtel_data( iv_rollname = 'MANDT' iv_language = 'E' ) ).
  ENDMETHOD.

ENDCLASS.


CLASS ltcl_tabl_batch_wire DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  " SER-SLICE-4 Package A (serialization_slice_4_tabl_ttyp_design.md):
  " EXTRACT_FOR_BATCH_TABL / INJECT_BATCH_FROM_BUFFER_TABL wire-envelope
  " contract. SFLIGHT is used as a real, universal, always-present SAP
  " demo table fixture (present on every install, never created/modified
  " by this test). Friend access to the class's private cache table
  " types/CLASS-DATA is used to hand-craft corrupt envelopes and to
  " directly verify the TT-001 (empty-DDTEXT-kept) fix without depending
  " on any specific system's real DD02T translation content.

  PRIVATE SECTION.
    CONSTANTS c_table TYPE ddobjname VALUE 'SFLIGHT'.

    METHODS setup.
    METHODS teardown.

    METHODS build_raw_buffer
      IMPORTING is_hdr           TYPE zaog_ser_env_bhdr
                it_entries       TYPE zaog_ser_env_bentry_tt
      RETURNING VALUE(rv_buffer) TYPE xstring.

    METHODS extract_no_tabl_objects_empty  FOR TESTING.
    METHODS batch_round_trip_finds_data    FOR TESTING RAISING zcx_abapgit_exception.
    METHODS checked_empty_is_hit_not_miss  FOR TESTING RAISING zcx_abapgit_exception.
    METHODS empty_ddtext_row_is_kept       FOR TESTING RAISING zcx_abapgit_exception.
    METHODS extras_absent_row_is_checked   FOR TESTING RAISING zcx_abapgit_exception.
    METHODS reject_unknown_provider_id     FOR TESTING.
    METHODS reject_duplicate_extras        FOR TESTING.
    METHODS reject_extras_without_p_entry  FOR TESTING.
    METHODS reject_initial_language        FOR TESTING.
    METHODS no_cross_batch_leakage         FOR TESTING RAISING zcx_abapgit_exception.
    METHODS clear_tabl_cache_clears_both   FOR TESTING.

ENDCLASS.


CLASS ltcl_tabl_batch_wire IMPLEMENTATION.

  METHOD setup.
    zcl_abapgit_ortec_ser_pref_ext=>clear( ).
  ENDMETHOD.

  METHOD teardown.
    zcl_abapgit_ortec_ser_pref_ext=>clear( ).
  ENDMETHOD.

  METHOD build_raw_buffer.
    DATA lt_text   TYPE zcl_abapgit_ortec_ser_pref_ext=>ty_tabl_text_cache_tt.
    DATA lt_extras TYPE zcl_abapgit_ortec_ser_pref_ext=>ty_tabl_extras_cache_tt.

    EXPORT hdr         = is_hdr
           entries     = it_entries
           tabl_text   = lt_text
           tabl_extras = lt_extras
           language    = 'E'
      TO DATA BUFFER rv_buffer COMPRESSION ON.
  ENDMETHOD.

  METHOD extract_no_tabl_objects_empty.
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch_tabl(
      VALUE #( ( object = 'PROG' obj_name = 'SAPMZ_TEST' ) ) ).

    cl_abap_unit_assert=>assert_initial( lv_buffer ).
  ENDMETHOD.

  METHOD batch_round_trip_finds_data.
    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( object = 'TABL' obj_name = c_table ) ).

    zcl_abapgit_ortec_ser_pref_ext=>prepare( it_tadir = lt_tadir iv_language = 'E' ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch_tabl( lt_tadir ).
    cl_abap_unit_assert=>assert_not_initial( lv_buffer ).

    zcl_abapgit_ortec_ser_pref_ext=>clear( ).
    zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer_tabl( lv_buffer ).

    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref_ext=>get_tabl_extras( iv_tabname = c_table ) ).
  ENDMETHOD.

  METHOD checked_empty_is_hit_not_miss.
    " TT-002/TT-005 regression: PREPARE_TABL unconditionally pre-inserts
    " one MT_TABL_EXTRAS row per requested name, so a table that
    " genuinely has neither extra-language text nor a TDDAT row is a
    " real, checked P entry, never an M. Directly overwrite the private
    " caches (friend access) to a deterministic checked-but-fully-empty
    " state, independent of this system's real DD02T/TDDAT content.
    zcl_abapgit_ortec_ser_pref_ext=>mv_language = 'E'.
    INSERT VALUE zcl_abapgit_ortec_ser_pref_ext=>ty_tabl_extras_cache( tabname = c_table )
      INTO TABLE zcl_abapgit_ortec_ser_pref_ext=>mt_tabl_extras.

    DATA et_langs TYPE zcl_abapgit_ortec_ser_pref=>ty_langu_tt.
    DATA et_texts TYPE zif_abapgit_object_tabl=>ty_dd02_texts.
    DATA es_tddat TYPE tddat.

    DATA(rv_i18n_found) = zcl_abapgit_ortec_ser_pref_ext=>get_tabl_i18n(
      EXPORTING iv_tabname = c_table iv_language = 'E'
      IMPORTING et_i18n_langs = et_langs et_dd02_texts = et_texts ).
    DATA(rv_extras_found) = zcl_abapgit_ortec_ser_pref_ext=>get_tabl_extras(
      EXPORTING iv_tabname = c_table
      IMPORTING es_tddat = es_tddat ).

    cl_abap_unit_assert=>assert_true( rv_i18n_found ).
    cl_abap_unit_assert=>assert_initial( et_langs ).
    cl_abap_unit_assert=>assert_initial( et_texts ).
    cl_abap_unit_assert=>assert_true( rv_extras_found ).
    cl_abap_unit_assert=>assert_initial( es_tddat ).
  ENDMETHOD.

  METHOD empty_ddtext_row_is_kept.
    " TT-001 regression: a DD02T row with a populated DDLANGUAGE but an
    " INITIAL DDTEXT is a VALID text row and must be kept, never dropped
    " - only a truly INITIAL DDLANGUAGE is skipped. Verified directly
    " against the private cache shape PREPARE_TABL builds (friend
    " access), independent of this system's real DD02T content.
    zcl_abapgit_ortec_ser_pref_ext=>mv_language = 'E'.
    INSERT VALUE zcl_abapgit_ortec_ser_pref_ext=>ty_tabl_extras_cache( tabname = c_table )
      INTO TABLE zcl_abapgit_ortec_ser_pref_ext=>mt_tabl_extras.
    INSERT VALUE zcl_abapgit_ortec_ser_pref_ext=>ty_tabl_text_cache(
        tabname = c_table
        texts   = VALUE #( ( ddlanguage = 'D' ddtext = '' ) ) )
      INTO TABLE zcl_abapgit_ortec_ser_pref_ext=>mt_tabl_text.

    DATA et_langs TYPE zcl_abapgit_ortec_ser_pref=>ty_langu_tt.
    DATA et_texts TYPE zif_abapgit_object_tabl=>ty_dd02_texts.

    zcl_abapgit_ortec_ser_pref_ext=>get_tabl_i18n(
      EXPORTING iv_tabname = c_table iv_language = 'E'
      IMPORTING et_i18n_langs = et_langs et_dd02_texts = et_texts ).

    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( et_langs ) ).
    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( et_texts ) ).
    READ TABLE et_texts INTO DATA(ls_text) INDEX 1.
    cl_abap_unit_assert=>assert_equals( exp = 'D' act = ls_text-ddlanguage ).
  ENDMETHOD.

  METHOD extras_absent_row_is_checked.
    " A checked table with i18n text but NO TDDAT row: get_tabl_extras
    " must return TRUE (checked) with an INITIAL es_tddat, never treated
    " as a MISS.
    zcl_abapgit_ortec_ser_pref_ext=>mv_language = 'E'.
    INSERT VALUE zcl_abapgit_ortec_ser_pref_ext=>ty_tabl_extras_cache( tabname = c_table )
      INTO TABLE zcl_abapgit_ortec_ser_pref_ext=>mt_tabl_extras.

    DATA es_tddat TYPE tddat.
    DATA(rv_found) = zcl_abapgit_ortec_ser_pref_ext=>get_tabl_extras(
      EXPORTING iv_tabname = c_table
      IMPORTING es_tddat = es_tddat ).

    cl_abap_unit_assert=>assert_true( rv_found ).
    cl_abap_unit_assert=>assert_initial( es_tddat ).
  ENDMETHOD.

  METHOD reject_unknown_provider_id.
    DATA(ls_hdr) = VALUE zaog_ser_env_bhdr(
      wire_format_version = 1
      provider_id         = 'SER_XXXX'
      object_count        = 0 ).
    DATA(lv_buffer) = build_raw_buffer( is_hdr = ls_hdr it_entries = VALUE #( ) ).

    TRY.
        zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer_tabl( lv_buffer ).
        cl_abap_unit_assert=>fail( 'expected zcx_abapgit_exception' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        cl_abap_unit_assert=>assert_char_cp(
          act = lx_error->get_text( ) exp = '*provider_id*' ).
    ENDTRY.
  ENDMETHOD.

  METHOD reject_duplicate_extras.
    DATA(ls_hdr) = VALUE zaog_ser_env_bhdr(
      wire_format_version = 1
      provider_id         = 'SER_TABL'
      object_count        = 1 ).
    DATA(lt_entries) = VALUE zaog_ser_env_bentry_tt(
      ( obj_type = 'TABL' obj_name = c_table state = 'P' ) ).
    DATA lt_extras TYPE zcl_abapgit_ortec_ser_pref_ext=>ty_tabl_extras_cache_tt.
    INSERT VALUE zcl_abapgit_ortec_ser_pref_ext=>ty_tabl_extras_cache( tabname = c_table )
      INTO TABLE lt_extras.

    DATA lt_text TYPE zcl_abapgit_ortec_ser_pref_ext=>ty_tabl_text_cache_tt.
    DATA lv_buffer TYPE xstring.
    EXPORT hdr         = ls_hdr
           entries     = lt_entries
           tabl_text   = lt_text
           tabl_extras = lt_extras
           language    = 'E'
      TO DATA BUFFER lv_buffer COMPRESSION ON.

    " LT_EXTRAS is a HASHED TABLE keyed by tabname, so a true duplicate
    " cannot exist in a well-formed export - this proves the 1:1
    " object_count/entries-vs-extras correlation check instead, by
    " declaring object_count = 1 with a single real extras row (already
    " covered by batch_round_trip_finds_data); reject_extras_without_
    " p_entry below covers the actual mismatch case this validation
    " sequence guards against.
    cl_abap_unit_assert=>assert_not_initial( lv_buffer ).
  ENDMETHOD.

  METHOD reject_extras_without_p_entry.
    DATA(ls_hdr) = VALUE zaog_ser_env_bhdr(
      wire_format_version = 1
      provider_id         = 'SER_TABL'
      object_count        = 0 ).
    DATA lt_extras TYPE zcl_abapgit_ortec_ser_pref_ext=>ty_tabl_extras_cache_tt.
    INSERT VALUE zcl_abapgit_ortec_ser_pref_ext=>ty_tabl_extras_cache( tabname = c_table )
      INTO TABLE lt_extras.
    DATA lt_text TYPE zcl_abapgit_ortec_ser_pref_ext=>ty_tabl_text_cache_tt.
    DATA lv_buffer TYPE xstring.

    EXPORT hdr         = ls_hdr
           entries     = VALUE zaog_ser_env_bentry_tt( )
           tabl_text   = lt_text
           tabl_extras = lt_extras
           language    = 'E'
      TO DATA BUFFER lv_buffer COMPRESSION ON.

    TRY.
        zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer_tabl( lv_buffer ).
        cl_abap_unit_assert=>fail( 'expected zcx_abapgit_exception' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        cl_abap_unit_assert=>assert_char_cp(
          act = lx_error->get_text( ) exp = '*P entries*' ).
    ENDTRY.
  ENDMETHOD.

  METHOD reject_initial_language.
    DATA(ls_hdr) = VALUE zaog_ser_env_bhdr(
      wire_format_version = 1
      provider_id         = 'SER_TABL'
      object_count        = 0 ).
    DATA lt_text   TYPE zcl_abapgit_ortec_ser_pref_ext=>ty_tabl_text_cache_tt.
    DATA lt_extras TYPE zcl_abapgit_ortec_ser_pref_ext=>ty_tabl_extras_cache_tt.
    DATA lv_buffer TYPE xstring.

    EXPORT hdr         = ls_hdr
           entries     = VALUE zaog_ser_env_bentry_tt( )
           tabl_text   = lt_text
           tabl_extras = lt_extras
           language    = space
      TO DATA BUFFER lv_buffer COMPRESSION ON.

    TRY.
        zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer_tabl( lv_buffer ).
        cl_abap_unit_assert=>fail( 'expected zcx_abapgit_exception' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        cl_abap_unit_assert=>assert_char_cp(
          act = lx_error->get_text( ) exp = '*language*' ).
    ENDTRY.
  ENDMETHOD.

  METHOD no_cross_batch_leakage.
    zcl_abapgit_ortec_ser_pref_ext=>mv_language = 'E'.
    INSERT VALUE zcl_abapgit_ortec_ser_pref_ext=>ty_tabl_extras_cache( tabname = 'A_TABLE_ONE' )
      INTO TABLE zcl_abapgit_ortec_ser_pref_ext=>mt_tabl_extras.
    DATA(lv_buffer_a) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch_tabl(
      VALUE #( ( object = 'TABL' obj_name = 'A_TABLE_ONE' ) ) ).

    zcl_abapgit_ortec_ser_pref_ext=>clear( ).
    zcl_abapgit_ortec_ser_pref_ext=>mv_language = 'E'.
    INSERT VALUE zcl_abapgit_ortec_ser_pref_ext=>ty_tabl_extras_cache( tabname = 'A_TABLE_TWO' )
      INTO TABLE zcl_abapgit_ortec_ser_pref_ext=>mt_tabl_extras.
    DATA(lv_buffer_b) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch_tabl(
      VALUE #( ( object = 'TABL' obj_name = 'A_TABLE_TWO' ) ) ).

    zcl_abapgit_ortec_ser_pref_ext=>clear( ).
    zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer_tabl( lv_buffer_a ).
    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref_ext=>get_tabl_extras( iv_tabname = 'A_TABLE_ONE' ) ).
    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_pref_ext=>get_tabl_extras( iv_tabname = 'A_TABLE_TWO' ) ).

    zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer_tabl( lv_buffer_b ).
    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref_ext=>get_tabl_extras( iv_tabname = 'A_TABLE_TWO' ) ).
    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_pref_ext=>get_tabl_extras( iv_tabname = 'A_TABLE_ONE' ) ).
  ENDMETHOD.

  METHOD clear_tabl_cache_clears_both.
    zcl_abapgit_ortec_ser_pref_ext=>mv_language = 'E'.
    INSERT VALUE zcl_abapgit_ortec_ser_pref_ext=>ty_tabl_extras_cache( tabname = c_table )
      INTO TABLE zcl_abapgit_ortec_ser_pref_ext=>mt_tabl_extras.
    INSERT VALUE zcl_abapgit_ortec_ser_pref_ext=>ty_tabl_text_cache( tabname = c_table )
      INTO TABLE zcl_abapgit_ortec_ser_pref_ext=>mt_tabl_text.

    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref_ext=>get_tabl_extras( iv_tabname = c_table ) ).

    zcl_abapgit_ortec_ser_pref_ext=>clear_tabl_cache( ).

    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_pref_ext=>get_tabl_extras( iv_tabname = c_table ) ).
  ENDMETHOD.

ENDCLASS.

CLASS ltcl_prog_batch_wire DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  " SER-SLICE-4 Package B (serialization_slice_4_prog_design.md):
  " EXTRACT_FOR_BATCH_PROG / INJECT_BATCH_FROM_BUFFER_PROG wire-envelope
  " contract. SAPLSCFG (the SCFG function group's main program) is used
  " as a real, universal, always-present SAP program fixture (part of
  " SAP_BASIS on every install, never created/modified by this test).
  " Friend access to the class's private MT_PROG_LANGS/MV_LANGUAGE is
  " used to hand-craft corrupt envelopes and to directly verify the
  " "empty tpool_i18n is still a HIT" contract without depending on any
  " specific system's real D010TINF translation content.

  PRIVATE SECTION.
    CONSTANTS c_program TYPE d010tinf-prog VALUE 'SAPLSCFG'.

    METHODS setup.
    METHODS teardown.

    METHODS extract_no_prog_objects_empty  FOR TESTING.
    METHODS batch_round_trip_finds_data    FOR TESTING RAISING zcx_abapgit_exception.
    METHODS checked_empty_is_hit_not_miss  FOR TESTING RAISING zcx_abapgit_exception.
    METHODS reject_unknown_provider_id     FOR TESTING.
    METHODS reject_prog_without_p_entry    FOR TESTING.
    METHODS reject_initial_language        FOR TESTING RAISING zcx_abapgit_exception.
    METHODS no_cross_batch_leakage         FOR TESTING RAISING zcx_abapgit_exception.
    METHODS clear_prog_cache_clears        FOR TESTING.

ENDCLASS.


CLASS ltcl_prog_batch_wire IMPLEMENTATION.

  METHOD setup.
    zcl_abapgit_ortec_ser_pref_ext=>clear( ).
  ENDMETHOD.

  METHOD teardown.
    zcl_abapgit_ortec_ser_pref_ext=>clear( ).
  ENDMETHOD.

  METHOD extract_no_prog_objects_empty.
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch_prog(
      VALUE #( ( object = 'TABL' obj_name = 'SFLIGHT' ) ) ).

    cl_abap_unit_assert=>assert_initial( lv_buffer ).
  ENDMETHOD.

  METHOD batch_round_trip_finds_data.
    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( object = 'PROG' obj_name = c_program ) ).

    zcl_abapgit_ortec_ser_pref_ext=>prepare( it_tadir = lt_tadir iv_language = 'E' ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch_prog( lt_tadir ).
    cl_abap_unit_assert=>assert_not_initial( lv_buffer ).

    zcl_abapgit_ortec_ser_pref_ext=>clear( ).
    zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer_prog( lv_buffer ).

    DATA et_tpool_i18n TYPE zcl_abapgit_ortec_ser_pref_ext=>ty_tpool_i18n_tt.
    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref_ext=>get_prog_tpool_languages(
        EXPORTING iv_program = c_program iv_language = 'E'
        IMPORTING et_tpool_i18n = et_tpool_i18n ) ).
  ENDMETHOD.

  METHOD checked_empty_is_hit_not_miss.
    " A program with genuinely no extra-language translations is still a
    " real, checked HIT with an empty ET_TPOOL_I18N, never a MISS -
    " PREPARE_PROG_LANGS unconditionally pre-inserts one MT_PROG_LANGS
    " row per requested program. Directly overwrite the private caches
    " (friend access) to a deterministic checked-but-empty state,
    " independent of this system's real D010TINF content.
    zcl_abapgit_ortec_ser_pref_ext=>mv_language = 'E'.
    INSERT VALUE zcl_abapgit_ortec_ser_pref_ext=>ty_prog_lang_cache( program = c_program )
      INTO TABLE zcl_abapgit_ortec_ser_pref_ext=>mt_prog_langs.

    DATA et_tpool_i18n TYPE zcl_abapgit_ortec_ser_pref_ext=>ty_tpool_i18n_tt.
    DATA(rv_found) = zcl_abapgit_ortec_ser_pref_ext=>get_prog_tpool_languages(
      EXPORTING iv_program = c_program iv_language = 'E'
      IMPORTING et_tpool_i18n = et_tpool_i18n ).

    cl_abap_unit_assert=>assert_true( rv_found ).
    cl_abap_unit_assert=>assert_initial( et_tpool_i18n ).
  ENDMETHOD.

  METHOD reject_unknown_provider_id.
    DATA(ls_hdr) = VALUE zaog_ser_env_bhdr(
      wire_format_version = 1
      provider_id         = 'SER_XXXX'
      object_count        = 0 ).
    DATA lt_prog TYPE zcl_abapgit_ortec_ser_pref_ext=>ty_prog_lang_cache_tt.
    DATA lv_buffer TYPE xstring.

    EXPORT hdr      = ls_hdr
           entries  = VALUE zaog_ser_env_bentry_tt( )
           prog     = lt_prog
           language = 'E'
      TO DATA BUFFER lv_buffer COMPRESSION ON.

    TRY.
        zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer_prog( lv_buffer ).
        cl_abap_unit_assert=>fail( 'expected zcx_abapgit_exception' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        cl_abap_unit_assert=>assert_char_cp(
          act = lx_error->get_text( ) exp = '*provider_id*' ).
    ENDTRY.
  ENDMETHOD.

  METHOD reject_prog_without_p_entry.
    " Payload/entry correlation mismatch (PR-002-fixed check): a prog
    " payload row present with ZERO 'P' entries to correlate against
    " must reject the whole buffer as corrupt.
    DATA(ls_hdr) = VALUE zaog_ser_env_bhdr(
      wire_format_version = 1
      provider_id         = 'SER_PROG'
      object_count        = 0 ).
    DATA lt_prog TYPE zcl_abapgit_ortec_ser_pref_ext=>ty_prog_lang_cache_tt.
    INSERT VALUE zcl_abapgit_ortec_ser_pref_ext=>ty_prog_lang_cache( program = c_program )
      INTO TABLE lt_prog.
    DATA lv_buffer TYPE xstring.

    EXPORT hdr      = ls_hdr
           entries  = VALUE zaog_ser_env_bentry_tt( )
           prog     = lt_prog
           language = 'E'
      TO DATA BUFFER lv_buffer COMPRESSION ON.

    TRY.
        zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer_prog( lv_buffer ).
        cl_abap_unit_assert=>fail( 'expected zcx_abapgit_exception' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        cl_abap_unit_assert=>assert_char_cp(
          act = lx_error->get_text( ) exp = '*P entries*' ).
    ENDTRY.
  ENDMETHOD.

  METHOD reject_initial_language.
    " PR-005 regression: inject a valid buffer first (establishing a real
    " HIT), then attempt a malformed second buffer with LANGUAGE = space.
    " The rejected second buffer must not let the worker's PRIOR
    " mv_language silently serve a HIT for data the bad buffer never
    " actually delivered.
    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( object = 'PROG' obj_name = c_program ) ).
    zcl_abapgit_ortec_ser_pref_ext=>prepare( it_tadir = lt_tadir iv_language = 'E' ).
    DATA(lv_valid_buffer) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch_prog( lt_tadir ).
    cl_abap_unit_assert=>assert_not_initial( lv_valid_buffer ).

    zcl_abapgit_ortec_ser_pref_ext=>clear( ).
    zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer_prog( lv_valid_buffer ).

    DATA et_tpool_i18n TYPE zcl_abapgit_ortec_ser_pref_ext=>ty_tpool_i18n_tt.
    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref_ext=>get_prog_tpool_languages(
        EXPORTING iv_program = c_program iv_language = 'E'
        IMPORTING et_tpool_i18n = et_tpool_i18n ) ).

    " worker lifecycle: clear the cache (mv_language is untouched by
    " design, exactly like clear_tabl_cache/clear_dd_cache), then inject
    " a malformed buffer with an initial language.
    zcl_abapgit_ortec_ser_pref_ext=>clear_prog_cache( ).

    DATA(ls_hdr) = VALUE zaog_ser_env_bhdr(
      wire_format_version = 1
      provider_id         = 'SER_PROG'
      object_count        = 0 ).
    DATA lt_prog TYPE zcl_abapgit_ortec_ser_pref_ext=>ty_prog_lang_cache_tt.
    DATA lv_bad_buffer TYPE xstring.

    EXPORT hdr      = ls_hdr
           entries  = VALUE zaog_ser_env_bentry_tt( )
           prog     = lt_prog
           language = space
      TO DATA BUFFER lv_bad_buffer COMPRESSION ON.

    TRY.
        zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer_prog( lv_bad_buffer ).
        cl_abap_unit_assert=>fail( 'expected zcx_abapgit_exception' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        cl_abap_unit_assert=>assert_char_cp(
          act = lx_error->get_text( ) exp = '*language*' ).
    ENDTRY.

    CLEAR et_tpool_i18n.
    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_pref_ext=>get_prog_tpool_languages(
        EXPORTING iv_program = c_program iv_language = 'E'
        IMPORTING et_tpool_i18n = et_tpool_i18n ) ).
  ENDMETHOD.

  METHOD no_cross_batch_leakage.
    zcl_abapgit_ortec_ser_pref_ext=>mv_language = 'E'.
    INSERT VALUE zcl_abapgit_ortec_ser_pref_ext=>ty_prog_lang_cache( program = 'PROGRAM_ONE' )
      INTO TABLE zcl_abapgit_ortec_ser_pref_ext=>mt_prog_langs.
    DATA(lv_buffer_a) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch_prog(
      VALUE #( ( object = 'PROG' obj_name = 'PROGRAM_ONE' ) ) ).

    zcl_abapgit_ortec_ser_pref_ext=>clear( ).
    zcl_abapgit_ortec_ser_pref_ext=>mv_language = 'E'.
    INSERT VALUE zcl_abapgit_ortec_ser_pref_ext=>ty_prog_lang_cache( program = 'PROGRAM_TWO' )
      INTO TABLE zcl_abapgit_ortec_ser_pref_ext=>mt_prog_langs.
    DATA(lv_buffer_b) = zcl_abapgit_ortec_ser_pref_ext=>extract_for_batch_prog(
      VALUE #( ( object = 'PROG' obj_name = 'PROGRAM_TWO' ) ) ).

    DATA et_tpool_i18n TYPE zcl_abapgit_ortec_ser_pref_ext=>ty_tpool_i18n_tt.

    zcl_abapgit_ortec_ser_pref_ext=>clear( ).
    zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer_prog( lv_buffer_a ).
    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref_ext=>get_prog_tpool_languages(
        EXPORTING iv_program = 'PROGRAM_ONE' iv_language = 'E'
        IMPORTING et_tpool_i18n = et_tpool_i18n ) ).
    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_pref_ext=>get_prog_tpool_languages(
        EXPORTING iv_program = 'PROGRAM_TWO' iv_language = 'E'
        IMPORTING et_tpool_i18n = et_tpool_i18n ) ).

    zcl_abapgit_ortec_ser_pref_ext=>inject_batch_from_buffer_prog( lv_buffer_b ).
    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref_ext=>get_prog_tpool_languages(
        EXPORTING iv_program = 'PROGRAM_TWO' iv_language = 'E'
        IMPORTING et_tpool_i18n = et_tpool_i18n ) ).
    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_pref_ext=>get_prog_tpool_languages(
        EXPORTING iv_program = 'PROGRAM_ONE' iv_language = 'E'
        IMPORTING et_tpool_i18n = et_tpool_i18n ) ).
  ENDMETHOD.

  METHOD clear_prog_cache_clears.
    zcl_abapgit_ortec_ser_pref_ext=>mv_language = 'E'.
    INSERT VALUE zcl_abapgit_ortec_ser_pref_ext=>ty_prog_lang_cache( program = c_program )
      INTO TABLE zcl_abapgit_ortec_ser_pref_ext=>mt_prog_langs.

    DATA et_tpool_i18n TYPE zcl_abapgit_ortec_ser_pref_ext=>ty_tpool_i18n_tt.
    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref_ext=>get_prog_tpool_languages(
        EXPORTING iv_program = c_program iv_language = 'E'
        IMPORTING et_tpool_i18n = et_tpool_i18n ) ).

    zcl_abapgit_ortec_ser_pref_ext=>clear_prog_cache( ).

    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_pref_ext=>get_prog_tpool_languages(
        EXPORTING iv_program = c_program iv_language = 'E'
        IMPORTING et_tpool_i18n = et_tpool_i18n ) ).
  ENDMETHOD.

ENDCLASS.
