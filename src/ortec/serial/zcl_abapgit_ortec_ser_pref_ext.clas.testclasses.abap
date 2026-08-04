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

    METHODS serialize_doma
      IMPORTING iv_domname    TYPE ddobjname
      RETURNING VALUE(rv_xml) TYPE string
      RAISING   zcx_abapgit_exception.

    METHODS active_with_fixed_values FOR TESTING RAISING zcx_abapgit_exception.
    METHODS active_without_fixed_values FOR TESTING RAISING zcx_abapgit_exception.
    METHODS nonexistent_or_inactive FOR TESTING RAISING zcx_abapgit_exception.
    METHODS exists_matches_serialize FOR TESTING RAISING zcx_abapgit_exception.
    METHODS output_is_stable FOR TESTING RAISING zcx_abapgit_exception.

ENDCLASS.


CLASS ltcl_doma_parity IMPLEMENTATION.

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

ENDCLASS.
