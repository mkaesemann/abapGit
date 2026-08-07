CLASS zcl_abapgit_ortec_ser_pref_oo DEFINITION LOCAL FRIENDS ltcl_oo_batch_wire.

CLASS ltcl_oo_batch_wire DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  " SER-SLICE-3 Phase 4 (serialization_slice_3_clas_intf.md):
  " EXTRACT_FOR_BATCH / INJECT_BATCH_FROM_BUFFER wire-envelope contract for
  " the CLAS/INTF provider, mirroring ZCL_ABAPGIT_ORTEC_SER_PREF_EXT's
  " LTCL_DD_BATCH_WIRE test class exactly. No live CLAS/INTF objects are
  " read from the database - fixtures are seeded directly into the
  " private MT_CLASSTX/MT_COMPOTX/MT_SUBCOTX caches via friend access,
  " with MV_LANGUAGE/MV_PREPARED set the same way PREPARE() itself would
  " leave them, so every test is deterministic and independent of live
  " SAP Basis DDIC content.

  PRIVATE SECTION.
    METHODS setup.
    METHODS teardown.

    METHODS seed_classtx
      IMPORTING iv_clsname TYPE seoclsname
                iv_langu   TYPE spras DEFAULT 'D'
                iv_descr   TYPE string DEFAULT 'Translated class description'.
    METHODS seed_compotx
      IMPORTING iv_clsname TYPE seoclsname
                iv_cmpname TYPE seocmpname DEFAULT 'METHOD1'
                iv_langu   TYPE spras DEFAULT 'E'
                iv_descr   TYPE string DEFAULT 'Component description'.
    METHODS seed_subcotx
      IMPORTING iv_clsname TYPE seoclsname
                iv_cmpname TYPE seocmpname DEFAULT 'METHOD1'
                iv_sconame TYPE seocmpname DEFAULT 'PARAM1'
                iv_langu   TYPE spras DEFAULT 'E'
                iv_descr   TYPE string DEFAULT 'Subcomponent description'.

    METHODS build_raw_buffer
      IMPORTING is_hdr           TYPE zaog_ser_env_bhdr
                it_entries       TYPE zaog_ser_env_bentry_tt
      RETURNING VALUE(rv_buffer) TYPE xstring.
    METHODS read_envelope
      IMPORTING iv_buffer  TYPE xstring
      EXPORTING es_hdr     TYPE zaog_ser_env_bhdr
                et_entries TYPE zaog_ser_env_bentry_tt.

    METHODS small_class_classtx_only        FOR TESTING RAISING zcx_abapgit_exception.
    METHODS large_class_many_rows           FOR TESTING RAISING zcx_abapgit_exception.
    METHODS interface_object_type           FOR TESTING RAISING zcx_abapgit_exception.
    METHODS compo_only_is_hit               FOR TESTING RAISING zcx_abapgit_exception.
    METHODS subco_only_is_hit               FOR TESTING RAISING zcx_abapgit_exception.
    METHODS both_compo_subco_is_hit         FOR TESTING RAISING zcx_abapgit_exception.
    METHODS missing_optional_still_hit      FOR TESTING RAISING zcx_abapgit_exception.
    METHODS multiple_languages_round_trip   FOR TESTING RAISING zcx_abapgit_exception.
    METHODS namespaced_name_round_trip      FOR TESTING RAISING zcx_abapgit_exception.
    METHODS mixed_clas_intf_batch           FOR TESTING RAISING zcx_abapgit_exception.
    METHODS all_miss_batch_is_initial       FOR TESTING.
    METHODS neither_present_is_miss         FOR TESTING RAISING zcx_abapgit_exception.
    METHODS partial_data_no_cross_contam    FOR TESTING RAISING zcx_abapgit_exception.
    METHODS reject_unknown_version          FOR TESTING.
    METHODS reject_duplicate_entries        FOR TESTING.
    METHODS reject_count_mismatch           FOR TESTING.
    METHODS reject_corrupt_import           FOR TESTING.
    METHODS cross_batch_isolation           FOR TESTING RAISING zcx_abapgit_exception.
    METHODS full_round_trip_byte_ident      FOR TESTING RAISING zcx_abapgit_exception.
    METHODS extract_no_oo_objects_empty     FOR TESTING.
    METHODS extract_not_prepared_empty      FOR TESTING.

ENDCLASS.


CLASS ltcl_oo_batch_wire IMPLEMENTATION.

  METHOD setup.
    zcl_abapgit_ortec_ser_pref_oo=>clear( ).
  ENDMETHOD.

  METHOD teardown.
    zcl_abapgit_ortec_ser_pref_oo=>clear( ).
  ENDMETHOD.

  METHOD seed_classtx.
    DATA ls_row TYPE seoclasstx.
    ls_row-langu    = iv_langu.
    ls_row-descript = iv_descr.

    IF NOT line_exists( zcl_abapgit_ortec_ser_pref_oo=>mt_classtx[ clsname = iv_clsname ] ).
      INSERT VALUE #( clsname = iv_clsname ) INTO TABLE zcl_abapgit_ortec_ser_pref_oo=>mt_classtx.
    ENDIF.
    READ TABLE zcl_abapgit_ortec_ser_pref_oo=>mt_classtx ASSIGNING FIELD-SYMBOL(<ls_cache>)
      WITH TABLE KEY clsname = iv_clsname.
    APPEND ls_row TO <ls_cache>-descriptions.
  ENDMETHOD.

  METHOD seed_compotx.
    DATA ls_row TYPE seocompotx.
    ls_row-cmpname  = iv_cmpname.
    ls_row-langu    = iv_langu.
    ls_row-descript = iv_descr.

    IF NOT line_exists( zcl_abapgit_ortec_ser_pref_oo=>mt_compotx[ clsname = iv_clsname ] ).
      INSERT VALUE #( clsname = iv_clsname ) INTO TABLE zcl_abapgit_ortec_ser_pref_oo=>mt_compotx.
    ENDIF.
    READ TABLE zcl_abapgit_ortec_ser_pref_oo=>mt_compotx ASSIGNING FIELD-SYMBOL(<ls_cache>)
      WITH TABLE KEY clsname = iv_clsname.
    APPEND ls_row TO <ls_cache>-descriptions.
  ENDMETHOD.

  METHOD seed_subcotx.
    DATA ls_row TYPE seosubcotx.
    ls_row-cmpname  = iv_cmpname.
    ls_row-sconame  = iv_sconame.
    ls_row-langu    = iv_langu.
    ls_row-descript = iv_descr.

    IF NOT line_exists( zcl_abapgit_ortec_ser_pref_oo=>mt_subcotx[ clsname = iv_clsname ] ).
      INSERT VALUE #( clsname = iv_clsname ) INTO TABLE zcl_abapgit_ortec_ser_pref_oo=>mt_subcotx.
    ENDIF.
    READ TABLE zcl_abapgit_ortec_ser_pref_oo=>mt_subcotx ASSIGNING FIELD-SYMBOL(<ls_cache>)
      WITH TABLE KEY clsname = iv_clsname.
    APPEND ls_row TO <ls_cache>-descriptions.
  ENDMETHOD.

  METHOD build_raw_buffer.
    DATA lt_classtx TYPE zcl_abapgit_ortec_ser_pref_oo=>ty_classtx_cache_tt.
    DATA lt_compotx TYPE zcl_abapgit_ortec_ser_pref_oo=>ty_compotx_cache_tt.
    DATA lt_subcotx TYPE zcl_abapgit_ortec_ser_pref_oo=>ty_subcotx_cache_tt.

    EXPORT hdr      = is_hdr
           entries  = it_entries
           classtx  = lt_classtx
           compotx  = lt_compotx
           subcotx  = lt_subcotx
           language = 'E'
      TO DATA BUFFER rv_buffer COMPRESSION ON.
  ENDMETHOD.

  METHOD read_envelope.
    DATA lt_classtx TYPE zcl_abapgit_ortec_ser_pref_oo=>ty_classtx_cache_tt.
    DATA lt_compotx TYPE zcl_abapgit_ortec_ser_pref_oo=>ty_compotx_cache_tt.
    DATA lt_subcotx TYPE zcl_abapgit_ortec_ser_pref_oo=>ty_subcotx_cache_tt.
    DATA lv_language TYPE spras.

    IMPORT hdr      = es_hdr
           entries  = et_entries
           classtx  = lt_classtx
           compotx  = lt_compotx
           subcotx  = lt_subcotx
           language = lv_language
      FROM DATA BUFFER iv_buffer.
  ENDMETHOD.

  METHOD small_class_classtx_only.
    zcl_abapgit_ortec_ser_pref_oo=>mv_language = 'E'.
    zcl_abapgit_ortec_ser_pref_oo=>mv_prepared = abap_true.
    seed_classtx( iv_clsname = 'ZCL_SMALL' iv_langu = 'D' iv_descr = 'Kleine Klasse' ).

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt( ( object = 'CLAS' obj_name = 'ZCL_SMALL' ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_oo=>extract_for_batch( lt_tadir ).
    cl_abap_unit_assert=>assert_not_initial( lv_buffer ).

    zcl_abapgit_ortec_ser_pref_oo=>clear( ).
    zcl_abapgit_ortec_ser_pref_oo=>inject_batch_from_buffer( lv_buffer ).

    DATA et_desc TYPE zif_abapgit_oo_object_fnc=>ty_seoclasstx_tt.
    DATA(rv_found) = zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_class(
      EXPORTING iv_clsname = 'ZCL_SMALL' iv_language = 'E'
      IMPORTING et_descriptions = et_desc ).

    cl_abap_unit_assert=>assert_true( rv_found ).
    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( et_desc ) ).
    READ TABLE et_desc INDEX 1 INTO DATA(ls_desc).
    cl_abap_unit_assert=>assert_equals( exp = 'Kleine Klasse' act = ls_desc-descript ).
  ENDMETHOD.

  METHOD large_class_many_rows.
    zcl_abapgit_ortec_ser_pref_oo=>mv_language = 'E'.
    zcl_abapgit_ortec_ser_pref_oo=>mv_prepared = abap_true.

    DO 10 TIMES.
      DATA(lv_method) = |METHOD{ sy-index }|.
      seed_compotx( iv_clsname = 'ZCL_LARGE' iv_cmpname = CONV #( lv_method ) iv_langu = 'E'
                    iv_descr = |Method { sy-index }| ).
      seed_subcotx( iv_clsname = 'ZCL_LARGE' iv_cmpname = CONV #( lv_method ) iv_sconame = 'PARAM1'
                    iv_langu = 'E' iv_descr = |Param of { sy-index }| ).
    ENDDO.

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt( ( object = 'CLAS' obj_name = 'ZCL_LARGE' ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_oo=>extract_for_batch( lt_tadir ).
    cl_abap_unit_assert=>assert_not_initial( lv_buffer ).

    zcl_abapgit_ortec_ser_pref_oo=>clear( ).
    zcl_abapgit_ortec_ser_pref_oo=>inject_batch_from_buffer( lv_buffer ).

    DATA et_compo TYPE zif_abapgit_oo_object_fnc=>ty_seocompotx_tt.
    DATA et_subco TYPE zif_abapgit_oo_object_fnc=>ty_seosubcotx_tt.
    zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_compo(
      EXPORTING iv_clsname = 'ZCL_LARGE' iv_language = 'E'
      IMPORTING et_descriptions = et_compo ).
    zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_subco(
      EXPORTING iv_clsname = 'ZCL_LARGE' iv_language = 'E'
      IMPORTING et_descriptions = et_subco ).

    cl_abap_unit_assert=>assert_equals( exp = 10 act = lines( et_compo ) ).
    cl_abap_unit_assert=>assert_equals( exp = 10 act = lines( et_subco ) ).
  ENDMETHOD.

  METHOD interface_object_type.
    zcl_abapgit_ortec_ser_pref_oo=>mv_language = 'E'.
    zcl_abapgit_ortec_ser_pref_oo=>mv_prepared = abap_true.
    seed_classtx( iv_clsname = 'ZIF_SMALL' iv_langu = 'D' iv_descr = 'Kleines Interface' ).

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt( ( object = 'INTF' obj_name = 'ZIF_SMALL' ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_oo=>extract_for_batch( lt_tadir ).
    cl_abap_unit_assert=>assert_not_initial( lv_buffer ).

    zcl_abapgit_ortec_ser_pref_oo=>clear( ).
    zcl_abapgit_ortec_ser_pref_oo=>inject_batch_from_buffer( lv_buffer ).

    DATA(rv_found) = zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_class(
      iv_clsname = 'ZIF_SMALL' iv_language = 'E' ).
    cl_abap_unit_assert=>assert_true( rv_found ).
  ENDMETHOD.

  METHOD compo_only_is_hit.
    zcl_abapgit_ortec_ser_pref_oo=>mv_language = 'E'.
    zcl_abapgit_ortec_ser_pref_oo=>mv_prepared = abap_true.
    seed_compotx( iv_clsname = 'ZCL_COMPO_ONLY' ).

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt( ( object = 'CLAS' obj_name = 'ZCL_COMPO_ONLY' ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_oo=>extract_for_batch( lt_tadir ).

    DATA ls_hdr TYPE zaog_ser_env_bhdr.
    DATA lt_entries TYPE zaog_ser_env_bentry_tt.
    read_envelope( EXPORTING iv_buffer = lv_buffer IMPORTING es_hdr = ls_hdr et_entries = lt_entries ).

    READ TABLE lt_entries INTO DATA(ls_entry) WITH KEY obj_name = 'ZCL_COMPO_ONLY'.
    cl_abap_unit_assert=>assert_subrc( exp = 0 act = sy-subrc ).
    cl_abap_unit_assert=>assert_equals( exp = 'P' act = ls_entry-state ).
    cl_abap_unit_assert=>assert_true( xsdbool( ls_entry-actual_bytes > 0 ) ).
  ENDMETHOD.

  METHOD subco_only_is_hit.
    zcl_abapgit_ortec_ser_pref_oo=>mv_language = 'E'.
    zcl_abapgit_ortec_ser_pref_oo=>mv_prepared = abap_true.
    seed_subcotx( iv_clsname = 'ZCL_SUBCO_ONLY' ).

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt( ( object = 'CLAS' obj_name = 'ZCL_SUBCO_ONLY' ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_oo=>extract_for_batch( lt_tadir ).

    DATA ls_hdr TYPE zaog_ser_env_bhdr.
    DATA lt_entries TYPE zaog_ser_env_bentry_tt.
    read_envelope( EXPORTING iv_buffer = lv_buffer IMPORTING es_hdr = ls_hdr et_entries = lt_entries ).

    READ TABLE lt_entries INTO DATA(ls_entry) WITH KEY obj_name = 'ZCL_SUBCO_ONLY'.
    cl_abap_unit_assert=>assert_subrc( exp = 0 act = sy-subrc ).
    cl_abap_unit_assert=>assert_equals( exp = 'P' act = ls_entry-state ).
  ENDMETHOD.

  METHOD both_compo_subco_is_hit.
    zcl_abapgit_ortec_ser_pref_oo=>mv_language = 'E'.
    zcl_abapgit_ortec_ser_pref_oo=>mv_prepared = abap_true.
    seed_compotx( iv_clsname = 'ZCL_BOTH' ).
    seed_subcotx( iv_clsname = 'ZCL_BOTH' ).

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt( ( object = 'CLAS' obj_name = 'ZCL_BOTH' ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_oo=>extract_for_batch( lt_tadir ).

    DATA ls_hdr TYPE zaog_ser_env_bhdr.
    DATA lt_entries TYPE zaog_ser_env_bentry_tt.
    read_envelope( EXPORTING iv_buffer = lv_buffer IMPORTING es_hdr = ls_hdr et_entries = lt_entries ).

    READ TABLE lt_entries INTO DATA(ls_entry) WITH KEY obj_name = 'ZCL_BOTH'.
    cl_abap_unit_assert=>assert_subrc( exp = 0 act = sy-subrc ).
    cl_abap_unit_assert=>assert_equals( exp = 'P' act = ls_entry-state ).
  ENDMETHOD.

  METHOD missing_optional_still_hit.
    " classtx present, no compotx/subcotx at all - still a real HIT, per
    " EXTRACT_FOR_BATCH's "ANY of the three tables has a row" rule.
    zcl_abapgit_ortec_ser_pref_oo=>mv_language = 'E'.
    zcl_abapgit_ortec_ser_pref_oo=>mv_prepared = abap_true.
    seed_classtx( iv_clsname = 'ZCL_CLASSTX_ONLY' ).

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt( ( object = 'CLAS' obj_name = 'ZCL_CLASSTX_ONLY' ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_oo=>extract_for_batch( lt_tadir ).

    DATA ls_hdr TYPE zaog_ser_env_bhdr.
    DATA lt_entries TYPE zaog_ser_env_bentry_tt.
    read_envelope( EXPORTING iv_buffer = lv_buffer IMPORTING es_hdr = ls_hdr et_entries = lt_entries ).

    READ TABLE lt_entries INTO DATA(ls_entry) WITH KEY obj_name = 'ZCL_CLASSTX_ONLY'.
    cl_abap_unit_assert=>assert_subrc( exp = 0 act = sy-subrc ).
    cl_abap_unit_assert=>assert_equals( exp = 'P' act = ls_entry-state ).
  ENDMETHOD.

  METHOD multiple_languages_round_trip.
    zcl_abapgit_ortec_ser_pref_oo=>mv_language = 'E'.
    zcl_abapgit_ortec_ser_pref_oo=>mv_prepared = abap_true.
    seed_compotx( iv_clsname = 'ZCL_MULTI_LANG' iv_langu = 'E' iv_descr = 'English text' ).
    seed_compotx( iv_clsname = 'ZCL_MULTI_LANG' iv_langu = 'D' iv_descr = 'German text' ).

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt( ( object = 'CLAS' obj_name = 'ZCL_MULTI_LANG' ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_oo=>extract_for_batch( lt_tadir ).
    cl_abap_unit_assert=>assert_not_initial( lv_buffer ).

    zcl_abapgit_ortec_ser_pref_oo=>clear( ).
    zcl_abapgit_ortec_ser_pref_oo=>inject_batch_from_buffer( lv_buffer ).

    DATA et_all TYPE zif_abapgit_oo_object_fnc=>ty_seocompotx_tt.
    DATA et_en  TYPE zif_abapgit_oo_object_fnc=>ty_seocompotx_tt.
    DATA et_de  TYPE zif_abapgit_oo_object_fnc=>ty_seocompotx_tt.
    zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_compo(
      EXPORTING iv_clsname = 'ZCL_MULTI_LANG' iv_language = space
      IMPORTING et_descriptions = et_all ).
    zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_compo(
      EXPORTING iv_clsname = 'ZCL_MULTI_LANG' iv_language = 'E'
      IMPORTING et_descriptions = et_en ).
    zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_compo(
      EXPORTING iv_clsname = 'ZCL_MULTI_LANG' iv_language = 'D'
      IMPORTING et_descriptions = et_de ).

    cl_abap_unit_assert=>assert_equals( exp = 2 act = lines( et_all ) ).
    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( et_en ) ).
    cl_abap_unit_assert=>assert_equals( exp = 1 act = lines( et_de ) ).
  ENDMETHOD.

  METHOD namespaced_name_round_trip.
    " CHAR40-safe round trip, mirroring the DOMA/DTEL parity incident's H8
    " check for the OBJ_NAME (SOBJ_NAME) field.
    DATA(lv_clsname) = CONV seoclsname( '/NS/ZCL_FOO' ).
    zcl_abapgit_ortec_ser_pref_oo=>mv_language = 'E'.
    zcl_abapgit_ortec_ser_pref_oo=>mv_prepared = abap_true.
    seed_classtx( iv_clsname = lv_clsname iv_langu = 'D' iv_descr = 'Namespaced class' ).

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt( ( object = 'CLAS' obj_name = CONV #( lv_clsname ) ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_oo=>extract_for_batch( lt_tadir ).
    cl_abap_unit_assert=>assert_not_initial( lv_buffer ).

    zcl_abapgit_ortec_ser_pref_oo=>clear( ).
    zcl_abapgit_ortec_ser_pref_oo=>inject_batch_from_buffer( lv_buffer ).

    DATA(rv_found) = zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_class(
      iv_clsname = lv_clsname iv_language = 'E' ).
    cl_abap_unit_assert=>assert_true( rv_found ).
  ENDMETHOD.

  METHOD mixed_clas_intf_batch.
    zcl_abapgit_ortec_ser_pref_oo=>mv_language = 'E'.
    zcl_abapgit_ortec_ser_pref_oo=>mv_prepared = abap_true.
    seed_classtx( iv_clsname = 'ZCL_MIXED' iv_descr = 'Mixed class' ).
    seed_classtx( iv_clsname = 'ZIF_MIXED' iv_descr = 'Mixed interface' ).

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( object = 'CLAS' obj_name = 'ZCL_MIXED' )
      ( object = 'INTF' obj_name = 'ZIF_MIXED' ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_oo=>extract_for_batch( lt_tadir ).
    cl_abap_unit_assert=>assert_not_initial( lv_buffer ).

    zcl_abapgit_ortec_ser_pref_oo=>clear( ).
    zcl_abapgit_ortec_ser_pref_oo=>inject_batch_from_buffer( lv_buffer ).

    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_class( iv_clsname = 'ZCL_MIXED' iv_language = 'E' ) ).
    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_class( iv_clsname = 'ZIF_MIXED' iv_language = 'E' ) ).
  ENDMETHOD.

  METHOD all_miss_batch_is_initial.
    " PREPARE()'d (mv_prepared = abap_true), but nothing at all cached for
    " these two objects - every entry would be MISS, so no envelope must
    " be built at all, mirroring the DD provider's identical guard.
    zcl_abapgit_ortec_ser_pref_oo=>mv_language = 'E'.
    zcl_abapgit_ortec_ser_pref_oo=>mv_prepared = abap_true.

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( object = 'CLAS' obj_name = 'ZCL_NEVER_CACHED' )
      ( object = 'INTF' obj_name = 'ZIF_NEVER_CACHED' ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_oo=>extract_for_batch( lt_tadir ).

    cl_abap_unit_assert=>assert_initial( lv_buffer ).
  ENDMETHOD.

  METHOD neither_present_is_miss.
    " A real envelope needs at least one HIT to be built at all (see
    " ALL_MISS_BATCH_IS_INITIAL) - so the MISS state is observed here
    " together with one genuine HIT sibling in the same batch.
    zcl_abapgit_ortec_ser_pref_oo=>mv_language = 'E'.
    zcl_abapgit_ortec_ser_pref_oo=>mv_prepared = abap_true.
    seed_classtx( iv_clsname = 'ZCL_HAS_DATA' ).

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( object = 'CLAS' obj_name = 'ZCL_HAS_DATA' )
      ( object = 'CLAS' obj_name = 'ZCL_NO_DATA_AT_ALL' ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_oo=>extract_for_batch( lt_tadir ).
    cl_abap_unit_assert=>assert_not_initial( lv_buffer ).

    DATA ls_hdr TYPE zaog_ser_env_bhdr.
    DATA lt_entries TYPE zaog_ser_env_bentry_tt.
    read_envelope( EXPORTING iv_buffer = lv_buffer IMPORTING es_hdr = ls_hdr et_entries = lt_entries ).

    READ TABLE lt_entries INTO DATA(ls_miss) WITH KEY obj_name = 'ZCL_NO_DATA_AT_ALL'.
    cl_abap_unit_assert=>assert_subrc( exp = 0 act = sy-subrc ).
    cl_abap_unit_assert=>assert_equals( exp = 'M' act = ls_miss-state ).
    cl_abap_unit_assert=>assert_equals( exp = 0 act = ls_miss-actual_bytes ).
  ENDMETHOD.

  METHOD partial_data_no_cross_contam.
    zcl_abapgit_ortec_ser_pref_oo=>mv_language = 'E'.
    zcl_abapgit_ortec_ser_pref_oo=>mv_prepared = abap_true.
    seed_classtx( iv_clsname = 'ZCL_PRESENT' iv_descr = 'Present class' ).

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( object = 'CLAS' obj_name = 'ZCL_PRESENT' )
      ( object = 'CLAS' obj_name = 'ZCL_ABSENT' ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_oo=>extract_for_batch( lt_tadir ).

    zcl_abapgit_ortec_ser_pref_oo=>clear( ).
    zcl_abapgit_ortec_ser_pref_oo=>inject_batch_from_buffer( lv_buffer ).

    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_class( iv_clsname = 'ZCL_PRESENT' iv_language = 'E' ) ).
    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_class( iv_clsname = 'ZCL_ABSENT' iv_language = 'E' ) ).
  ENDMETHOD.

  METHOD reject_unknown_version.
    DATA(ls_hdr) = VALUE zaog_ser_env_bhdr(
      wire_format_version = 99
      provider_id         = 'SER_OO01'
      object_count        = 0 ).
    DATA(lv_buffer) = build_raw_buffer( is_hdr = ls_hdr it_entries = VALUE #( ) ).

    TRY.
        zcl_abapgit_ortec_ser_pref_oo=>inject_batch_from_buffer( lv_buffer ).
        cl_abap_unit_assert=>fail( 'expected zcx_abapgit_exception' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        cl_abap_unit_assert=>assert_char_cp(
          act = lx_error->get_text( ) exp = '*wire_format_version*' ).
    ENDTRY.
  ENDMETHOD.

  METHOD reject_duplicate_entries.
    DATA(ls_hdr) = VALUE zaog_ser_env_bhdr(
      wire_format_version = 1
      provider_id         = 'SER_OO01'
      object_count        = 2 ).
    DATA(lt_entries) = VALUE zaog_ser_env_bentry_tt(
      ( obj_type = 'CLAS' obj_name = 'ZCL_DUP' state = 'P' )
      ( obj_type = 'CLAS' obj_name = 'ZCL_DUP' state = 'P' ) ).
    DATA(lv_buffer) = build_raw_buffer( is_hdr = ls_hdr it_entries = lt_entries ).

    TRY.
        zcl_abapgit_ortec_ser_pref_oo=>inject_batch_from_buffer( lv_buffer ).
        cl_abap_unit_assert=>fail( 'expected zcx_abapgit_exception' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        cl_abap_unit_assert=>assert_char_cp(
          act = lx_error->get_text( ) exp = '*duplicate*' ).
    ENDTRY.
  ENDMETHOD.

  METHOD reject_count_mismatch.
    DATA(ls_hdr) = VALUE zaog_ser_env_bhdr(
      wire_format_version = 1
      provider_id         = 'SER_OO01'
      object_count        = 5 ).
    DATA(lt_entries) = VALUE zaog_ser_env_bentry_tt(
      ( obj_type = 'CLAS' obj_name = 'ZCL_ONE' state = 'P' ) ).
    DATA(lv_buffer) = build_raw_buffer( is_hdr = ls_hdr it_entries = lt_entries ).

    TRY.
        zcl_abapgit_ortec_ser_pref_oo=>inject_batch_from_buffer( lv_buffer ).
        cl_abap_unit_assert=>fail( 'expected zcx_abapgit_exception' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_error).
        cl_abap_unit_assert=>assert_char_cp(
          act = lx_error->get_text( ) exp = '*object_count*' ).
    ENDTRY.
  ENDMETHOD.

  METHOD reject_corrupt_import.
    DATA(lv_buffer) = build_raw_buffer(
      is_hdr     = VALUE #( wire_format_version = 1 provider_id = 'SER_OO01' object_count = 0 )
      it_entries = VALUE #( ) ).
    DATA(lv_corrupt) = lv_buffer(3).

    TRY.
        zcl_abapgit_ortec_ser_pref_oo=>inject_batch_from_buffer( lv_corrupt ).
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
    zcl_abapgit_ortec_ser_pref_oo=>mv_language = 'E'.
    zcl_abapgit_ortec_ser_pref_oo=>mv_prepared = abap_true.
    seed_classtx( iv_clsname = 'ZCL_BATCH_A' ).
    DATA(lv_buffer_a) = zcl_abapgit_ortec_ser_pref_oo=>extract_for_batch(
      VALUE #( ( object = 'CLAS' obj_name = 'ZCL_BATCH_A' ) ) ).

    zcl_abapgit_ortec_ser_pref_oo=>clear( ).
    zcl_abapgit_ortec_ser_pref_oo=>mv_language = 'E'.
    zcl_abapgit_ortec_ser_pref_oo=>mv_prepared = abap_true.
    seed_classtx( iv_clsname = 'ZCL_BATCH_B' ).
    DATA(lv_buffer_b) = zcl_abapgit_ortec_ser_pref_oo=>extract_for_batch(
      VALUE #( ( object = 'CLAS' obj_name = 'ZCL_BATCH_B' ) ) ).

    zcl_abapgit_ortec_ser_pref_oo=>clear( ).
    zcl_abapgit_ortec_ser_pref_oo=>inject_batch_from_buffer( lv_buffer_a ).
    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_class( iv_clsname = 'ZCL_BATCH_A' iv_language = 'E' ) ).
    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_class( iv_clsname = 'ZCL_BATCH_B' iv_language = 'E' ) ).

    " a pooled worker session is reused for the SECOND dispatch WITHOUT an
    " intervening CLEAR( ) - INJECT_BATCH_FROM_BUFFER itself must clear
    " the caches, not rely on the caller to do so.
    zcl_abapgit_ortec_ser_pref_oo=>inject_batch_from_buffer( lv_buffer_b ).
    cl_abap_unit_assert=>assert_true(
      zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_class( iv_clsname = 'ZCL_BATCH_B' iv_language = 'E' ) ).
    cl_abap_unit_assert=>assert_false(
      zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_class( iv_clsname = 'ZCL_BATCH_A' iv_language = 'E' ) ).
  ENDMETHOD.

  METHOD full_round_trip_byte_ident.
    " Full EXTRACT_FOR_BATCH -> (simulated worker) INJECT_BATCH_FROM_BUFFER
    " -> GET_DESCRIPTIONS_* round trip returns byte-identical data to what
    " was originally prepared, for a fixture with several objects and
    " several languages. Live serialized-file-set parity against a real
    " CLAS/INTF object remains an IT8-only step (no live SAP connectivity
    " this session) - same disclosed boundary DOMA/DTEL had before its own
    " IT8 validation.
    zcl_abapgit_ortec_ser_pref_oo=>mv_language = 'E'.
    zcl_abapgit_ortec_ser_pref_oo=>mv_prepared = abap_true.
    seed_classtx( iv_clsname = 'ZCL_FULL_A' iv_langu = 'D' iv_descr = 'Klasse A' ).
    seed_compotx( iv_clsname = 'ZCL_FULL_A' iv_cmpname = 'M1' iv_langu = 'E' iv_descr = 'Method 1 EN' ).
    seed_compotx( iv_clsname = 'ZCL_FULL_A' iv_cmpname = 'M1' iv_langu = 'D' iv_descr = 'Methode 1 DE' ).
    seed_subcotx( iv_clsname = 'ZCL_FULL_A' iv_cmpname = 'M1' iv_sconame = 'P1' iv_langu = 'E' iv_descr = 'Param 1 EN' ).
    seed_classtx( iv_clsname = 'ZIF_FULL_B' iv_langu = 'D' iv_descr = 'Interface B' ).

    DATA et_classtx_a_before TYPE zif_abapgit_oo_object_fnc=>ty_seoclasstx_tt.
    DATA et_compotx_a_before TYPE zif_abapgit_oo_object_fnc=>ty_seocompotx_tt.
    DATA et_subcotx_a_before TYPE zif_abapgit_oo_object_fnc=>ty_seosubcotx_tt.
    DATA et_classtx_b_before TYPE zif_abapgit_oo_object_fnc=>ty_seoclasstx_tt.
    zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_class(
      EXPORTING iv_clsname = 'ZCL_FULL_A' iv_language = 'E' IMPORTING et_descriptions = et_classtx_a_before ).
    zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_compo(
      EXPORTING iv_clsname = 'ZCL_FULL_A' iv_language = space IMPORTING et_descriptions = et_compotx_a_before ).
    zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_subco(
      EXPORTING iv_clsname = 'ZCL_FULL_A' iv_language = space IMPORTING et_descriptions = et_subcotx_a_before ).
    zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_class(
      EXPORTING iv_clsname = 'ZIF_FULL_B' iv_language = 'E' IMPORTING et_descriptions = et_classtx_b_before ).

    DATA(lt_tadir) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( object = 'CLAS' obj_name = 'ZCL_FULL_A' )
      ( object = 'INTF' obj_name = 'ZIF_FULL_B' ) ).
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_oo=>extract_for_batch( lt_tadir ).
    cl_abap_unit_assert=>assert_not_initial( lv_buffer ).

    zcl_abapgit_ortec_ser_pref_oo=>clear( ).
    zcl_abapgit_ortec_ser_pref_oo=>inject_batch_from_buffer( lv_buffer ).

    DATA et_classtx_a_after TYPE zif_abapgit_oo_object_fnc=>ty_seoclasstx_tt.
    DATA et_compotx_a_after TYPE zif_abapgit_oo_object_fnc=>ty_seocompotx_tt.
    DATA et_subcotx_a_after TYPE zif_abapgit_oo_object_fnc=>ty_seosubcotx_tt.
    DATA et_classtx_b_after TYPE zif_abapgit_oo_object_fnc=>ty_seoclasstx_tt.
    zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_class(
      EXPORTING iv_clsname = 'ZCL_FULL_A' iv_language = 'E' IMPORTING et_descriptions = et_classtx_a_after ).
    zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_compo(
      EXPORTING iv_clsname = 'ZCL_FULL_A' iv_language = space IMPORTING et_descriptions = et_compotx_a_after ).
    zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_subco(
      EXPORTING iv_clsname = 'ZCL_FULL_A' iv_language = space IMPORTING et_descriptions = et_subcotx_a_after ).
    zcl_abapgit_ortec_ser_pref_oo=>get_descriptions_class(
      EXPORTING iv_clsname = 'ZIF_FULL_B' iv_language = 'E' IMPORTING et_descriptions = et_classtx_b_after ).

    cl_abap_unit_assert=>assert_equals( exp = et_classtx_a_before act = et_classtx_a_after ).
    cl_abap_unit_assert=>assert_equals( exp = et_compotx_a_before act = et_compotx_a_after ).
    cl_abap_unit_assert=>assert_equals( exp = et_subcotx_a_before act = et_subcotx_a_after ).
    cl_abap_unit_assert=>assert_equals( exp = et_classtx_b_before act = et_classtx_b_after ).
  ENDMETHOD.

  METHOD extract_no_oo_objects_empty.
    zcl_abapgit_ortec_ser_pref_oo=>mv_language = 'E'.
    zcl_abapgit_ortec_ser_pref_oo=>mv_prepared = abap_true.

    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_oo=>extract_for_batch(
      VALUE #( ( object = 'PROG' obj_name = 'SAPMZ_TEST' ) ) ).

    cl_abap_unit_assert=>assert_initial( lv_buffer ).
  ENDMETHOD.

  METHOD extract_not_prepared_empty.
    " PREPARE() was never called (MV_PREPARED = ABAP_FALSE) - must return
    " INITIAL with no DB access, even though the object keys are genuine
    " CLAS/INTF rows.
    DATA(lv_buffer) = zcl_abapgit_ortec_ser_pref_oo=>extract_for_batch(
      VALUE #( ( object = 'CLAS' obj_name = 'ZCL_ANYTHING' ) ) ).

    cl_abap_unit_assert=>assert_initial( lv_buffer ).
  ENDMETHOD.

ENDCLASS.
