CLASS zcl_abapgit_ortec_delta DEFINITION LOCAL FRIENDS ltcl_delta.

CLASS ltcl_delta DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_D1DLT'.

    METHODS setup.
    METHODS teardown.

    "! Package D1: two REF_DELTA rows declaring the SAME external base must
    "! trigger exactly one bulk-loaded object, not two.
    METHODS bulk_base_dedups_sha1s FOR TESTING RAISING cx_static_check.
    "! Package D1: three distinct external bases must still be resolved in
    "! exactly one bulk_resolve_external_bases/get_objects call.
    METHODS bulk_base_one_call_only FOR TESTING RAISING cx_static_check.
    "! Package D1: every merged external base must receive its own unique
    "! ct_objects index - no shared/defaulted index = 0 collision.
    METHODS bulk_base_unique_index FOR TESTING RAISING cx_static_check.
    "! Package D1: a declared external base that does not exist anywhere
    "! (neither in-pack nor in the object store) must raise.
    METHODS bulk_base_missing_raises FOR TESTING RAISING cx_static_check.
    "! Package D1: a "base" that is itself still an unresolved delta type
    "! (ref_d/ofs_d) must be rejected, never silently accepted as a base.
    METHODS bulk_base_wrong_type_raises FOR TESTING RAISING cx_static_check.
    "! Package D1: a stored row whose data does not hash to its own key
    "! (corruption) must be rejected by bulk_resolve_external_bases's
    "! mandatory hash-verification step.
    METHODS bulk_base_corrupt_hash_raises FOR TESTING RAISING cx_static_check.
    "! Package D1 end-to-end: two REF_DELTA entries sharing one external
    "! base must both resolve correctly against that single loaded base.
    METHODS shared_base_two_deltas FOR TESTING RAISING cx_static_check.
    "! Package D1 end-to-end: a pack mixing an in-pack OFS_DELTA chain with
    "! an external REF_DELTA base must resolve both correctly in one pass.
    METHODS mixed_ref_ofs_chain_ok FOR TESTING RAISING cx_static_check.
    "! Package D1 performance contract: resolve_all with external bases
    "! present must never fall back to resolve_one's on-demand per-object
    "! thin fetch - phase 1.5's bulk load must resolve them all up front.
    METHODS no_sql_in_pack_phase FOR TESTING RAISING cx_static_check.
    "! Package D1 acceptance: two REF_DELTA entries declaring the identical
    "! IN-PACK base SHA1 (not an external base - phase 1 alone must resolve
    "! this, no bulk load involved) must both resolve correctly against
    "! that single shared base - the non-unique "sha" secondary key lookup
    "! in resolve_one must not pick the wrong row or double-count the base.
    METHODS duplicate_declared_sha_ok FOR TESTING RAISING cx_static_check.
    "! Package D1 acceptance: a REF_DELTA whose declared base is genuinely
    "! missing everywhere (not in-pack, not in the object store) must raise
    "! via the FULL resolve_all path - phase 1's sweeps converge with no
    "! progress, phase 1.5's bulk load then raises - not just via a direct
    "! unit call to bulk_resolve_external_bases (see bulk_base_missing_raises).
    METHODS exhausted_recovery_raises FOR TESTING RAISING cx_static_check.
    "! Package D1 acceptance: extends the legacy chain_onto_later_unresolved
    "! shape (a 3-link A->B->C in-pack chain) to the new phase 1.5 path - the
    "! chain's terminal base is now genuinely EXTERNAL, and each successive
    "! link is an OFS_DELTA physically later in the pack. Proves the OFS
    "! branch's recursive resolve_one call correctly terminates through a
    "! phase-1.5-merged external result, not just an in-pack plain object.
    METHODS base_later_in_pack_order FOR TESTING RAISING cx_static_check.
ENDCLASS.

CLASS ltcl_delta IMPLEMENTATION.

  METHOD setup.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    zcl_abapgit_ortec_delta=>gv_bulk_load_calls = 0.
    zcl_abapgit_ortec_delta=>gv_thin_fetch_calls = 0.
  ENDMETHOD.

  METHOD teardown.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    ROLLBACK WORK.
  ENDMETHOD.

  METHOD bulk_base_dedups_sha1s.
    DATA lt_sha1s   TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_base_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    lv_base_sha = zcl_abapgit_hash=>sha1_blob( '41414141' ). " "AAAA"
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_base_sha iv_type = 'blob' iv_data = '41414141' ).

    APPEND lv_base_sha TO lt_sha1s.
    APPEND lv_base_sha TO lt_sha1s.

    lt_objects = zcl_abapgit_ortec_delta=>bulk_resolve_external_bases(
      iv_repo_key = mc_repo
      it_sha1s    = lt_sha1s ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_objects ) exp = 1
      msg = 'A duplicate requested SHA1 must be deduplicated into one loaded object' ).
    cl_abap_unit_assert=>assert_equals( act = zcl_abapgit_ortec_delta=>gv_bulk_load_calls exp = 1 ).
  ENDMETHOD.

  METHOD bulk_base_one_call_only.
    DATA lt_sha1s   TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_sha1    TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_data    TYPE xstring.

    DATA(lt_sources) = VALUE string_table( ( `41414141` ) ( `42424242` ) ( `43434343` ) ).
    LOOP AT lt_sources INTO DATA(lv_hex).
      lv_data = lv_hex.
      lv_sha1 = zcl_abapgit_hash=>sha1_blob( lv_data ).
      zcl_abapgit_ortec_obj_store=>store_object(
        iv_repo_key = mc_repo iv_sha1 = lv_sha1 iv_type = 'blob' iv_data = lv_data ).
      APPEND lv_sha1 TO lt_sha1s.
    ENDLOOP.

    lt_objects = zcl_abapgit_ortec_delta=>bulk_resolve_external_bases(
      iv_repo_key = mc_repo
      it_sha1s    = lt_sha1s ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_objects ) exp = 3 ).
    cl_abap_unit_assert=>assert_equals( act = zcl_abapgit_ortec_delta=>gv_bulk_load_calls exp = 1
      msg = 'Three distinct external bases must still cost exactly one bulk call' ).
  ENDMETHOD.

  METHOD bulk_base_unique_index.
    DATA lt_objects    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object     LIKE LINE OF lt_objects.
    DATA lt_offset_map TYPE zcl_abapgit_ortec_delta=>ty_offset_map_tt.
    DATA lt_ofs_meta   TYPE zcl_abapgit_ortec_delta=>ty_ofs_meta_tt.
    DATA lv_base1_sha  TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_base2_sha  TYPE zif_abapgit_git_definitions=>ty_sha1.

    lv_base1_sha = zcl_abapgit_hash=>sha1_blob( '41414141' ). " "AAAA"
    lv_base2_sha = zcl_abapgit_hash=>sha1_blob( '42424242' ). " "BBBB"
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_base1_sha iv_type = 'blob' iv_data = '41414141' ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_base2_sha iv_type = 'blob' iv_data = '42424242' ).

    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_base1_sha.
    ls_object-data  = '040590040121'.
    ls_object-index = 1.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 0 obj_index = 1 ) INTO TABLE lt_offset_map.

    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_base2_sha.
    ls_object-data  = '040590040121'.
    ls_object-index = 2.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 10 obj_index = 2 ) INTO TABLE lt_offset_map.

    zcl_abapgit_ortec_delta=>resolve_all(
      EXPORTING
        it_offset_map = lt_offset_map
        it_ofs_meta   = lt_ofs_meta
        iv_repo_key   = mc_repo
      CHANGING
        ct_objects    = lt_objects ).

    DATA lt_merged_indexes TYPE HASHED TABLE OF i WITH UNIQUE KEY table_line.
    LOOP AT lt_objects INTO ls_object WHERE index > 2.
      cl_abap_unit_assert=>assert_true(
        act = xsdbool( NOT line_exists( lt_merged_indexes[ table_line = ls_object-index ] ) )
        msg = 'Every merged external base must have a distinct index - no shared/defaulted index' ).
      INSERT ls_object-index INTO TABLE lt_merged_indexes.
    ENDLOOP.
    cl_abap_unit_assert=>assert_equals( act = lines( lt_merged_indexes ) exp = 2 ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 1.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '4141414121'
      msg = 'The first delta must resolve against its own base "AAAA", not the second' ).
    READ TABLE lt_objects INTO ls_object WITH KEY index = 2.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '4242424221'
      msg = 'The second delta must resolve against its own base "BBBB", not the first' ).
  ENDMETHOD.

  METHOD bulk_base_missing_raises.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' TO lt_sha1s.

    TRY.
        zcl_abapgit_ortec_delta=>bulk_resolve_external_bases(
          iv_repo_key = mc_repo
          it_sha1s    = lt_sha1s ).
        cl_abap_unit_assert=>fail( 'A genuinely missing external base must raise' ).
      CATCH zcx_abapgit_exception.
    ENDTRY.
  ENDMETHOD.

  METHOD bulk_base_wrong_type_raises.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv_sha1  TYPE zif_abapgit_git_definitions=>ty_sha1.

    " A row stored under a still-unresolved delta type can never be a valid
    " base - simulates a corrupted/incompletely-decoded object store row.
    lv_sha1 = 'cccccccccccccccccccccccccccccccccccccccc'.
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_sha1
      iv_type     = zif_abapgit_git_definitions=>c_type-ref_d
      iv_data     = '0102030405' ).
    APPEND lv_sha1 TO lt_sha1s.

    TRY.
        zcl_abapgit_ortec_delta=>bulk_resolve_external_bases(
          iv_repo_key = mc_repo
          it_sha1s    = lt_sha1s ).
        cl_abap_unit_assert=>fail( 'A base row that is itself an unresolved delta type must raise' ).
      CATCH zcx_abapgit_exception.
    ENDTRY.
  ENDMETHOD.

  METHOD bulk_base_corrupt_hash_raises.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv_sha1  TYPE zif_abapgit_git_definitions=>ty_sha1.

    " Store real blob data under a SHA1 key that does NOT match its own
    " content - simulates on-disk/table corruption.
    lv_sha1 = 'dddddddddddddddddddddddddddddddddddddddd'.
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_sha1
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = '41414141' ). " "AAAA" does not hash to lv_sha1
    APPEND lv_sha1 TO lt_sha1s.

    TRY.
        zcl_abapgit_ortec_delta=>bulk_resolve_external_bases(
          iv_repo_key = mc_repo
          it_sha1s    = lt_sha1s ).
        cl_abap_unit_assert=>fail( 'A base whose data does not hash to its own key must raise' ).
      CATCH zcx_abapgit_exception.
    ENDTRY.
  ENDMETHOD.

  METHOD shared_base_two_deltas.
    DATA lt_objects    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object     LIKE LINE OF lt_objects.
    DATA lt_offset_map TYPE zcl_abapgit_ortec_delta=>ty_offset_map_tt.
    DATA lt_ofs_meta   TYPE zcl_abapgit_ortec_delta=>ty_ofs_meta_tt.
    DATA lv_base_sha   TYPE zif_abapgit_git_definitions=>ty_sha1.

    lv_base_sha = zcl_abapgit_hash=>sha1_blob( '48656C6C6F' ). " "Hello"
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_base_sha iv_type = 'blob' iv_data = '48656C6C6F' ).

    " Both deltas declare the SAME external base and apply the same
    " "Hello" -> "Hello!" transform.
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_base_sha.
    ls_object-data  = '050690050121'.
    ls_object-index = 1.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 0 obj_index = 1 ) INTO TABLE lt_offset_map.

    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_base_sha.
    ls_object-data  = '050690050121'.
    ls_object-index = 2.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 10 obj_index = 2 ) INTO TABLE lt_offset_map.

    zcl_abapgit_ortec_delta=>resolve_all(
      EXPORTING
        it_offset_map = lt_offset_map
        it_ofs_meta   = lt_ofs_meta
        iv_repo_key   = mc_repo
      CHANGING
        ct_objects    = lt_objects ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 1.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '48656C6C6F21' ).
    READ TABLE lt_objects INTO ls_object WITH KEY index = 2.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '48656C6C6F21' ).

    cl_abap_unit_assert=>assert_equals( act = zcl_abapgit_ortec_delta=>gv_bulk_load_calls exp = 1
      msg = 'Two deltas sharing one external base must still cost exactly one bulk call' ).
  ENDMETHOD.

  METHOD mixed_ref_ofs_chain_ok.
    DATA lt_objects    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object     LIKE LINE OF lt_objects.
    DATA lt_offset_map TYPE zcl_abapgit_ortec_delta=>ty_offset_map_tt.
    DATA lt_ofs_meta   TYPE zcl_abapgit_ortec_delta=>ty_ofs_meta_tt.
    DATA lv_base_sha   TYPE zif_abapgit_git_definitions=>ty_sha1.

    " External base "Hello" (loaded via phase 1.5).
    lv_base_sha = zcl_abapgit_hash=>sha1_blob( '48656C6C6F' ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_base_sha iv_type = 'blob' iv_data = '48656C6C6F' ).

    " Object 1: REF_DELTA against the external base -> "Hello!".
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_base_sha.
    ls_object-data  = '050690050121'.
    ls_object-index = 1.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 0 obj_index = 1 ) INTO TABLE lt_offset_map.

    " Object 2: in-pack OFS_DELTA chained onto object 1 -> "Hello!!".
    CLEAR ls_object.
    ls_object-type  = zcl_abapgit_ortec_delta=>c_type_ofs_d.
    ls_object-data  = '060790060121'.
    ls_object-index = 2.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 20 obj_index = 2 ) INTO TABLE lt_offset_map.
    INSERT VALUE #( obj_index = 2 base_offset = 0 ) INTO TABLE lt_ofs_meta.

    zcl_abapgit_ortec_delta=>resolve_all(
      EXPORTING
        it_offset_map = lt_offset_map
        it_ofs_meta   = lt_ofs_meta
        iv_repo_key   = mc_repo
      CHANGING
        ct_objects    = lt_objects ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 1.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '48656C6C6F21'
      msg = 'The externally-based REF_DELTA must resolve to "Hello!"' ).
    READ TABLE lt_objects INTO ls_object WITH KEY index = 2.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '48656C6C6F2121'
      msg = 'The in-pack OFS_DELTA chained onto the external-based delta must resolve to "Hello!!"' ).
  ENDMETHOD.

  METHOD no_sql_in_pack_phase.
    DATA lt_objects    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object     LIKE LINE OF lt_objects.
    DATA lt_offset_map TYPE zcl_abapgit_ortec_delta=>ty_offset_map_tt.
    DATA lt_ofs_meta   TYPE zcl_abapgit_ortec_delta=>ty_ofs_meta_tt.
    DATA lv_base_sha   TYPE zif_abapgit_git_definitions=>ty_sha1.

    lv_base_sha = zcl_abapgit_hash=>sha1_blob( '48656C6C6F' ). " "Hello"
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_base_sha iv_type = 'blob' iv_data = '48656C6C6F' ).

    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_base_sha.
    ls_object-data  = '050690050121'.
    ls_object-index = 1.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 0 obj_index = 1 ) INTO TABLE lt_offset_map.

    zcl_abapgit_ortec_delta=>resolve_all(
      EXPORTING
        it_offset_map = lt_offset_map
        it_ofs_meta   = lt_ofs_meta
        iv_repo_key   = mc_repo
      CHANGING
        ct_objects    = lt_objects ).

    cl_abap_unit_assert=>assert_equals( act = zcl_abapgit_ortec_delta=>gv_bulk_load_calls exp = 1
      msg = 'External base loading must go through exactly one bulk call' ).
    cl_abap_unit_assert=>assert_equals( act = zcl_abapgit_ortec_delta=>gv_thin_fetch_calls exp = 0
      msg = 'Phase 1.5''s bulk load must resolve every external base up front - resolve_one''s ' &&
            'on-demand per-object thin-fetch fallback must never be reached' ).
  ENDMETHOD.

  METHOD duplicate_declared_sha_ok.
    DATA lt_objects    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object     LIKE LINE OF lt_objects.
    DATA lt_offset_map TYPE zcl_abapgit_ortec_delta=>ty_offset_map_tt.
    DATA lt_ofs_meta   TYPE zcl_abapgit_ortec_delta=>ty_ofs_meta_tt.
    DATA lv_hi_sha     TYPE zif_abapgit_git_definitions=>ty_sha1.

    lv_hi_sha = zcl_abapgit_hash=>sha1_blob( '4869' ). " "Hi"

    " Object 1: REF_DELTA declaring lv_hi_sha as its base.
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_hi_sha.
    ls_object-data  = '020390020121'.
    ls_object-index = 1.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 0 obj_index = 1 ) INTO TABLE lt_offset_map.

    " Object 2: a SECOND, independent REF_DELTA declaring the IDENTICAL
    " base SHA1 as object 1 - not an external base, resolvable purely
    " in-pack once object 3 (the real base) is reached.
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_hi_sha.
    ls_object-data  = '020390020121'.
    ls_object-index = 2.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 10 obj_index = 2 ) INTO TABLE lt_offset_map.

    " Object 3: the real, already-resolved base "Hi", shared by both deltas.
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-data  = '4869'.
    ls_object-sha1  = lv_hi_sha.
    ls_object-index = 3.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 20 obj_index = 3 ) INTO TABLE lt_offset_map.

    zcl_abapgit_ortec_delta=>resolve_all(
      EXPORTING
        it_offset_map = lt_offset_map
        it_ofs_meta   = lt_ofs_meta
        iv_repo_key   = mc_repo
      CHANGING
        ct_objects    = lt_objects ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 1.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '486921'
      msg = 'The first delta must resolve against the shared in-pack base "Hi"' ).
    READ TABLE lt_objects INTO ls_object WITH KEY index = 2.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '486921'
      msg = 'The second delta declaring the identical base SHA1 must resolve correctly too' ).

    cl_abap_unit_assert=>assert_equals( act = zcl_abapgit_ortec_delta=>gv_bulk_load_calls exp = 0
      msg = 'A base resolvable purely in-pack must never trigger an external bulk load' ).
  ENDMETHOD.

  METHOD exhausted_recovery_raises.
    DATA lt_objects    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object     LIKE LINE OF lt_objects.
    DATA lt_offset_map TYPE zcl_abapgit_ortec_delta=>ty_offset_map_tt.
    DATA lt_ofs_meta   TYPE zcl_abapgit_ortec_delta=>ty_ofs_meta_tt.
    DATA lv_caught     TYPE abap_bool.

    " REF_DELTA declaring a base that exists NOWHERE - not in-pack, never
    " stored. Phase 1's sweeps converge with zero progress (no in-pack
    " candidate), then phase 1.5's bulk load must raise.
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = 'ffffffffffffffffffffffffffffffffffffffff'.
    ls_object-data  = '020390020121'.
    ls_object-index = 1.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 0 obj_index = 1 ) INTO TABLE lt_offset_map.

    lv_caught = abap_false.
    TRY.
        zcl_abapgit_ortec_delta=>resolve_all(
          EXPORTING
            it_offset_map = lt_offset_map
            it_ofs_meta   = lt_ofs_meta
            iv_repo_key   = mc_repo
          CHANGING
            ct_objects    = lt_objects ).
      CATCH zcx_abapgit_exception.
        lv_caught = abap_true.
    ENDTRY.

    cl_abap_unit_assert=>assert_true( act = lv_caught
      msg = 'resolve_all must raise once sweeps are exhausted and the declared base is ' &&
            'genuinely missing everywhere - not silently leave the delta unresolved' ).
  ENDMETHOD.

  METHOD base_later_in_pack_order.
    " Extends the legacy chain_onto_later_unresolved shape (a 3-link A->B->C
    " in-pack chain, each link one further step "later in the pack") to
    " phase 1.5: the chain's terminal base is now genuinely EXTERNAL instead
    " of in-pack, and each successive link is an OFS_DELTA physically later
    " in the pack (OFS's format-guaranteed backward offset makes REF_DELTA's
    " "unresolved sibling" ambiguity moot here - resolve_one's OFS branch
    " recursively resolves its base on demand, so this specifically proves
    " that recursion correctly terminates at a phase-1.5-merged external
    " result, not just at an already in-pack plain object).
    DATA lt_objects    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object     LIKE LINE OF lt_objects.
    DATA lt_offset_map TYPE zcl_abapgit_ortec_delta=>ty_offset_map_tt.
    DATA lt_ofs_meta   TYPE zcl_abapgit_ortec_delta=>ty_ofs_meta_tt.
    DATA lv_x_sha      TYPE zif_abapgit_git_definitions=>ty_sha1.

    " X = "Hi" (2 bytes) - a genuinely EXTERNAL base, resolvable only via
    " phase 1.5's bulk load, never present in this pack.
    lv_x_sha = zcl_abapgit_hash=>sha1_blob( '4869' ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_x_sha iv_type = 'blob' iv_data = '4869' ).

    " Object 1: REF_DELTA against the external base X -> "Hi!" (3 bytes).
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_x_sha.
    ls_object-data  = '020390020121'.
    ls_object-index = 1.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 0 obj_index = 1 ) INTO TABLE lt_offset_map.

    " Object 2: OFS_DELTA physically later, chained onto object 1
    " -> "Hi!!" (4 bytes).
    CLEAR ls_object.
    ls_object-type  = zcl_abapgit_ortec_delta=>c_type_ofs_d.
    ls_object-data  = '030490030121'.
    ls_object-index = 2.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 20 obj_index = 2 ) INTO TABLE lt_offset_map.
    INSERT VALUE #( obj_index = 2 base_offset = 0 ) INTO TABLE lt_ofs_meta.

    " Object 3: OFS_DELTA physically even later, chained onto object 2
    " -> "Hi!!!" (5 bytes). A third, deeper "later in pack order" step
    " beyond mixed_ref_ofs_chain_ok's single-link extension.
    CLEAR ls_object.
    ls_object-type  = zcl_abapgit_ortec_delta=>c_type_ofs_d.
    ls_object-data  = '040590040121'.
    ls_object-index = 3.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 40 obj_index = 3 ) INTO TABLE lt_offset_map.
    INSERT VALUE #( obj_index = 3 base_offset = 20 ) INTO TABLE lt_ofs_meta.

    zcl_abapgit_ortec_delta=>resolve_all(
      EXPORTING
        it_offset_map = lt_offset_map
        it_ofs_meta   = lt_ofs_meta
        iv_repo_key   = mc_repo
      CHANGING
        ct_objects    = lt_objects ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 1.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '486921'
      msg = 'The externally-based REF_DELTA must resolve to "Hi!"' ).
    READ TABLE lt_objects INTO ls_object WITH KEY index = 2.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '48692121'
      msg = 'The first OFS_DELTA chained onto it must resolve to "Hi!!"' ).
    READ TABLE lt_objects INTO ls_object WITH KEY index = 3.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '4869212121'
      msg = 'The second, deeper OFS_DELTA link must resolve to "Hi!!!" - proving the ' &&
            'recursive OFS resolution chain terminates correctly through a phase-1.5-' &&
            'merged external result, not just an in-pack plain object' ).

    cl_abap_unit_assert=>assert_equals( act = zcl_abapgit_ortec_delta=>gv_bulk_load_calls exp = 1
      msg = 'The single external base X must be loaded via exactly one bulk call' ).
  ENDMETHOD.

ENDCLASS.
