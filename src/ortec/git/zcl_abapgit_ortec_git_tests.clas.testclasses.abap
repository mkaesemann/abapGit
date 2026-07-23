CLASS ltcl_missing_objects DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_MISOB'.
    METHODS setup. METHODS teardown.
    METHODS noop_when_nothing_missing FOR TESTING RAISING cx_static_check.
    METHODS no_fetch_without_url FOR TESTING RAISING cx_static_check.
    METHODS no_fetch_when_opt_in_off FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_missing_objects IMPLEMENTATION.
  METHOD setup. DELETE FROM zaog_obj_store WHERE repo_key = mc_repo. ENDMETHOD.
  METHOD teardown. DELETE FROM zaog_obj_store WHERE repo_key = mc_repo. ROLLBACK WORK. ENDMETHOD.
  METHOD noop_when_nothing_missing.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv TYPE xstring.
    lv = '31'.

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = '7777777777777777777777777777777777777777'
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv ).
    APPEND '7777777777777777777777777777777777777777' TO lt_sha1s.

    " Everything is already buffered, so this must return without ever
    " attempting a network call (a blank/unreachable URL would fail loudly
    " if a fetch were attempted).
    zcl_abapgit_ortec_missing_obj=>ensure_available(
      iv_repo_key = mc_repo
      iv_url      = 'https://example.invalid/not-a-real-remote.git'
      iv_commit   = '8888888888888888888888888888888888888888'
      it_sha1s    = lt_sha1s ).
  ENDMETHOD.
  METHOD no_fetch_without_url.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND '9999999999999999999999999999999999999999' TO lt_sha1s.

    " Object is not buffered and no URL is supplied - must raise immediately
    " without attempting any network access.
    TRY.
        zcl_abapgit_ortec_missing_obj=>ensure_available(
          iv_repo_key = mc_repo
          iv_url      = ''
          iv_commit   = '8888888888888888888888888888888888888888'
          it_sha1s    = lt_sha1s ).
        cl_abap_unit_assert=>fail( 'Missing object without a URL must raise' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.
  METHOD no_fetch_when_opt_in_off.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' TO lt_sha1s.

    " No repository has opted into the ORTEC write/protocol behavior for this
    " URL, so ensure_available must refuse to fetch and raise rather than
    " attempt a non-negotiated network call.
    TRY.
        zcl_abapgit_ortec_missing_obj=>ensure_available(
          iv_repo_key = mc_repo
          iv_url      = 'https://example.invalid/opt-in-off-repo.git'
          iv_commit   = '8888888888888888888888888888888888888888'
          it_sha1s    = lt_sha1s ).
        cl_abap_unit_assert=>fail( 'Missing object with opt-in inactive must raise' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.
ENDCLASS.

CLASS ltcl_ofs_delta DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_OFSDLT'.
    METHODS offset_single_byte    FOR TESTING RAISING cx_static_check.
    METHODS offset_multi_byte     FOR TESTING RAISING cx_static_check.
    METHODS apply_copy_and_insert FOR TESTING RAISING cx_static_check.
    METHODS resolve_ofs_direct    FOR TESTING RAISING cx_static_check.
    METHODS resolve_ofs_chain     FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_ofs_delta IMPLEMENTATION.

  METHOD offset_single_byte.
    DATA lv_data   TYPE xstring.
    DATA lv_offset TYPE i.

    " Vectors from target_design_phase5.md §1.2 (T1). The +1 continuation
    " bias is the single highest-risk line in the whole OFS_DELTA feature -
    " these vectors pin it exactly.
    lv_data = '00'.
    lv_offset = zcl_abapgit_ortec_delta=>get_offset( CHANGING cv_data = lv_data ).
    cl_abap_unit_assert=>assert_equals( act = lv_offset exp = 0 ).
    cl_abap_unit_assert=>assert_equals( act = xstrlen( lv_data ) exp = 0
      msg = 'A single-byte varint must consume exactly 1 byte' ).

    lv_data = '7F'.
    lv_offset = zcl_abapgit_ortec_delta=>get_offset( CHANGING cv_data = lv_data ).
    cl_abap_unit_assert=>assert_equals( act = lv_offset exp = 127 ).
  ENDMETHOD.

  METHOD offset_multi_byte.
    DATA lv_data   TYPE xstring.
    DATA lv_offset TYPE i.

    " Each vector is followed by a trailer byte 'AA' to prove the decoder
    " stops exactly at the varint boundary and leaves the remainder untouched
    " (important: cv_data must be correctly positioned for the caller to
    " continue parsing the delta instruction stream that follows).
    lv_data = '8000AA'.
    lv_offset = zcl_abapgit_ortec_delta=>get_offset( CHANGING cv_data = lv_data ).
    cl_abap_unit_assert=>assert_equals( act = lv_offset exp = 128 ).
    cl_abap_unit_assert=>assert_equals( act = lv_data exp = 'AA' ).

    lv_data = '807FAA'.
    lv_offset = zcl_abapgit_ortec_delta=>get_offset( CHANGING cv_data = lv_data ).
    cl_abap_unit_assert=>assert_equals( act = lv_offset exp = 255 ).
    cl_abap_unit_assert=>assert_equals( act = lv_data exp = 'AA' ).

    lv_data = '8100AA'.
    lv_offset = zcl_abapgit_ortec_delta=>get_offset( CHANGING cv_data = lv_data ).
    cl_abap_unit_assert=>assert_equals( act = lv_offset exp = 256 ).

    lv_data = 'FF7FAA'.
    lv_offset = zcl_abapgit_ortec_delta=>get_offset( CHANGING cv_data = lv_data ).
    " Git's OFS_DELTA varint adds 1 before each shift (offset = (offset+1)<<7 | byte),
    " NOT a naive base-128 concatenation - this makes the encoding canonical/unique.
    " 127 -> (127+1)*128 + 127 = 16511, not the naive-concat value 16383.
    cl_abap_unit_assert=>assert_equals( act = lv_offset exp = 16511 ).

    lv_data = '808000AA'.
    lv_offset = zcl_abapgit_ortec_delta=>get_offset( CHANGING cv_data = lv_data ).
    cl_abap_unit_assert=>assert_equals( act = lv_offset exp = 16512 ).
    cl_abap_unit_assert=>assert_equals( act = lv_data exp = 'AA' ).
  ENDMETHOD.

  METHOD apply_copy_and_insert.
    DATA lv_base   TYPE xstring.
    DATA lv_delta  TYPE xstring.
    DATA lv_result TYPE xstring.

    " base = "Hello". delta = base-size(5) result-size(6)
    " copy-op 0x90 (copy, size-byte0 present, no offset bytes -> offset 0)
    " + size-byte 0x05 (copy length 5) + insert-op 0x01 + literal 0x21 ('!').
    " Expected result = "Hello!" - see target_design_phase5.md §7.2 (T2).
    lv_base  = '48656C6C6F'.
    lv_delta = '050690050121'.

    lv_result = zcl_abapgit_ortec_delta=>apply( iv_base = lv_base iv_delta = lv_delta ).

    cl_abap_unit_assert=>assert_equals( act = lv_result exp = '48656C6C6F21' ).
  ENDMETHOD.

  METHOD resolve_ofs_direct.
    DATA lt_objects     TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object      LIKE LINE OF lt_objects.
    DATA lt_offset_map  TYPE zcl_abapgit_ortec_delta=>ty_offset_map_tt.
    DATA lt_ofs_meta    TYPE zcl_abapgit_ortec_delta=>ty_ofs_meta_tt.
    DATA lv_expected_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    " Object 1: full blob "Hello" at pack_offset 0.
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-data  = '48656C6C6F'.
    ls_object-sha1  = zcl_abapgit_hash=>sha1_blob( ls_object-data ).
    ls_object-index = 1.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 0 obj_index = 1 ) INTO TABLE lt_offset_map.

    " Object 2: OFS_DELTA at pack_offset 20, base_offset 0 (points back to
    " object 1). Same delta bytes as apply_copy_and_insert: "Hello" -> "Hello!".
    CLEAR ls_object.
    ls_object-type  = zcl_abapgit_ortec_delta=>c_type_ofs_d.
    ls_object-data  = '050690050121'.
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

    READ TABLE lt_objects INTO ls_object WITH KEY index = 2.
    cl_abap_unit_assert=>assert_subrc( msg = 'Resolved OFS_DELTA object must remain in ct_objects' ).
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '48656C6C6F21'
      msg = 'OFS_DELTA must resolve to the base object located by pack offset' ).
    cl_abap_unit_assert=>assert_equals( act = ls_object-type
      exp = zif_abapgit_git_definitions=>c_type-blob
      msg = 'A resolved delta inherits its type from its ultimate base' ).

    lv_expected_sha = zcl_abapgit_hash=>sha1_blob( '48656C6C6F21' ).
    cl_abap_unit_assert=>assert_equals( act = ls_object-sha1 exp = lv_expected_sha
      msg = 'Resolved object must carry its real recomputed content SHA1' ).
  ENDMETHOD.

  METHOD resolve_ofs_chain.
    DATA lt_objects    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object     LIKE LINE OF lt_objects.
    DATA lt_offset_map TYPE zcl_abapgit_ortec_delta=>ty_offset_map_tt.
    DATA lt_ofs_meta   TYPE zcl_abapgit_ortec_delta=>ty_ofs_meta_tt.
    DATA lv_expected_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    " A 2-hop OFS_DELTA chain: obj1 "Hello" -> obj2 "Hello!" -> obj3 "Hello!!".
    " Proves dependency-ordered resolution: obj3's base (obj2) is itself an
    " unresolved delta at the time obj3 is visited - the case a one-pass
    " offset->sha rewrite would resolve incorrectly (see
    " target_design_phase5.md §2.1 / T4).

    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-data  = '48656C6C6F'. " "Hello"
    ls_object-sha1  = zcl_abapgit_hash=>sha1_blob( ls_object-data ).
    ls_object-index = 1.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 0 obj_index = 1 ) INTO TABLE lt_offset_map.

    CLEAR ls_object.
    ls_object-type  = zcl_abapgit_ortec_delta=>c_type_ofs_d.
    ls_object-data  = '050690050121'. " "Hello" -> "Hello!"
    ls_object-index = 2.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 20 obj_index = 2 ) INTO TABLE lt_offset_map.
    INSERT VALUE #( obj_index = 2 base_offset = 0 ) INTO TABLE lt_ofs_meta.

    CLEAR ls_object.
    ls_object-type  = zcl_abapgit_ortec_delta=>c_type_ofs_d.
    " base-size(6) result-size(7) copy(off=0,len=6) insert(1,'!') -> "Hello!!"
    ls_object-data  = '060790060121'.
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

    READ TABLE lt_objects INTO ls_object WITH KEY index = 2.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '48656C6C6F21'
      msg = 'First hop of the chain must resolve to "Hello!"' ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 3.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '48656C6C6F2121'
      msg = 'Second hop of the chain must resolve to "Hello!!", proving the ' &&
            'base (object 2) was resolved before object 3 applied its delta' ).

    lv_expected_sha = zcl_abapgit_hash=>sha1_blob( '48656C6C6F2121' ).
    cl_abap_unit_assert=>assert_equals( act = ls_object-sha1 exp = lv_expected_sha ).
  ENDMETHOD.

ENDCLASS.

CLASS ltcl_ref_delta DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_REFDLT'.
    METHODS setup.
    METHODS teardown.
    METHODS base_after_dependent FOR TESTING RAISING cx_static_check.
    METHODS resolve_after_prior_in_pass FOR TESTING RAISING cx_static_check.
    METHODS two_thin_bases_do_not_collide FOR TESTING RAISING cx_static_check.
    METHODS chain_onto_later_unresolved FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_ref_delta IMPLEMENTATION.

  METHOD setup.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
  ENDMETHOD.

  METHOD teardown.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    ROLLBACK WORK.
  ENDMETHOD.

  METHOD base_after_dependent.
    " REF_DELTA carries no ordering guarantee (unlike OFS_DELTA, which is
    " always positioned strictly backwards in the pack byte stream): the
    " base object CAN legitimately appear AFTER the delta that depends on
    " it. Before the fix, resolve_one's base lookup for object 1 (a ref_d
    " entry whose OWN row also shows the searched-for sha1 as its own
    " unresolved placeholder) would match itself first and recurse forever
    " (spurious "chain exceeds maximum depth"), instead of finding the
    " real, already-resolved base at object 2 (a plain blob, positioned
    " AFTER object 1). This test pins the corrected behavior: the base
    " lookup must skip any still-unresolved ref_d/ofs_d candidate and find
    " the genuinely resolved one regardless of table position.
    DATA lt_objects      TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object       LIKE LINE OF lt_objects.
    DATA lt_offset_map   TYPE zcl_abapgit_ortec_delta=>ty_offset_map_tt.
    DATA lt_ofs_meta     TYPE zcl_abapgit_ortec_delta=>ty_ofs_meta_tt.
    DATA lv_expected_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_base_sha     TYPE zif_abapgit_git_definitions=>ty_sha1.

    " The true base, "Hello!" (6 bytes), is placed at index 2 - AFTER the
    " delta (index 1) that depends on it.
    lv_base_sha = zcl_abapgit_hash=>sha1_blob( '48656C6C6F21' ). " "Hello!"

    " Object 1: REF_DELTA whose declared base is the blob at object 2.
    " Delta instructions: base-size(6) result-size(7)
    " copy-op 0x90 (copy, size-byte0 present, no offset bytes -> offset 0)
    " + size-byte 0x06 (copy length 6) + insert-op 0x01 + literal 0x21 ('!').
    " Applying this against the real 6-byte base "Hello!" must produce
    " "Hello!!" (7 bytes).
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_base_sha. " declared base, NOT this entry's own identity
    ls_object-data  = '060790060121'.
    ls_object-index = 1.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 0 obj_index = 1 ) INTO TABLE lt_offset_map.

    " Object 2: the real, already-resolved base, positioned AFTER object 1.
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-data  = '48656C6C6F21'. " "Hello!"
    ls_object-sha1  = lv_base_sha.
    ls_object-index = 2.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 20 obj_index = 2 ) INTO TABLE lt_offset_map.

    zcl_abapgit_ortec_delta=>resolve_all(
      EXPORTING
        it_offset_map = lt_offset_map
        it_ofs_meta   = lt_ofs_meta
        iv_repo_key   = mc_repo
      CHANGING
        ct_objects    = lt_objects ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 1.
    cl_abap_unit_assert=>assert_subrc( msg = 'Resolved REF_DELTA object must remain in ct_objects' ).
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '48656C6C6F2121'
      msg = 'REF_DELTA must resolve against its real base even when that base is ' &&
            'positioned after the delta in the pack, not self-match or misresolve' ).
    cl_abap_unit_assert=>assert_equals( act = ls_object-type
      exp = zif_abapgit_git_definitions=>c_type-blob
      msg = 'A resolved delta inherits its type from its ultimate base' ).

    lv_expected_sha = zcl_abapgit_hash=>sha1_blob( '48656C6C6F2121' ).
    cl_abap_unit_assert=>assert_equals( act = ls_object-sha1 exp = lv_expected_sha
      msg = 'Resolved object must carry its real recomputed content SHA1' ).
  ENDMETHOD.

  METHOD resolve_after_prior_in_pass.
    " Regression for the MODIFY fix: resolve_one previously promoted a
    " resolved delta via direct field-symbol writes to <ls_object>-sha1, a
    " component of the "sha" secondary sorted key. That does not update the
    " key's internal structure (documented ABAP behavior), so any base
    " lookup performed AFTER at least one prior promotion in the same
    " resolve_all pass risked matching the wrong row via a now-stale key.
    " This test forces exactly that ordering: object 2 is fully resolved
    " and promoted BEFORE object 3 performs its own base lookup for a true
    " base (object 4) positioned even later.
    DATA lt_objects      TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object       LIKE LINE OF lt_objects.
    DATA lt_offset_map   TYPE zcl_abapgit_ortec_delta=>ty_offset_map_tt.
    DATA lt_ofs_meta     TYPE zcl_abapgit_ortec_delta=>ty_ofs_meta_tt.
    DATA lv_expected_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_base_sha     TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_hi_sha       TYPE zif_abapgit_git_definitions=>ty_sha1.

    lv_hi_sha   = zcl_abapgit_hash=>sha1_blob( '4869' ). " "Hi"
    lv_base_sha = zcl_abapgit_hash=>sha1_blob( '48656C6C6F21' ). " "Hello!"

    " Object 1: plain blob "Hi" (2 bytes).
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-data  = '4869'.
    ls_object-sha1  = lv_hi_sha.
    ls_object-index = 1.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 0 obj_index = 1 ) INTO TABLE lt_offset_map.

    " Object 2: REF_DELTA depending on object 1, resolved FIRST by
    " resolve_all's sequential walk (index 2 < 3), forcing a promotion
    " (and, with the fix, a MODIFY) before object 3's own lookup runs.
    " base-size(2) result-size(3) copy(off=0,len=2) insert(1,'!') -> "Hi!".
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_hi_sha.
    ls_object-data  = '020390020121'.
    ls_object-index = 2.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 10 obj_index = 2 ) INTO TABLE lt_offset_map.

    " Object 3: the entry under test - REF_DELTA whose true base (object 4)
    " is positioned AFTER it, unrelated to objects 1/2.
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_base_sha.
    ls_object-data  = '060790060121'.
    ls_object-index = 3.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 20 obj_index = 3 ) INTO TABLE lt_offset_map.

    " Object 4: the true base, "Hello!" (6 bytes), positioned after object 3.
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-data  = '48656C6C6F21'.
    ls_object-sha1  = lv_base_sha.
    ls_object-index = 4.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 30 obj_index = 4 ) INTO TABLE lt_offset_map.

    zcl_abapgit_ortec_delta=>resolve_all(
      EXPORTING
        it_offset_map = lt_offset_map
        it_ofs_meta   = lt_ofs_meta
        iv_repo_key   = mc_repo
      CHANGING
        ct_objects    = lt_objects ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 2.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '486921'
      msg = 'The unrelated, earlier-resolved delta must still resolve correctly to "Hi!"' ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 3.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '48656C6C6F2121'
      msg = 'The entry under test must resolve against its real, later-positioned base ' &&
            'even after another delta was already promoted earlier in the same pass' ).

    lv_expected_sha = zcl_abapgit_hash=>sha1_blob( '48656C6C6F2121' ).
    cl_abap_unit_assert=>assert_equals( act = ls_object-sha1 exp = lv_expected_sha ).
  ENDMETHOD.

  METHOD two_thin_bases_do_not_collide.
    " Regression for a gap found alongside the tabix hotfix:
    " zcl_abapgit_ortec_obj_store=>get_object never populates the returned
    " object's -index field, so every thin-fetched base previously defaulted
    " to index = 0. If two different REF_DELTA entries in the same
    " resolve_all pass each need a DIFFERENT thin base (neither present in
    " ct_objects), both fetched bases would collide on index = 0 and an
    " index-keyed lookup could bind the wrong delta to the wrong base. This
    " test forces exactly two distinct thin fetches in one pass and asserts
    " each delta resolves against its own, correct base.
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

    " Both bases are absent from ct_objects - each delta must go through
    " the thin-base persistent-store fetch, not an in-pack lookup.
    " base-size(4) result-size(5) copy(off=0,len=4) insert(1,'!') -> base+'!'.
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

    READ TABLE lt_objects INTO ls_object WITH KEY index = 1.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '4141414121'
      msg = 'The first delta must resolve against its own thin base "AAAA", not the second' ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 2.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '4242424221'
      msg = 'The second delta must resolve against its own thin base "BBBB", not the first' ).
  ENDMETHOD.

  METHOD chain_onto_later_unresolved.
    " Regression for the multi-pass fixpoint fix: A (index 1) is a REF_DELTA
    " declaring a dependency on B's REAL identity, but B (index 2, positioned
    " AFTER A) is ITSELF still an unresolved REF_DELTA at pack-scan time - its
    " -sha1 field currently holds ITS OWN declared dependency (C's identity),
    " not yet B's real identity. A single ascending pass over the pack can
    " never find B for A (B's row does not carry B's real identity until AFTER
    " B itself is resolved, and resolve_all would already have moved past
    " object 1 by the time object 2 is reached). Before the fix this raised
    " "Delta base not found" even though the whole chain is fully resolvable
    " from within this single pack. This is also the shape of the real-world
    " "Delta base not found in pack/store (N missing)" failure: a raw
    " pre-scan (in zcl_abapgit_ortec_pack_dec) that only recognizes
    " ALREADY-non-delta objects as "in the pack" would flag B's needed
    " identity as external/missing purely because B had not been resolved
    " yet, even though it never leaves this pack.
    DATA lt_objects      TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object       LIKE LINE OF lt_objects.
    DATA lt_offset_map   TYPE zcl_abapgit_ortec_delta=>ty_offset_map_tt.
    DATA lt_ofs_meta     TYPE zcl_abapgit_ortec_delta=>ty_ofs_meta_tt.
    DATA lv_expected_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_b_sha        TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_c_sha        TYPE zif_abapgit_git_definitions=>ty_sha1.

    " C = "Hi" (2 bytes), the real, plain base at the end of the chain.
    lv_c_sha = zcl_abapgit_hash=>sha1_blob( '4869' ).
    " B = "Hi!" (3 bytes), produced by applying object 2's delta to C.
    lv_b_sha = zcl_abapgit_hash=>sha1_blob( '486921' ).

    " Object 1 (A): REF_DELTA declaring a dependency on B's real identity.
    " Delta: base-size(3) result-size(4), copy(off=0,len=3), insert(1,'!')
    " -> applying against B's eventual content "Hi!" yields "Hi!!".
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_b_sha. " declared dependency: B's real identity
    ls_object-data  = '030490030121'.
    ls_object-index = 1.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 0 obj_index = 1 ) INTO TABLE lt_offset_map.

    " Object 2 (B): REF_DELTA declaring a dependency on C's real identity -
    " ITS OWN -sha1 is C's identity, NOT B's identity, until resolved.
    " Positioned AFTER object 1, the dependent that needs B.
    " Delta: base-size(2) result-size(3), copy(off=0,len=2), insert(1,'!')
    " -> applying against C's content "Hi" yields "Hi!".
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-ref_d.
    ls_object-sha1  = lv_c_sha. " declared dependency: C's real identity
    ls_object-data  = '020390020121'.
    ls_object-index = 2.
    APPEND ls_object TO lt_objects.
    INSERT VALUE #( pack_offset = 10 obj_index = 2 ) INTO TABLE lt_offset_map.

    " Object 3 (C): the real, plain base at the very end of the chain.
    CLEAR ls_object.
    ls_object-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-data  = '4869'.
    ls_object-sha1  = lv_c_sha.
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

    READ TABLE lt_objects INTO ls_object WITH KEY index = 2.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '486921'
      msg = 'The intermediate delta (B) must resolve against its real base (C) ' &&
            'even though B itself is only reached later in the ascending pass' ).

    READ TABLE lt_objects INTO ls_object WITH KEY index = 1.
    cl_abap_unit_assert=>assert_equals( act = ls_object-data exp = '48692121'
      msg = 'The dependent delta (A) must resolve against its real base (B) even ' &&
            'though B was still an unresolved delta - not yet a plain object - the ' &&
            'first time A was attempted' ).

    lv_expected_sha = zcl_abapgit_hash=>sha1_blob( '48692121' ).
    cl_abap_unit_assert=>assert_equals( act = ls_object-sha1 exp = lv_expected_sha ).
  ENDMETHOD.

ENDCLASS.

CLASS ltcl_filtered_fetch DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_FLTFCH'.
    METHODS setup. METHODS teardown.
    "! Blank inputs must short-circuit before any network attempt - proven
    "! implicitly: a real HTTP call would fail/hang in this test
    "! environment, so a clean abap_false return proves the early guard
    "! fired instead.
    METHODS blank_inputs_no_network FOR TESTING RAISING cx_static_check.
    "! A commit already present locally must short-circuit to TRUE without
    "! attempting any network call - same implicit proof as above.
    METHODS already_local_short_circuits FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_filtered_fetch IMPLEMENTATION.
  METHOD setup. DELETE FROM zaog_obj_store WHERE repo_key = mc_repo. ENDMETHOD.
  METHOD teardown.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    ROLLBACK WORK.
  ENDMETHOD.

  METHOD blank_inputs_no_network.
    DATA lv_applicable TYPE abap_bool.

    lv_applicable = zcl_abapgit_ortec_fastpath=>try_filtered_commit_fetch(
      iv_url         = ''
      iv_branch_name = 'refs/heads/main'
      iv_commit      = ''
      iv_repo_key    = mc_repo ).

    cl_abap_unit_assert=>assert_equals( act = lv_applicable exp = abap_false
      msg = 'Blank URL/commit must never attempt a network call' ).
  ENDMETHOD.

  METHOD already_local_short_circuits.
    DATA lv_commit_data TYPE xstring.
    DATA lv_commit_sha  TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_applicable  TYPE abap_bool.

    lv_commit_data = '48656C6C6F'.
    lv_commit_sha = zcl_abapgit_hash=>sha1_commit( lv_commit_data ).

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_commit_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-commit
      iv_data     = lv_commit_data ).

    lv_applicable = zcl_abapgit_ortec_fastpath=>try_filtered_commit_fetch(
      iv_url         = 'https://filtered-fetch-test.example.com/repo.git'
      iv_branch_name = 'refs/heads/main'
      iv_commit      = lv_commit_sha
      iv_repo_key    = mc_repo ).

    cl_abap_unit_assert=>assert_equals( act = lv_applicable exp = abap_true
      msg = 'An already-locally-present commit must short-circuit to TRUE without any network attempt' ).
  ENDMETHOD.
ENDCLASS.

CLASS ltcl_switch DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    METHODS no_dump FOR TESTING.
    METHODS absent_strictness_default FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_switch IMPLEMENTATION.
  METHOD no_dump. DATA lv TYPE abap_bool. lv = zcl_abapgit_ortec_git_switch=>is_active_for_repo( 'https://dummy.test/repo.git' ). ENDMETHOD.
  METHOD absent_strictness_default.
    " D4: STRICT must be the shipping default. RELAXED exists only to
    " benchmark the completeness-check cost and must never ship as default.
    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_git_switch=>cs_absent_strictness-mode
      exp = zcl_abapgit_ortec_git_switch=>cs_absent_strictness-mode_strict ).
  ENDMETHOD.
ENDCLASS.

CLASS ltcl_persist_flow DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_url TYPE string VALUE 'https://test-persist.example.com/repo.git'.
    METHODS setup. METHODS teardown.
    METHODS persist_creates_state FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_persist_flow IMPLEMENTATION.
  METHOD setup.
    DATA lv_key TYPE c LENGTH 12.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    IF lv_key IS NOT INITIAL.
      zcl_abapgit_ortec_repo_state=>clear_state( lv_key ).
      DELETE FROM zaog_obj_store WHERE repo_key = lv_key.
    ENDIF.
  ENDMETHOD.
  METHOD teardown.
    DATA lv_key TYPE c LENGTH 12.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    IF lv_key IS NOT INITIAL.
      zcl_abapgit_ortec_repo_state=>clear_state( lv_key ).
      DELETE FROM zaog_obj_store WHERE repo_key = lv_key.
    ENDIF. ROLLBACK WORK.
  ENDMETHOD.
  METHOD persist_creates_state.
    DATA lt_obj TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_key TYPE c LENGTH 12.
    ls_obj-sha1 = 'aabbccddee00112233445566778899aabbccddee'. ls_obj-type = 'commit'. ls_obj-data = '436F6D6D6974'. APPEND ls_obj TO lt_obj.
    ls_obj-sha1 = '1122334455667788990011223344556677889900'. ls_obj-type = 'blob'. ls_obj-data = '48656C6C6F'. APPEND ls_obj TO lt_obj.
    TRY.
        zcl_abapgit_ortec_fastpath=>persist_pull_result( iv_url = mc_url iv_branch_name = 'refs/heads/main'
          iv_commit = 'aabbccddee00112233445566778899aabbccddee' it_objects = lt_obj ).
      CATCH zcx_abapgit_ortec_git. RETURN.
    ENDTRY.
    lv_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( mc_url ).
    IF lv_key IS INITIAL. RETURN. ENDIF.
    cl_abap_unit_assert=>assert_equals( exp = abap_true msg = 'Commit stored'
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = lv_key iv_sha1 = 'aabbccddee00112233445566778899aabbccddee' ) ).
    cl_abap_unit_assert=>assert_equals( exp = abap_true msg = 'Blob stored'
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = lv_key iv_sha1 = '1122334455667788990011223344556677889900' ) ).
    DATA ls_state TYPE zcl_abapgit_ortec_repo_state=>ty_state.
    ls_state = zcl_abapgit_ortec_repo_state=>get_state( iv_repo_key = lv_key iv_branch_name = 'refs/heads/main' ).
    cl_abap_unit_assert=>assert_equals( act = ls_state-fetch_commit
      exp = 'aabbccddee00112233445566778899aabbccddee' msg = 'State commit must match' ).
  ENDMETHOD.
ENDCLASS.

CLASS ltcl_stream_resolve DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_RESOLV'.
    METHODS setup.
    METHODS teardown.
    METHODS ref_chain_resolves FOR TESTING RAISING cx_static_check.
    METHODS ofs_chain_resolves FOR TESTING RAISING cx_static_check.
    METHODS external_thin_base_resolves FOR TESTING RAISING cx_static_check.
    METHODS two_thin_bases_do_not_collide FOR TESTING RAISING cx_static_check.
    METHODS missing_base_raises FOR TESTING RAISING cx_static_check.
    "! F-2C-001 correction: a genuinely missing external base with a
    "! NON-BLANK iv_url must still escalate via retry_without_haves, not
    "! attempt a per-base HTTP completion fetch (complete_missing_base is
    "! now permanently disabled).
    METHODS missing_base_no_http_retry FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_stream_resolve IMPLEMENTATION.
  METHOD setup.
    ROLLBACK WORK.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    COMMIT WORK.
  ENDMETHOD.
  METHOD teardown.
    ROLLBACK WORK.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    COMMIT WORK.
  ENDMETHOD.

  METHOD ref_chain_resolves.
    " Ports ltcl_ref_delta=>chain_onto_later_unresolved onto the metadata-only
    " streaming contract: A (a REF_DELTA) declares a dependency on B's REAL
    " identity, but B is ITSELF still an unresolved REF_DELTA (positioned
    " after A) depending on a real, already-resolved blob C. A single
    " ascending sweep cannot resolve A on its first visit - resolve_streaming's
    " multi-pass fixpoint must resolve B first, then revisit A.
    DATA lt_meta  TYPE zcl_abapgit_ortec_pack_stream=>ty_meta_tt.
    DATA ls_meta  LIKE LINE OF lt_meta.
    DATA lv_pack_id TYPE zcl_abapgit_ortec_pack_stream=>ty_pack_id.
    DATA lv_b_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_c_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_a_delta TYPE xstring.
    DATA lv_b_delta TYPE xstring.
    DATA lv_c_data  TYPE xstring.

    lv_pack_id = mc_repo && '_CHAIN'.
    lv_c_data  = '4869'.   " "Hi"
    lv_c_sha   = zcl_abapgit_hash=>sha1_blob( lv_c_data ).
    lv_b_sha   = zcl_abapgit_hash=>sha1_blob( '486921' ). " "Hi!"
    lv_a_delta = '030490030121'. " applies to "Hi!" -> "Hi!!"
    lv_b_delta = '020390020121'. " applies to "Hi" -> "Hi!"

    " C: already-resolved plain blob, part of the SAME pack (a ct_meta row) -
    " this is what lets B find it via the in-pack sha1 index during Pass 1,
    " exactly like ltcl_ref_delta=>chain_onto_later_unresolved. (Making C
    " purely external/thin instead would combine two distinct scenarios -
    " an in-pack chain AND a thin base - that the ported algorithm does not
    " claim to solve together in one single-pass Pass 2; that combination
    " is out of scope here, matching the original resolve_all's own scope.)
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_c_sha iv_type = 'blob' iv_data = lv_c_data ).
    CLEAR ls_meta.
    ls_meta-obj_index   = 3.
    ls_meta-pack_offset = 20.
    ls_meta-obj_type    = 'blob'.
    ls_meta-sha1        = lv_c_sha.
    ls_meta-is_resolved = abap_true.
    APPEND ls_meta TO lt_meta.

    " A: unresolved REF_DELTA depending on B's real (not-yet-known) identity.
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = mc_repo && 'A' iv_type = 'ref_d' iv_data = lv_a_delta ).
    CLEAR ls_meta.
    ls_meta-obj_index = 1.
    ls_meta-pack_offset = 0.
    ls_meta-obj_type  = 'ref_d'.
    ls_meta-temp_key  = mc_repo && 'A'.
    ls_meta-delta_base = lv_b_sha.
    APPEND ls_meta TO lt_meta.

    " B: unresolved REF_DELTA depending on C's real identity, positioned AFTER A.
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = mc_repo && 'B' iv_type = 'ref_d' iv_data = lv_b_delta ).
    CLEAR ls_meta.
    ls_meta-obj_index = 2.
    ls_meta-pack_offset = 10.
    ls_meta-obj_type  = 'ref_d'.
    ls_meta-temp_key  = mc_repo && 'B'.
    ls_meta-delta_base = lv_c_sha.
    APPEND ls_meta TO lt_meta.

    zcl_abapgit_ortec_pack_stream=>resolve_streaming(
      EXPORTING iv_repo_key = mc_repo
                iv_pack_id  = lv_pack_id
      CHANGING  ct_meta     = lt_meta ).

    READ TABLE lt_meta INTO ls_meta WITH KEY obj_index = 1.
    cl_abap_unit_assert=>assert_equals( act = ls_meta-is_resolved exp = abap_true
      msg = 'A must resolve once B (its declared base) has itself been resolved' ).
    cl_abap_unit_assert=>assert_equals( act = ls_meta-sha1 exp = zcl_abapgit_hash=>sha1_blob( '48692121' )
      msg = 'A must resolve to "Hi!!"' ).

    READ TABLE lt_meta INTO ls_meta WITH KEY obj_index = 2.
    cl_abap_unit_assert=>assert_equals( act = ls_meta-is_resolved exp = abap_true
      msg = 'B must resolve against C' ).
    cl_abap_unit_assert=>assert_equals( act = ls_meta-sha1 exp = lv_b_sha
      msg = 'B must resolve to exactly its declared, pre-computed identity "Hi!"' ).

    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = mc_repo iv_sha1 = lv_b_sha )
      msg = 'B''s resolved bytes must be persisted under its real sha1' ).
  ENDMETHOD.

  METHOD ofs_chain_resolves.
    " Ports ltcl_ofs_delta=>resolve_ofs_chain: obj1 "Hello" -> obj2 "Hello!"
    " -> obj3 "Hello!!", proving base_offset-based lookups resolve a chain in
    " dependency order even though obj3's base (obj2) is unresolved when
    " obj3 is first visited.
    DATA lt_meta    TYPE zcl_abapgit_ortec_pack_stream=>ty_meta_tt.
    DATA ls_meta    LIKE LINE OF lt_meta.
    DATA lv_pack_id TYPE zcl_abapgit_ortec_pack_stream=>ty_pack_id.
    DATA lv_hello_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    lv_pack_id  = mc_repo && '_OFSCH'.
    lv_hello_sha = zcl_abapgit_hash=>sha1_blob( '48656C6C6F' ).

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_hello_sha iv_type = 'blob' iv_data = '48656C6C6F' ).
    CLEAR ls_meta.
    ls_meta-obj_index   = 1.
    ls_meta-pack_offset = 0.
    ls_meta-obj_type    = 'blob'.
    ls_meta-sha1        = lv_hello_sha.
    ls_meta-is_resolved = abap_true.
    APPEND ls_meta TO lt_meta.

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = mc_repo && 'OFS2' iv_type = 'ofs_d' iv_data = '050690050121' ).
    CLEAR ls_meta.
    ls_meta-obj_index   = 2.
    ls_meta-pack_offset = 20.
    ls_meta-obj_type    = 'ofs_d'.
    ls_meta-temp_key    = mc_repo && 'OFS2'.
    ls_meta-base_offset = 0.
    APPEND ls_meta TO lt_meta.

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = mc_repo && 'OFS3' iv_type = 'ofs_d' iv_data = '060790060121' ).
    CLEAR ls_meta.
    ls_meta-obj_index   = 3.
    ls_meta-pack_offset = 40.
    ls_meta-obj_type    = 'ofs_d'.
    ls_meta-temp_key    = mc_repo && 'OFS3'.
    ls_meta-base_offset = 20.
    APPEND ls_meta TO lt_meta.

    zcl_abapgit_ortec_pack_stream=>resolve_streaming(
      EXPORTING iv_repo_key = mc_repo
                iv_pack_id  = lv_pack_id
      CHANGING  ct_meta     = lt_meta ).

    READ TABLE lt_meta INTO ls_meta WITH KEY obj_index = 2.
    cl_abap_unit_assert=>assert_equals( act = ls_meta-is_resolved exp = abap_true ).
    cl_abap_unit_assert=>assert_equals( act = ls_meta-sha1 exp = zcl_abapgit_hash=>sha1_blob( '48656C6C6F21' )
      msg = 'First OFS hop must resolve to "Hello!"' ).

    READ TABLE lt_meta INTO ls_meta WITH KEY obj_index = 3.
    cl_abap_unit_assert=>assert_equals( act = ls_meta-is_resolved exp = abap_true ).
    cl_abap_unit_assert=>assert_equals( act = ls_meta-sha1 exp = zcl_abapgit_hash=>sha1_blob( '48656C6C6F2121' )
      msg = 'Second OFS hop must resolve to "Hello!!", proving obj2 was resolved before obj3 applied' ).
  ENDMETHOD.

  METHOD external_thin_base_resolves.
    " A REF_DELTA whose declared base is not represented by any ct_meta row
    " at all - genuinely external, already-resolved data from a prior
    " pull/pack, fetched directly from zaog_obj_store (and warmed into the
    " Phase 1 LRU base cache) rather than found in-pack.
    DATA lt_meta    TYPE zcl_abapgit_ortec_pack_stream=>ty_meta_tt.
    DATA ls_meta    LIKE LINE OF lt_meta.
    DATA lv_pack_id TYPE zcl_abapgit_ortec_pack_stream=>ty_pack_id.
    DATA lv_base_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    lv_pack_id  = mc_repo && '_THIN1'.
    lv_base_sha = zcl_abapgit_hash=>sha1_blob( '41414141' ). " "AAAA", external

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_base_sha iv_type = 'blob' iv_data = '41414141' ).

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = mc_repo && 'EXT1' iv_type = 'ref_d' iv_data = '040590040121' ).
    CLEAR ls_meta.
    ls_meta-obj_index   = 1.
    ls_meta-pack_offset = 0.
    ls_meta-obj_type    = 'ref_d'.
    ls_meta-temp_key    = mc_repo && 'EXT1'.
    ls_meta-delta_base  = lv_base_sha.
    APPEND ls_meta TO lt_meta.

    zcl_abapgit_ortec_pack_stream=>resolve_streaming(
      EXPORTING iv_repo_key = mc_repo
                iv_pack_id  = lv_pack_id
      CHANGING  ct_meta     = lt_meta ).

    READ TABLE lt_meta INTO ls_meta WITH KEY obj_index = 1.
    cl_abap_unit_assert=>assert_equals( act = ls_meta-is_resolved exp = abap_true ).
    cl_abap_unit_assert=>assert_equals( act = ls_meta-sha1 exp = zcl_abapgit_hash=>sha1_blob( '4141414121' )
      msg = 'The delta must resolve against its genuinely external base "AAAA"' ).
  ENDMETHOD.

  METHOD two_thin_bases_do_not_collide.
    " Ports ltcl_ref_delta=>two_thin_bases_do_not_collide: two separate
    " REF_DELTA rows, each depending on a DIFFERENT external base fetched
    " from the object store (and the Phase 1 LRU cache) in the same resolve
    " pass - guards against any accidental key collision between the two
    " independent get_base_bytes calls.
    DATA lt_meta     TYPE zcl_abapgit_ortec_pack_stream=>ty_meta_tt.
    DATA ls_meta     LIKE LINE OF lt_meta.
    DATA lv_pack_id  TYPE zcl_abapgit_ortec_pack_stream=>ty_pack_id.
    DATA lv_base1_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_base2_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    lv_pack_id   = mc_repo && '_THIN2'.
    lv_base1_sha = zcl_abapgit_hash=>sha1_blob( '41414141' ). " "AAAA"
    lv_base2_sha = zcl_abapgit_hash=>sha1_blob( '42424242' ). " "BBBB"

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_base1_sha iv_type = 'blob' iv_data = '41414141' ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_base2_sha iv_type = 'blob' iv_data = '42424242' ).

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = mc_repo && 'TW1' iv_type = 'ref_d' iv_data = '040590040121' ).
    CLEAR ls_meta.
    ls_meta-obj_index   = 1.
    ls_meta-pack_offset = 0.
    ls_meta-obj_type    = 'ref_d'.
    ls_meta-temp_key    = mc_repo && 'TW1'.
    ls_meta-delta_base  = lv_base1_sha.
    APPEND ls_meta TO lt_meta.

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = mc_repo && 'TW2' iv_type = 'ref_d' iv_data = '040590040121' ).
    CLEAR ls_meta.
    ls_meta-obj_index   = 2.
    ls_meta-pack_offset = 10.
    ls_meta-obj_type    = 'ref_d'.
    ls_meta-temp_key    = mc_repo && 'TW2'.
    ls_meta-delta_base  = lv_base2_sha.
    APPEND ls_meta TO lt_meta.

    zcl_abapgit_ortec_pack_stream=>resolve_streaming(
      EXPORTING iv_repo_key = mc_repo
                iv_pack_id  = lv_pack_id
      CHANGING  ct_meta     = lt_meta ).

    READ TABLE lt_meta INTO ls_meta WITH KEY obj_index = 1.
    cl_abap_unit_assert=>assert_equals( act = ls_meta-sha1 exp = zcl_abapgit_hash=>sha1_blob( '4141414121' )
      msg = 'The first delta must resolve against its own thin base "AAAA", not the second' ).

    READ TABLE lt_meta INTO ls_meta WITH KEY obj_index = 2.
    cl_abap_unit_assert=>assert_equals( act = ls_meta-sha1 exp = zcl_abapgit_hash=>sha1_blob( '4242424221' )
      msg = 'The second delta must resolve against its own thin base "BBBB", not the first' ).
  ENDMETHOD.

  METHOD missing_base_raises.
    DATA lt_meta    TYPE zcl_abapgit_ortec_pack_stream=>ty_meta_tt.
    DATA ls_meta    LIKE LINE OF lt_meta.
    DATA lv_pack_id TYPE zcl_abapgit_ortec_pack_stream=>ty_pack_id.
    DATA lv_caught  TYPE abap_bool.

    lv_pack_id = mc_repo && '_MISS'.

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = mc_repo && 'MISS1' iv_type = 'ref_d' iv_data = '040590040121' ).
    CLEAR ls_meta.
    ls_meta-obj_index   = 1.
    ls_meta-pack_offset = 0.
    ls_meta-obj_type    = 'ref_d'.
    ls_meta-temp_key    = mc_repo && 'MISS1'.
    ls_meta-delta_base  = 'ffffffffffffffffffffffffffffffffffffffff'.
    APPEND ls_meta TO lt_meta.

    lv_caught = abap_false.
    TRY.
        zcl_abapgit_ortec_pack_stream=>resolve_streaming(
          EXPORTING iv_repo_key = mc_repo
                    iv_pack_id  = lv_pack_id
          CHANGING  ct_meta     = lt_meta ).
      CATCH zcx_abapgit_ortec_git.
        lv_caught = abap_true.
    ENDTRY.
    cl_abap_unit_assert=>assert_true( act = lv_caught msg = 'A genuinely missing base must raise zcx_abapgit_ortec_git' ).
  ENDMETHOD.

  METHOD missing_base_no_http_retry.
    " Before F-2C-001's correction, a non-blank iv_url here would have made
    " get_base_bytes attempt a real, targeted MATERIALIZE_BLOBS HTTP
    " request for this single missing sha1 (zcl_abapgit_ortec_pack_stream=>
    " complete_missing_base). That per-base HTTP repair is now permanently
    " disabled (always returns rv_attempted = abap_false without ever
    " calling zcl_abapgit_ortec_fastpath=>complete_missing_object), so this
    " test can safely pass an obviously unreachable URL: if the disabled
    " call were ever reactivated by accident, this test would fail/hang on
    " a real network attempt instead of completing immediately with the
    " expected retry_without_haves signal.
    DATA lt_meta    TYPE zcl_abapgit_ortec_pack_stream=>ty_meta_tt.
    DATA ls_meta    LIKE LINE OF lt_meta.
    DATA lv_pack_id TYPE zcl_abapgit_ortec_pack_stream=>ty_pack_id.
    DATA lx_missing TYPE REF TO zcx_abapgit_ortec_git.

    lv_pack_id = mc_repo && '_MISSU'.

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = mc_repo && 'MISSU1' iv_type = 'ref_d' iv_data = '040590040121' ).
    CLEAR ls_meta.
    ls_meta-obj_index   = 1.
    ls_meta-pack_offset = 0.
    ls_meta-obj_type    = 'ref_d'.
    ls_meta-temp_key    = mc_repo && 'MISSU1'.
    ls_meta-delta_base  = 'ffffffffffffffffffffffffffffffffffffffff'.
    APPEND ls_meta TO lt_meta.

    TRY.
        zcl_abapgit_ortec_pack_stream=>resolve_streaming(
          EXPORTING iv_repo_key = mc_repo
                    iv_pack_id  = lv_pack_id
                    iv_url      = 'http://unit-test.invalid/should-not-be-called.git'
          CHANGING  ct_meta     = lt_meta ).
        cl_abap_unit_assert=>fail( 'A genuinely missing external base must raise' ).
      CATCH zcx_abapgit_ortec_git INTO lx_missing.
        cl_abap_unit_assert=>assert_equals(
          act = lx_missing->mv_retry_without_haves
          exp = abap_true
          msg = 'A missing base with a non-blank URL must still signal retry_without_haves, ' &&
                'not attempt a per-base HTTP completion fetch' ).
    ENDTRY.
  ENDMETHOD.
ENDCLASS.

CLASS ltcl_ortec_git_exception DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    "! Regression: zcx_abapgit_ortec_git never populates the T100 message
    "! infrastructure (if_t100_message~t100key is left blank), so without a
    "! get_text( ) override the inherited cx_root default returns generic/
    "! empty text instead of mv_text - confirmed live via a blank
    "! "thin: , non-thin: " cascade-failure message reaching Michael despite
    "! both underlying exceptions having real text set via raise( iv_text ).
    METHODS get_text_returns_mv_text FOR TESTING RAISING cx_static_check.
    "! Regression: by the time an exception reaches a CATCH block or a
    "! debugger breakpoint, the call stack that led to RAISE EXCEPTION
    "! (always inside this class's own static raise( ) method) has already
    "! unwound - without capturing it at construction time, the only
    "! inspectable "source position" points inside raise( ) itself, never
    "! the actual calling code that decided to raise. Asserts the filtered
    "! call stack's top frame is THIS test method (not RAISE or
    "! CONSTRUCTOR), proving the class's own frames were correctly removed.
    METHODS source_position_to_caller FOR TESTING RAISING cx_static_check.
    "! Regression: serve_cached_when_nothing_new's failures previously gave
    "! upload_pack_by_branch/_by_commit no way to know a fresh, haves-free
    "! retry might succeed where the server's "nothing new" claim disagreed
    "! with the local cache - confirmed live across multiple objects/call
    "! paths. Asserts raise( iv_retry_without_haves = abap_true ) is
    "! correctly readable back off the caught exception instance.
    METHODS retry_without_haves_flag_set FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_ortec_git_exception IMPLEMENTATION.
  METHOD get_text_returns_mv_text.
    DATA lx_direct TYPE REF TO zcx_abapgit_ortec_git.
    TRY.
        zcx_abapgit_ortec_git=>raise( 'a specific, non-generic failure detail' ).
      CATCH zcx_abapgit_ortec_git INTO lx_direct.
    ENDTRY.
    cl_abap_unit_assert=>assert_equals(
      act = lx_direct->get_text( )
      exp = 'a specific, non-generic failure detail'
      msg = 'get_text( ) must return mv_text, not a generic/blank cx_root default' ).
  ENDMETHOD.
  METHOD source_position_to_caller.
    DATA lx_direct TYPE REF TO zcx_abapgit_ortec_git.
    DATA lv_program_name TYPE progname.
    DATA lv_include_name TYPE progname.
    DATA lv_source_line  TYPE i.

    TRY.
        zcx_abapgit_ortec_git=>raise( 'source position regression check' ).
      CATCH zcx_abapgit_ortec_git INTO lx_direct.
    ENDTRY.

    cl_abap_unit_assert=>assert_not_initial(
      act = lx_direct->mt_callstack
      msg = 'mt_callstack must be captured at raise time' ).
    cl_abap_unit_assert=>assert_equals(
      act = lx_direct->mt_callstack[ 1 ]-blockname
      exp = 'SOURCE_POSITION_TO_CALLER'
      msg = 'The top of the filtered call stack must be the actual caller, ' &&
            'not RAISE or CONSTRUCTOR from inside zcx_abapgit_ortec_git itself' ).

    cl_abap_unit_assert=>assert_not_initial(
      act = lx_direct->ms_src_info-line
      msg = 'ms_src_info must be populated at construction time so it ' &&
            'survives a debugger breakpoint after the stack has unwound' ).

    lx_direct->get_source_position(
      IMPORTING
        program_name = lv_program_name
        include_name = lv_include_name
        source_line  = lv_source_line ).
    cl_abap_unit_assert=>assert_equals(
      act = lv_source_line
      exp = lx_direct->ms_src_info-line
      msg = 'get_source_position( ) must be consistent with ms_src_info' ).
  ENDMETHOD.
  METHOD retry_without_haves_flag_set.
    DATA lx_default TYPE REF TO zcx_abapgit_ortec_git.
    DATA lx_retryable TYPE REF TO zcx_abapgit_ortec_git.

    TRY.
        zcx_abapgit_ortec_git=>raise( 'not retryable by default' ).
      CATCH zcx_abapgit_ortec_git INTO lx_default.
    ENDTRY.
    cl_abap_unit_assert=>assert_equals(
      act = lx_default->mv_retry_without_haves
      exp = abap_false
      msg = 'mv_retry_without_haves must default to false when not passed' ).

    TRY.
        zcx_abapgit_ortec_git=>raise(
          iv_text                = 'retryable failure'
          iv_retry_without_haves = abap_true ).
      CATCH zcx_abapgit_ortec_git INTO lx_retryable.
    ENDTRY.
    cl_abap_unit_assert=>assert_equals(
      act = lx_retryable->mv_retry_without_haves
      exp = abap_true
      msg = 'mv_retry_without_haves must be readable back off the caught exception' ).
  ENDMETHOD.
ENDCLASS.

CLASS ltcl_spike_a_db_base_parity DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_SPIKEA'.
    METHODS setup.
    METHODS teardown.
    "! Streaming-decoder design Spike A (see .memory/state.md "Live crash
    "! confirms..." entry / streaming decoder open question 8): proves that
    "! zcl_abapgit_ortec_delta=>apply's result is identical whether its base
    "! object's bytes come from an in-memory literal or are freshly read
    "! back from zaog_obj_store via zcl_abapgit_ortec_obj_store=>get_object -
    "! the exact substitution the streaming resolver design depends on
    "! (DB-backed bases instead of a shared in-memory ct_objects table).
    "! apply() is a pure function (IMPORTING iv_base/iv_delta TYPE xstring,
    "! no shared-state dependency), so this also incidentally proves the
    "! store/get_object round-trip preserves bytes exactly - if it didn't,
    "! this would be the first place to notice.
    METHODS db_base_matches_in_memory FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_spike_a_db_base_parity IMPLEMENTATION.
  METHOD setup.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
  ENDMETHOD.
  METHOD teardown.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    ROLLBACK WORK.
  ENDMETHOD.

  METHOD db_base_matches_in_memory.
    " Same hand-verified vector already used elsewhere in this test file
    " (ltcl_ref_delta): base "Hello!" (6 bytes) + delta 060790060121
    " (copy 6 bytes from offset 0, then insert literal '!') must produce
    " "Hello!!" (7 bytes).
    DATA lv_base_in_memory TYPE xstring VALUE '48656C6C6F21'.
    DATA lv_base_from_db   TYPE xstring.
    DATA lv_delta          TYPE xstring VALUE '060790060121'.
    DATA lv_result_memory  TYPE xstring.
    DATA lv_result_db      TYPE xstring.
    DATA lv_base_sha       TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA ls_object         TYPE zif_abapgit_definitions=>ty_object.

    lv_base_sha = zcl_abapgit_hash=>sha1_blob( lv_base_in_memory ).

    " Persist the SAME base bytes for real, through the actual production
    " persistence path (store_object), then read them back through the
    " actual production read path (get_object) - not a shortcut/mock.
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_base_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_base_in_memory ).

    ls_object = zcl_abapgit_ortec_obj_store=>get_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_base_sha ).
    lv_base_from_db = ls_object-data.

    cl_abap_unit_assert=>assert_equals(
      act = lv_base_from_db
      exp = lv_base_in_memory
      msg = 'store_object/get_object round-trip must preserve base bytes exactly - ' &&
            'the streaming resolver design depends on this' ).

    lv_result_memory = zcl_abapgit_ortec_delta=>apply(
      iv_base  = lv_base_in_memory
      iv_delta = lv_delta ).

    lv_result_db = zcl_abapgit_ortec_delta=>apply(
      iv_base  = lv_base_from_db
      iv_delta = lv_delta ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_result_memory
      exp = '48656C6C6F2121'
      msg = 'Sanity check: applying the known-good vector against the in-memory ' &&
            'base must produce the expected "Hello!!" result' ).

    cl_abap_unit_assert=>assert_equals(
      act = lv_result_db
      exp = lv_result_memory
      msg = 'SPIKE A: apply() must produce an IDENTICAL result whether its base ' &&
            'came from an in-memory literal or was freshly read back from ' &&
            'zaog_obj_store via get_object - this is the core assumption the ' &&
            'streaming delta-resolver redesign depends on (DB-backed bases ' &&
            'instead of a shared in-memory ct_objects table)' ).
  ENDMETHOD.
ENDCLASS.

CLASS ltcl_git_roundtrip DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION. METHODS encode_decode FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_git_roundtrip IMPLEMENTATION.
  METHOD encode_decode.
    DATA lt_obj TYPE zif_abapgit_definitions=>ty_objects_tt. DATA ls_obj TYPE zif_abapgit_definitions=>ty_object.
    ls_obj-sha1 = zcl_abapgit_hash=>sha1_blob( CONV xstring( '48656C6C6F' ) ).
    ls_obj-type = 'blob'. ls_obj-data = '48656C6C6F'. ls_obj-index = 1. APPEND ls_obj TO lt_obj.
    DATA lv_pack TYPE xstring.
    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).
    cl_abap_unit_assert=>assert_not_initial( act = lv_pack ).
    DATA lt_dec TYPE zif_abapgit_definitions=>ty_objects_tt.
    lt_dec = zcl_abapgit_git_pack=>decode( lv_pack ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_dec ) exp = 1 ).
    cl_abap_unit_assert=>assert_equals( act = lt_dec[ 1 ]-sha1 exp = ls_obj-sha1 ).
    cl_abap_unit_assert=>assert_equals( act = lt_dec[ 1 ]-data exp = '48656C6C6F' ).
  ENDMETHOD.
ENDCLASS.
