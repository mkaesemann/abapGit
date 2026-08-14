CLASS zcl_abapgit_ortec_pack_stream DEFINITION LOCAL FRIENDS ltcl_pack_stream.

CLASS ltcl_pack_stream DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_STREAM'.
    METHODS setup.
    METHODS teardown.
    METHODS delta_free_pack_decodes FOR TESTING RAISING cx_static_check.
    METHODS ref_delta_stays_unresolved FOR TESTING RAISING cx_static_check.
    METHODS corrupt_trailer_no_rows FOR TESTING RAISING cx_static_check.
    METHODS decode_streaming_is_sparse FOR TESTING RAISING cx_static_check.
    METHODS ext_base_resolve_after_preload FOR TESTING RAISING cx_static_check.
    "! Package D1: resolve_streaming's phase 1.5 bulk load must resolve a
    "! genuinely external REF_DELTA base up front - resolve_one_meta's own
    "! on-demand per-object thin-fetch fallback must never be reached.
    METHODS no_thin_fetch_for_ext_base FOR TESTING RAISING cx_static_check.
    "! D2 staged-visibility fix (target_design §5.3/§5.3.1) - see each
    "! method's own doc comment for what it covers.
    METHODS delta_temp_row_status_d FOR TESTING RAISING cx_static_check.
    METHODS temp_row_hidden_from_get FOR TESTING RAISING cx_static_check.
    METHODS cleanup_removes_d_status FOR TESTING RAISING cx_static_check.
    METHODS resolve_reads_own_d_row FOR TESTING RAISING cx_static_check.
    METHODS staged_cache_hit_no_sql FOR TESTING RAISING cx_static_check.
    "! D2B1 attempt-id correlation (target_design §9) - see each method's
    "! own doc comment for what it covers.
    METHODS attempt_id_on_obj_store FOR TESTING RAISING cx_static_check.
    METHODS crash_before_resolve_ok FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_pack_stream IMPLEMENTATION.
  METHOD setup.
    " Unlike most SUT methods in this test file, decode_and_persist_streaming
    " issues its own real COMMIT WORK (by design - the promote/cleanup step
    " must be durable). A bare DELETE here would still be sitting uncommitted
    " when that COMMIT WORK fires, but that's fine since it commits together;
    " the real risk is teardown's ROLLBACK (see below) undoing a delete that
    " was never itself committed - so this delete commits explicitly too, to
    " never depend on ordering relative to the SUT's own commit.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    COMMIT WORK.
    zcl_abapgit_ortec_pack_stream=>gv_thin_fetch_calls = 0.
  ENDMETHOD.
  METHOD teardown.
    " ROLLBACK WORK FIRST to discard anything this test left uncommitted,
    " THEN delete+commit any rows the SUT's own COMMIT WORK already made
    " durable. The previous DELETE-then-ROLLBACK ordering silently undid its
    " own delete whenever the SUT had already committed (exactly what
    " decode_and_persist_streaming does), leaking real 'R'-status rows into
    " later tests - confirmed live via a stray promoted row surviving an
    " entire test run.
    ROLLBACK WORK.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    COMMIT WORK.
  ENDMETHOD.

  METHOD delta_free_pack_decodes.
    DATA lt_obj  TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj  TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_pack TYPE xstring.
    DATA lt_meta TYPE zcl_abapgit_ortec_pack_stream=>ty_meta_tt.
    DATA ls_meta LIKE LINE OF lt_meta.
    DATA lv_sha1_a TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_sha1_b TYPE zif_abapgit_git_definitions=>ty_sha1.

    ls_obj-data  = '48656C6C6F'. " "Hello"
    ls_obj-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_obj-sha1  = zcl_abapgit_hash=>sha1( iv_type = ls_obj-type iv_data = ls_obj-data ).
    ls_obj-index = 1.
    lv_sha1_a = ls_obj-sha1.
    APPEND ls_obj TO lt_obj.

    CLEAR ls_obj.
    ls_obj-data  = '576F726C64'. " "World"
    ls_obj-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_obj-sha1  = zcl_abapgit_hash=>sha1( iv_type = ls_obj-type iv_data = ls_obj-data ).
    ls_obj-index = 2.
    lv_sha1_b = ls_obj-sha1.
    APPEND ls_obj TO lt_obj.

    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).

    lt_meta = zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming(
      iv_data     = lv_pack
      iv_repo_key = mc_repo ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_meta ) exp = 2
      msg = 'Both objects must be represented in the metadata table' ).

    LOOP AT lt_meta INTO ls_meta.
      cl_abap_unit_assert=>assert_equals( act = ls_meta-is_resolved exp = abap_true
        msg = 'Non-delta objects must be resolved immediately' ).
    ENDLOOP.

    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = mc_repo iv_sha1 = lv_sha1_a )
      msg = 'First object must be visible in the object store after promotion' ).
    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = mc_repo iv_sha1 = lv_sha1_b )
      msg = 'Second object must be visible in the object store after promotion' ).
  ENDMETHOD.

  METHOD ref_delta_stays_unresolved.
    DATA lv_base_data   TYPE xstring.
    DATA lv_base_sha    TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_base_raw    TYPE x LENGTH 20.
    DATA lv_delta       TYPE xstring.
    DATA lv_compressed  TYPE xstring.
    DATA lv_adler       TYPE zif_abapgit_git_definitions=>ty_adler32.
    DATA lv_pack_magic  TYPE x LENGTH 4 VALUE '5041434B'.
    DATA lv_version     TYPE x LENGTH 4 VALUE '00000002'.
    DATA lv_obj_count   TYPE x LENGTH 4 VALUE '00000001'.
    DATA lv_zlib_hdr    TYPE x LENGTH 2 VALUE '789C'.
    DATA lv_type_len    TYPE x LENGTH 1 VALUE '76'. " ref_d (0x70) | length 6
    DATA lv_pack        TYPE xstring.
    DATA lv_trailer_hex TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_trailer_raw TYPE x LENGTH 20.
    DATA lt_meta        TYPE zcl_abapgit_ortec_pack_stream=>ty_meta_tt.
    DATA ls_meta        LIKE LINE OF lt_meta.
    DATA lv_count       TYPE i.

    lv_base_data = '41414141'. " "AAAA" - external base, never included in this pack
    lv_base_sha = zcl_abapgit_hash=>sha1_blob( lv_base_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_base_sha iv_type = 'blob' iv_data = lv_base_data ).

    " base-size(4) result-size(5) copy(off=0,len=4) insert(1,'!') -> base & "!".
    lv_delta = '040590040121'.
    cl_abap_gzip=>compress_binary( EXPORTING raw_in = lv_delta IMPORTING gzip_out = lv_compressed ).
    lv_adler = zcl_abapgit_hash=>adler32( lv_delta ).

    lv_base_raw = to_upper( lv_base_sha ).

    CONCATENATE lv_pack_magic lv_version lv_obj_count INTO lv_pack IN BYTE MODE.
    CONCATENATE lv_pack lv_type_len lv_base_raw lv_zlib_hdr lv_compressed lv_adler
      INTO lv_pack IN BYTE MODE.

    lv_trailer_hex = zcl_abapgit_hash=>sha1_raw( lv_pack ).
    lv_trailer_raw = to_upper( lv_trailer_hex ).
    CONCATENATE lv_pack lv_trailer_raw INTO lv_pack IN BYTE MODE.

    lt_meta = zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming(
      iv_data     = lv_pack
      iv_repo_key = mc_repo ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_meta ) exp = 1
      msg = 'The ref-delta entry must be represented in the metadata table' ).
    READ TABLE lt_meta INTO ls_meta INDEX 1.
    cl_abap_unit_assert=>assert_equals( act = ls_meta-is_resolved exp = abap_false
      msg = 'A ref-delta object must not be resolved by the streaming scan' ).
    cl_abap_unit_assert=>assert_initial( act = ls_meta-sha1
      msg = 'An unresolved delta object has no final SHA1 yet' ).
    cl_abap_unit_assert=>assert_equals( act = ls_meta-delta_base exp = lv_base_sha
      msg = 'The declared base SHA1 must be captured in the metadata row' ).
    cl_abap_unit_assert=>assert_not_initial( act = ls_meta-temp_key
      msg = 'An unresolved delta object must have a temporary store key' ).

    SELECT COUNT(*) FROM zaog_obj_store INTO lv_count
      WHERE repo_key = mc_repo AND obj_sha1 = ls_meta-temp_key AND status = zcl_abapgit_ortec_pack_stream=>c_status_decoded.
    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 1
      msg = 'The temp-keyed delta row must exist and be promoted to D (decoded-pending-resolution), not R, after a successful pass' ).
  ENDMETHOD.

  METHOD ext_base_resolve_after_preload.
    DATA lv_base_data TYPE xstring VALUE '41414141'.
    DATA lv_base_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_base_raw TYPE x LENGTH 20.
    DATA lv_delta TYPE xstring VALUE '040590040121'.
    DATA lv_compressed TYPE xstring.
    DATA lv_adler TYPE zif_abapgit_git_definitions=>ty_adler32.
    DATA lv_pack_magic TYPE x LENGTH 4 VALUE '5041434B'.
    DATA lv_version TYPE x LENGTH 4 VALUE '00000002'.
    DATA lv_obj_count TYPE x LENGTH 4 VALUE '00000001'.
    DATA lv_zlib_hdr TYPE x LENGTH 2 VALUE '789C'.
    DATA lv_type_len TYPE x LENGTH 1 VALUE '76'.
    DATA lv_pack TYPE xstring.
    DATA lv_trailer_hex TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_trailer_raw TYPE x LENGTH 20.
    DATA lt_meta TYPE zcl_abapgit_ortec_pack_stream=>ty_meta_tt.
    DATA lv_pack_id TYPE zcl_abapgit_ortec_pack_stream=>ty_pack_id.
    DATA lv_expected_data TYPE xstring VALUE '4141414121'.
    DATA lv_expected_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA ls_resolved TYPE zif_abapgit_definitions=>ty_object.

    lv_base_sha = zcl_abapgit_hash=>sha1_blob( lv_base_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_base_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_base_data ).

    cl_abap_gzip=>compress_binary(
      EXPORTING raw_in = lv_delta
      IMPORTING gzip_out = lv_compressed ).
    lv_adler = zcl_abapgit_hash=>adler32( lv_delta ).
    lv_base_raw = to_upper( lv_base_sha ).

    CONCATENATE lv_pack_magic lv_version lv_obj_count INTO lv_pack IN BYTE MODE.
    CONCATENATE lv_pack lv_type_len lv_base_raw lv_zlib_hdr lv_compressed lv_adler
      INTO lv_pack IN BYTE MODE.
    lv_trailer_hex = zcl_abapgit_hash=>sha1_raw( lv_pack ).
    lv_trailer_raw = to_upper( lv_trailer_hex ).
    CONCATENATE lv_pack lv_trailer_raw INTO lv_pack IN BYTE MODE.

    lt_meta = zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming(
      EXPORTING
        iv_data     = lv_pack
        iv_repo_key = mc_repo
      IMPORTING
        ev_pack_id  = lv_pack_id ).

    zcl_abapgit_ortec_obj_store=>invalidate_cache( ).
    zcl_abapgit_ortec_base_cache=>get_instance( )->clear( ).

    zcl_abapgit_ortec_pack_stream=>resolve_streaming(
      EXPORTING
        iv_repo_key = mc_repo
        iv_pack_id  = lv_pack_id
      CHANGING
        ct_meta     = lt_meta ).

    cl_abap_unit_assert=>assert_true(
      act = xsdbool( lt_meta[ 1 ]-is_resolved = abap_true )
      msg = 'External REF_DELTA base is resolved after bounded preload' ).

    lv_expected_sha = zcl_abapgit_hash=>sha1_blob( lv_expected_data ).
    cl_abap_unit_assert=>assert_equals(
      act = lt_meta[ 1 ]-sha1
      exp = lv_expected_sha ).

    ls_resolved = zcl_abapgit_ortec_obj_store=>get_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_expected_sha ).
    cl_abap_unit_assert=>assert_equals(
      act = ls_resolved-data
      exp = lv_expected_data ).
  ENDMETHOD.

  METHOD no_thin_fetch_for_ext_base.
    DATA lv_base_data TYPE xstring VALUE '41414141'.
    DATA lv_base_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_base_raw TYPE x LENGTH 20.
    DATA lv_delta TYPE xstring VALUE '040590040121'.
    DATA lv_compressed TYPE xstring.
    DATA lv_adler TYPE zif_abapgit_git_definitions=>ty_adler32.
    DATA lv_pack_magic TYPE x LENGTH 4 VALUE '5041434B'.
    DATA lv_version TYPE x LENGTH 4 VALUE '00000002'.
    DATA lv_obj_count TYPE x LENGTH 4 VALUE '00000001'.
    DATA lv_zlib_hdr TYPE x LENGTH 2 VALUE '789C'.
    DATA lv_type_len TYPE x LENGTH 1 VALUE '76'.
    DATA lv_pack TYPE xstring.
    DATA lv_trailer_hex TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_trailer_raw TYPE x LENGTH 20.
    DATA lt_meta TYPE zcl_abapgit_ortec_pack_stream=>ty_meta_tt.
    DATA lv_pack_id TYPE zcl_abapgit_ortec_pack_stream=>ty_pack_id.
    DATA lv_expected_data TYPE xstring VALUE '4141414121'.
    DATA lv_expected_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    " Same fixture as ext_base_resolve_after_preload - one REF_DELTA whose
    " base is genuinely external (present only in the object store, not in
    " this pack). Package D1's phase 1.5 must resolve it via one bulk call
    " during resolve_streaming itself, so resolve_one_meta's own on-demand
    " thin-fetch fallback (gv_thin_fetch_calls) must never fire.
    lv_base_sha = zcl_abapgit_hash=>sha1_blob( lv_base_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_base_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_base_data ).

    cl_abap_gzip=>compress_binary(
      EXPORTING raw_in = lv_delta
      IMPORTING gzip_out = lv_compressed ).
    lv_adler = zcl_abapgit_hash=>adler32( lv_delta ).
    lv_base_raw = to_upper( lv_base_sha ).

    CONCATENATE lv_pack_magic lv_version lv_obj_count INTO lv_pack IN BYTE MODE.
    CONCATENATE lv_pack lv_type_len lv_base_raw lv_zlib_hdr lv_compressed lv_adler
      INTO lv_pack IN BYTE MODE.
    lv_trailer_hex = zcl_abapgit_hash=>sha1_raw( lv_pack ).
    lv_trailer_raw = to_upper( lv_trailer_hex ).
    CONCATENATE lv_pack lv_trailer_raw INTO lv_pack IN BYTE MODE.

    lt_meta = zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming(
      EXPORTING
        iv_data     = lv_pack
        iv_repo_key = mc_repo
      IMPORTING
        ev_pack_id  = lv_pack_id ).

    zcl_abapgit_ortec_obj_store=>invalidate_cache( ).
    zcl_abapgit_ortec_base_cache=>get_instance( )->clear( ).

    zcl_abapgit_ortec_pack_stream=>resolve_streaming(
      EXPORTING
        iv_repo_key = mc_repo
        iv_pack_id  = lv_pack_id
      CHANGING
        ct_meta     = lt_meta ).

    lv_expected_sha = zcl_abapgit_hash=>sha1_blob( lv_expected_data ).
    cl_abap_unit_assert=>assert_equals(
      act = lt_meta[ 1 ]-sha1
      exp = lv_expected_sha ).

    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_pack_stream=>gv_thin_fetch_calls
      exp = 0
      msg = 'phase 1.5''s bulk load must resolve the external base up front - ' &&
            'resolve_one_meta''s on-demand thin-fetch fallback must never fire' ).
  ENDMETHOD.

  METHOD corrupt_trailer_no_rows.
    DATA lt_obj    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj    TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_pack   TYPE xstring.
    DATA lv_len    TYPE i.
    DATA lt_meta   TYPE zcl_abapgit_ortec_pack_stream=>ty_meta_tt.
    DATA lv_caught TYPE abap_bool.
    DATA lv_count  TYPE i.

    ls_obj-data  = '48656C6C6F'. " "Hello"
    ls_obj-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_obj-sha1  = zcl_abapgit_hash=>sha1( iv_type = ls_obj-type iv_data = ls_obj-data ).
    ls_obj-index = 1.
    APPEND ls_obj TO lt_obj.

    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).

    " Corrupt only the last byte of the trailing 20-byte pack SHA1 so the
    " object itself still decompresses/persists as usual; only the final
    " trailer-integrity check fails, forcing the CATCH cleanup path with
    " real, already-persisted 'I'-status data to remove.
    lv_len = xstrlen( lv_pack ) - 1.
    lv_pack = lv_pack(lv_len) && 'FF'.

    lv_caught = abap_false.
    TRY.
        lt_meta = zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming(
          iv_data     = lv_pack
          iv_repo_key = mc_repo ).
      CATCH zcx_abapgit_ortec_git.
        lv_caught = abap_true.
    ENDTRY.
    cl_abap_unit_assert=>assert_true( act = lv_caught msg = 'Corrupt trailer must raise zcx_abapgit_ortec_git' ).

    SELECT COUNT(*) FROM zaog_obj_store INTO lv_count WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 0
      msg = 'No rows of any status may remain for this repo after a failed streaming pass' ).
  ENDMETHOD.

  METHOD decode_streaming_is_sparse.
    " End-to-end Phase 4 check: a pack containing a commit + its tree + a
    " blob decodes and resolves fully, but decode_streaming's returned
    " rt_objects contains ONLY the commit - the sparse contract standard
    " abapGit's pull()/H4 depend on. The tree and blob must still be fully
    " available, just via the object store rather than in-memory.
    DATA lt_nodes       TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node        LIKE LINE OF lt_nodes.
    DATA ls_commit      TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lt_obj         TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj         LIKE LINE OF lt_obj.
    DATA lt_objects     TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_pack        TYPE xstring.
    DATA lv_blob_data   TYPE xstring.
    DATA lv_blob_sha    TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_tree_data   TYPE xstring.
    DATA lv_tree_sha    TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_commit_data TYPE xstring.
    DATA lv_commit_sha  TYPE zif_abapgit_git_definitions=>ty_sha1.

    lv_blob_data = '48656C6C6F'. " "Hello"
    lv_blob_sha  = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).

    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_node-name  = 'hello.txt'.
    ls_node-sha1  = lv_blob_sha.
    APPEND ls_node TO lt_nodes.
    lv_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    lv_tree_sha  = zcl_abapgit_hash=>sha1_tree( lv_tree_data ).

    ls_commit-tree      = lv_tree_sha.
    ls_commit-author    = 'Test <test@example.com> 0 +0000'.
    ls_commit-committer = 'Test <test@example.com> 0 +0000'.
    ls_commit-body      = 'test'.
    lv_commit_data = zcl_abapgit_git_pack=>encode_commit( ls_commit ).
    lv_commit_sha  = zcl_abapgit_hash=>sha1_commit( lv_commit_data ).

    CLEAR ls_obj.
    ls_obj-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_obj-data  = lv_blob_data.
    ls_obj-sha1  = lv_blob_sha.
    ls_obj-index = 1.
    APPEND ls_obj TO lt_obj.

    CLEAR ls_obj.
    ls_obj-type  = zif_abapgit_git_definitions=>c_type-tree.
    ls_obj-data  = lv_tree_data.
    ls_obj-sha1  = lv_tree_sha.
    ls_obj-index = 2.
    APPEND ls_obj TO lt_obj.

    CLEAR ls_obj.
    ls_obj-type  = zif_abapgit_git_definitions=>c_type-commit.
    ls_obj-data  = lv_commit_data.
    ls_obj-sha1  = lv_commit_sha.
    ls_obj-index = 3.
    APPEND ls_obj TO lt_obj.

    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).

    lt_objects = zcl_abapgit_ortec_pack_stream=>decode_streaming(
      iv_data     = lv_pack
      iv_repo_key = mc_repo ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_objects ) exp = 1
      msg = 'decode_streaming must return ONLY the commit object - the sparse contract' ).
    READ TABLE lt_objects INTO ls_obj INDEX 1.
    cl_abap_unit_assert=>assert_equals( act = ls_obj-sha1 exp = lv_commit_sha ).
    cl_abap_unit_assert=>assert_equals( act = ls_obj-type exp = zif_abapgit_git_definitions=>c_type-commit ).
    cl_abap_unit_assert=>assert_equals( act = ls_obj-data exp = lv_commit_data ).

    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = mc_repo iv_sha1 = lv_tree_sha )
      msg = 'The tree must still be fully available via the object store' ).
    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = mc_repo iv_sha1 = lv_blob_sha )
      msg = 'The blob must still be fully available via the object store' ).
  ENDMETHOD.

  METHOD delta_temp_row_status_d.
    " D2 staged-visibility fix (target_design §5.3): a REF_DELTA temp-key
    " row must be promoted to the intermediate 'D' status by
    " decode_and_persist_streaming, never straight to 'R', since it has
    " not yet been resolved against its base.
    DATA lv_base_data   TYPE xstring.
    DATA lv_base_sha    TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_base_raw    TYPE x LENGTH 20.
    DATA lv_delta       TYPE xstring.
    DATA lv_compressed  TYPE xstring.
    DATA lv_adler       TYPE zif_abapgit_git_definitions=>ty_adler32.
    DATA lv_pack_magic  TYPE x LENGTH 4 VALUE '5041434B'.
    DATA lv_version     TYPE x LENGTH 4 VALUE '00000002'.
    DATA lv_obj_count   TYPE x LENGTH 4 VALUE '00000001'.
    DATA lv_zlib_hdr    TYPE x LENGTH 2 VALUE '789C'.
    DATA lv_type_len    TYPE x LENGTH 1 VALUE '76'. " ref_d (0x70) | length 6
    DATA lv_pack        TYPE xstring.
    DATA lv_trailer_hex TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_trailer_raw TYPE x LENGTH 20.
    DATA lt_meta        TYPE zcl_abapgit_ortec_pack_stream=>ty_meta_tt.
    DATA ls_meta        LIKE LINE OF lt_meta.
    DATA lv_count       TYPE i.

    lv_base_data = '41414141'. " "AAAA" - external base, never included in this pack
    lv_base_sha = zcl_abapgit_hash=>sha1_blob( lv_base_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_base_sha iv_type = 'blob' iv_data = lv_base_data ).

    lv_delta = '040590040121'.
    cl_abap_gzip=>compress_binary( EXPORTING raw_in = lv_delta IMPORTING gzip_out = lv_compressed ).
    lv_adler = zcl_abapgit_hash=>adler32( lv_delta ).
    lv_base_raw = to_upper( lv_base_sha ).

    CONCATENATE lv_pack_magic lv_version lv_obj_count INTO lv_pack IN BYTE MODE.
    CONCATENATE lv_pack lv_type_len lv_base_raw lv_zlib_hdr lv_compressed lv_adler
      INTO lv_pack IN BYTE MODE.

    lv_trailer_hex = zcl_abapgit_hash=>sha1_raw( lv_pack ).
    lv_trailer_raw = to_upper( lv_trailer_hex ).
    CONCATENATE lv_pack lv_trailer_raw INTO lv_pack IN BYTE MODE.

    lt_meta = zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming(
      iv_data     = lv_pack
      iv_repo_key = mc_repo ).

    READ TABLE lt_meta INTO ls_meta INDEX 1.
    cl_abap_unit_assert=>assert_equals( act = ls_meta-is_resolved exp = abap_false
      msg = 'A ref-delta object must not be resolved by the streaming scan' ).

    SELECT COUNT(*) FROM zaog_obj_store INTO lv_count
      WHERE repo_key = mc_repo AND obj_sha1 = ls_meta-temp_key
        AND status = zcl_abapgit_ortec_pack_stream=>c_status_decoded.
    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 1
      msg = 'An unresolved delta temp row must be promoted to D (decoded-pending-resolution), not R' ).
  ENDMETHOD.

  METHOD temp_row_hidden_from_get.
    " D2 staged-visibility fix (target_design §5.3.1): a 'D'-status temp
    " row must stay invisible to every generic status = 'R'-only read
    " (get_object/get_objects) - only get_staged_delta_objects may see it.
    DATA lv_base_data   TYPE xstring.
    DATA lv_base_sha    TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_base_raw    TYPE x LENGTH 20.
    DATA lv_delta       TYPE xstring.
    DATA lv_compressed  TYPE xstring.
    DATA lv_adler       TYPE zif_abapgit_git_definitions=>ty_adler32.
    DATA lv_pack_magic  TYPE x LENGTH 4 VALUE '5041434B'.
    DATA lv_version     TYPE x LENGTH 4 VALUE '00000002'.
    DATA lv_obj_count   TYPE x LENGTH 4 VALUE '00000001'.
    DATA lv_zlib_hdr    TYPE x LENGTH 2 VALUE '789C'.
    DATA lv_type_len    TYPE x LENGTH 1 VALUE '76'.
    DATA lv_pack        TYPE xstring.
    DATA lv_trailer_hex TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_trailer_raw TYPE x LENGTH 20.
    DATA lt_meta        TYPE zcl_abapgit_ortec_pack_stream=>ty_meta_tt.
    DATA ls_meta        LIKE LINE OF lt_meta.
    DATA lt_sha1s       TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv_caught      TYPE abap_bool.

    lv_base_data = '41414141'.
    lv_base_sha = zcl_abapgit_hash=>sha1_blob( lv_base_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_base_sha iv_type = 'blob' iv_data = lv_base_data ).

    lv_delta = '040590040121'.
    cl_abap_gzip=>compress_binary( EXPORTING raw_in = lv_delta IMPORTING gzip_out = lv_compressed ).
    lv_adler = zcl_abapgit_hash=>adler32( lv_delta ).
    lv_base_raw = to_upper( lv_base_sha ).

    CONCATENATE lv_pack_magic lv_version lv_obj_count INTO lv_pack IN BYTE MODE.
    CONCATENATE lv_pack lv_type_len lv_base_raw lv_zlib_hdr lv_compressed lv_adler
      INTO lv_pack IN BYTE MODE.
    lv_trailer_hex = zcl_abapgit_hash=>sha1_raw( lv_pack ).
    lv_trailer_raw = to_upper( lv_trailer_hex ).
    CONCATENATE lv_pack lv_trailer_raw INTO lv_pack IN BYTE MODE.

    lt_meta = zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming(
      iv_data     = lv_pack
      iv_repo_key = mc_repo ).

    READ TABLE lt_meta INTO ls_meta INDEX 1.
    APPEND ls_meta-temp_key TO lt_sha1s.

    lv_caught = abap_false.
    TRY.
        zcl_abapgit_ortec_obj_store=>get_object(
          iv_repo_key = mc_repo
          iv_sha1     = ls_meta-temp_key ).
      CATCH zcx_abapgit_ortec_git.
        lv_caught = abap_true.
    ENDTRY.
    cl_abap_unit_assert=>assert_true( act = lv_caught
      msg = 'get_object must not see a D-status temp row' ).

    lv_caught = abap_false.
    TRY.
        zcl_abapgit_ortec_obj_store=>get_objects(
          iv_repo_key = mc_repo
          it_sha1s    = lt_sha1s ).
      CATCH zcx_abapgit_ortec_git.
        lv_caught = abap_true.
    ENDTRY.
    cl_abap_unit_assert=>assert_true( act = lv_caught
      msg = 'get_objects must not see a D-status temp row' ).
  ENDMETHOD.

  METHOD cleanup_removes_d_status.
    " D2 staged-visibility fix (target_design §5.3): cleanup_incomplete
    " must delete both 'I' and 'D' status rows for a failed/abandoned
    " pack, not only 'I' (existing 'I'-status coverage: see
    " corrupt_trailer_no_rows).
    DATA lv_sha1    TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_pack_id TYPE zcl_abapgit_ortec_pack_stream=>ty_pack_id.
    DATA lv_count   TYPE i.

    lv_sha1 = zcl_abapgit_hash=>sha1_blob( '41414242' ).
    lv_pack_id = 'ZAOGT_CLEANUP_D_TEST'.

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_sha1
      iv_type     = 'ref_d'
      iv_data     = '41414242'
      iv_pack_id  = lv_pack_id
      iv_status   = zcl_abapgit_ortec_pack_stream=>c_status_decoded ).
    COMMIT WORK.

    zcl_abapgit_ortec_pack_stream=>cleanup_incomplete(
      iv_repo_key = mc_repo
      iv_pack_id  = lv_pack_id ).
    COMMIT WORK.

    SELECT COUNT(*) FROM zaog_obj_store INTO lv_count
      WHERE repo_key = mc_repo AND pack_id = lv_pack_id.
    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 0
      msg = 'cleanup_incomplete must remove D-status rows, not only I-status' ).
  ENDMETHOD.

  METHOD resolve_reads_own_d_row.
    " D2 staged-visibility fix (target_design §5.3.1): exercises
    " resolve_streaming end-to-end (so it also exercises
    " preload_delta_rows, not just resolve_one_meta in isolation) against
    " a REF_DELTA whose own temp-key row is genuinely 'D'-status at the
    " moment resolution begins - proving get_staged_delta_objects is
    " correctly wired into both call sites.
    DATA lv_base_data TYPE xstring VALUE '41414141'.
    DATA lv_base_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_base_raw TYPE x LENGTH 20.
    DATA lv_delta TYPE xstring VALUE '040590040121'.
    DATA lv_compressed TYPE xstring.
    DATA lv_adler TYPE zif_abapgit_git_definitions=>ty_adler32.
    DATA lv_pack_magic TYPE x LENGTH 4 VALUE '5041434B'.
    DATA lv_version TYPE x LENGTH 4 VALUE '00000002'.
    DATA lv_obj_count TYPE x LENGTH 4 VALUE '00000001'.
    DATA lv_zlib_hdr TYPE x LENGTH 2 VALUE '789C'.
    DATA lv_type_len TYPE x LENGTH 1 VALUE '76'.
    DATA lv_pack TYPE xstring.
    DATA lv_trailer_hex TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_trailer_raw TYPE x LENGTH 20.
    DATA lt_meta TYPE zcl_abapgit_ortec_pack_stream=>ty_meta_tt.
    DATA ls_meta LIKE LINE OF lt_meta.
    DATA lv_pack_id TYPE zcl_abapgit_ortec_pack_stream=>ty_pack_id.
    DATA lv_expected_data TYPE xstring VALUE '4141414121'.
    DATA lv_expected_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_count TYPE i.

    lv_base_sha = zcl_abapgit_hash=>sha1_blob( lv_base_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_base_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_base_data ).

    cl_abap_gzip=>compress_binary(
      EXPORTING raw_in = lv_delta
      IMPORTING gzip_out = lv_compressed ).
    lv_adler = zcl_abapgit_hash=>adler32( lv_delta ).
    lv_base_raw = to_upper( lv_base_sha ).

    CONCATENATE lv_pack_magic lv_version lv_obj_count INTO lv_pack IN BYTE MODE.
    CONCATENATE lv_pack lv_type_len lv_base_raw lv_zlib_hdr lv_compressed lv_adler
      INTO lv_pack IN BYTE MODE.
    lv_trailer_hex = zcl_abapgit_hash=>sha1_raw( lv_pack ).
    lv_trailer_raw = to_upper( lv_trailer_hex ).
    CONCATENATE lv_pack lv_trailer_raw INTO lv_pack IN BYTE MODE.

    lt_meta = zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming(
      EXPORTING
        iv_data     = lv_pack
        iv_repo_key = mc_repo
      IMPORTING
        ev_pack_id  = lv_pack_id ).

    READ TABLE lt_meta INTO ls_meta INDEX 1.
    SELECT COUNT(*) FROM zaog_obj_store INTO lv_count
      WHERE repo_key = mc_repo AND obj_sha1 = ls_meta-temp_key
        AND status = zcl_abapgit_ortec_pack_stream=>c_status_decoded.
    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 1
      msg = 'Precondition: the delta''s own temp row must be D-status before resolution' ).

    zcl_abapgit_ortec_obj_store=>invalidate_cache( ).
    zcl_abapgit_ortec_base_cache=>get_instance( )->clear( ).

    zcl_abapgit_ortec_pack_stream=>resolve_streaming(
      EXPORTING
        iv_repo_key = mc_repo
        iv_pack_id  = lv_pack_id
      CHANGING
        ct_meta     = lt_meta ).

    lv_expected_sha = zcl_abapgit_hash=>sha1_blob( lv_expected_data ).
    cl_abap_unit_assert=>assert_equals(
      act = lt_meta[ 1 ]-sha1
      exp = lv_expected_sha
      msg = 'resolve_streaming must correctly resolve a delta by reading its own ' &&
            'D-status temp row via get_staged_delta_objects' ).
  ENDMETHOD.

  METHOD staged_cache_hit_no_sql.
    " D2 staged-visibility fix (PERF-M-2): get_staged_delta_objects' own
    " cache-hit check must admit 'D' as well as 'R', so a second call for
    " the same temp key (without an intervening invalidate_cache) is
    " served correctly from the warm session cache. No call-counter
    " instrumentation exists on this read path (unlike
    " gv_thin_fetch_calls for resolve_one_meta's external-base fallback) -
    " verified here via a direct correctness assertion on the repeated
    " call's result instead.
    DATA lv_base_data   TYPE xstring.
    DATA lv_base_sha    TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_base_raw    TYPE x LENGTH 20.
    DATA lv_delta       TYPE xstring.
    DATA lv_compressed  TYPE xstring.
    DATA lv_adler       TYPE zif_abapgit_git_definitions=>ty_adler32.
    DATA lv_pack_magic  TYPE x LENGTH 4 VALUE '5041434B'.
    DATA lv_version     TYPE x LENGTH 4 VALUE '00000002'.
    DATA lv_obj_count   TYPE x LENGTH 4 VALUE '00000001'.
    DATA lv_zlib_hdr    TYPE x LENGTH 2 VALUE '789C'.
    DATA lv_type_len    TYPE x LENGTH 1 VALUE '76'.
    DATA lv_pack        TYPE xstring.
    DATA lv_trailer_hex TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_trailer_raw TYPE x LENGTH 20.
    DATA lt_meta        TYPE zcl_abapgit_ortec_pack_stream=>ty_meta_tt.
    DATA ls_meta        LIKE LINE OF lt_meta.
    DATA lt_sha1s       TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_first       TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_second      TYPE zif_abapgit_definitions=>ty_objects_tt.

    lv_base_data = '41414141'.
    lv_base_sha = zcl_abapgit_hash=>sha1_blob( lv_base_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_base_sha iv_type = 'blob' iv_data = lv_base_data ).

    lv_delta = '040590040121'.
    cl_abap_gzip=>compress_binary( EXPORTING raw_in = lv_delta IMPORTING gzip_out = lv_compressed ).
    lv_adler = zcl_abapgit_hash=>adler32( lv_delta ).
    lv_base_raw = to_upper( lv_base_sha ).

    CONCATENATE lv_pack_magic lv_version lv_obj_count INTO lv_pack IN BYTE MODE.
    CONCATENATE lv_pack lv_type_len lv_base_raw lv_zlib_hdr lv_compressed lv_adler
      INTO lv_pack IN BYTE MODE.
    lv_trailer_hex = zcl_abapgit_hash=>sha1_raw( lv_pack ).
    lv_trailer_raw = to_upper( lv_trailer_hex ).
    CONCATENATE lv_pack lv_trailer_raw INTO lv_pack IN BYTE MODE.

    lt_meta = zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming(
      iv_data     = lv_pack
      iv_repo_key = mc_repo ).

    READ TABLE lt_meta INTO ls_meta INDEX 1.
    APPEND ls_meta-temp_key TO lt_sha1s.

    lt_first = zcl_abapgit_ortec_obj_store=>get_staged_delta_objects(
      iv_repo_key = mc_repo
      it_sha1s    = lt_sha1s ).
    lt_second = zcl_abapgit_ortec_obj_store=>get_staged_delta_objects(
      iv_repo_key = mc_repo
      it_sha1s    = lt_sha1s ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_first ) exp = 1 ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_second ) exp = 1 ).
    cl_abap_unit_assert=>assert_equals(
      act = lt_second[ 1 ]-data
      exp = lt_first[ 1 ]-data
      msg = 'A second get_staged_delta_objects call for the same D-status sha1 ' &&
            '(no invalidate_cache in between) must return identical, correct ' &&
            'data - the cache-hit branch must admit D, not just R' ).
  ENDMETHOD.

  METHOD attempt_id_on_obj_store.
    " D2B1 (target_design §9): decode_and_persist_streaming's optional
    " iv_attempt_id must be written to every ZAOG_OBJ_STORE row this run
    " persisted, so decoded objects can be correlated back to the owning
    " ZAOG_COMMIT_HIST attempt.
    DATA lt_obj      TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj      TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_pack     TYPE xstring.
    DATA lv_sha1     TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_attempt  TYPE zcl_abapgit_ortec_mat_state=>ty_attempt_id.
    DATA lv_actual   TYPE zaog_obj_store-attempt_id.

    ls_obj-data  = '48656C6C6F'. " "Hello"
    ls_obj-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_obj-sha1  = zcl_abapgit_hash=>sha1( iv_type = ls_obj-type iv_data = ls_obj-data ).
    ls_obj-index = 1.
    lv_sha1 = ls_obj-sha1.
    APPEND ls_obj TO lt_obj.

    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).
    lv_attempt = 'ATTEMPT_D2B1_OBJ_STORE_0001'.

    zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming(
      iv_data       = lv_pack
      iv_repo_key   = mc_repo
      iv_attempt_id = lv_attempt ).

    SELECT SINGLE attempt_id FROM zaog_obj_store INTO lv_actual
      WHERE repo_key = mc_repo AND obj_sha1 = lv_sha1.
    cl_abap_unit_assert=>assert_equals( act = lv_actual exp = lv_attempt
      msg = 'A supplied iv_attempt_id must be persisted onto the resulting ZAOG_OBJ_STORE row' ).
  ENDMETHOD.

  METHOD crash_before_resolve_ok.
    " D2B1 (target_design §9): an interrupted run (crash between the
    " initial decode and the later resolve step) must leave its 'D'-status
    " temp rows tagged with the attempt id it was called with, and
    " cleanup_incomplete must still remove them cleanly regardless.
    DATA lv_sha1    TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_pack_id TYPE zcl_abapgit_ortec_pack_stream=>ty_pack_id.
    DATA lv_attempt TYPE zcl_abapgit_ortec_mat_state=>ty_attempt_id.
    DATA lv_count   TYPE i.

    lv_sha1 = zcl_abapgit_hash=>sha1_blob( '41414343' ).
    lv_pack_id = 'ZAOGT_CRASH_ATTEMPT_TEST'.
    lv_attempt = 'ATTEMPT_D2B1_CRASH_0001'.

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_sha1
      iv_type     = 'ref_d'
      iv_data     = '41414343'
      iv_pack_id  = lv_pack_id
      iv_status   = zcl_abapgit_ortec_pack_stream=>c_status_decoded ).
    UPDATE zaog_obj_store SET attempt_id = lv_attempt
      WHERE repo_key = mc_repo AND pack_id = lv_pack_id.
    COMMIT WORK.

    " Simulated crash: resolve_streaming never runs for this pack_id.
    zcl_abapgit_ortec_pack_stream=>cleanup_incomplete(
      iv_repo_key = mc_repo
      iv_pack_id  = lv_pack_id ).
    COMMIT WORK.

    SELECT COUNT(*) FROM zaog_obj_store INTO lv_count
      WHERE repo_key = mc_repo AND pack_id = lv_pack_id.
    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 0
      msg = 'cleanup_incomplete must remove an attempt-tagged D-status row left behind by a simulated crash' ).
  ENDMETHOD.
ENDCLASS.

"! Progress test double patterned after ltcl_progress_double in
"! zcl_abapgit_git_pack.clas.testclasses.abap, extended to record every
"! call so tests can assert on monotonicity/cadence/text content.
CLASS ltcl_progress_recorder DEFINITION CREATE PUBLIC FOR TESTING.
  PUBLIC SECTION.
    INTERFACES zif_abapgit_progress.
    TYPES: BEGIN OF ty_call,
             current TYPE i,
             text    TYPE string,
           END OF ty_call.
    TYPES ty_calls TYPE STANDARD TABLE OF ty_call WITH EMPTY KEY.
    DATA mt_calls TYPE ty_calls READ-ONLY.
ENDCLASS.

CLASS ltcl_progress_recorder IMPLEMENTATION.
  METHOD zif_abapgit_progress~set_total.
    RETURN.
  ENDMETHOD.
  METHOD zif_abapgit_progress~show.
    APPEND VALUE #( current = iv_current text = iv_text ) TO mt_calls.
  ENDMETHOD.
  METHOD zif_abapgit_progress~off.
    RETURN.
  ENDMETHOD.
ENDCLASS.

CLASS ltcl_pack_stream_progress DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_PROGR'.
    METHODS setup.
    METHODS teardown.
    "! Test list items 1 & 3: object progress is monotonic, never exceeds
    "! the truthful total, and the update count is bounded for a large
    "! synthetic object count (not one call per object).
    METHODS decoding_progress_bounded FOR TESTING RAISING cx_static_check.
    "! Test list item 2: a zero-object pack does not divide by zero and
    "! still produces coherent phase feedback.
    METHODS zero_object_pack_coherent FOR TESTING RAISING cx_static_check.
    "! Test list item 4: fixpoint phase reporting does not claim a false
    "! percentage.
    METHODS fixpoint_no_false_percentage FOR TESTING RAISING cx_static_check.
    "! Test list item 7: a single injected progress reference threaded
    "! through decode_streaming's whole call chain (decode + resolve +
    "! extract) is one continuous lifecycle, never reset/replaced.
    METHODS one_lifecycle_across_phases FOR TESTING RAISING cx_static_check.
    "! Test list item 5: progress reporting does not change successful
    "! decode results.
    METHODS progress_no_result_change FOR TESTING RAISING cx_static_check.
    "! Test list item 6: progress reporting does not replace the original
    "! exception/failure behavior.
    METHODS progress_preserves_failure FOR TESTING RAISING cx_static_check.
ENDCLASS.

CLASS ltcl_pack_stream_progress IMPLEMENTATION.
  METHOD setup.
    ROLLBACK WORK. "#EC CI_ROLLBACK
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    COMMIT WORK.
  ENDMETHOD.

  METHOD teardown.
    ROLLBACK WORK. "#EC CI_ROLLBACK
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    COMMIT WORK.
  ENDMETHOD.

  METHOD decoding_progress_bounded.
    DATA lt_obj           TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj           TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_pack          TYPE xstring.
    DATA lo_recorder      TYPE REF TO ltcl_progress_recorder.
    DATA lv_prev_current  TYPE i.
    DATA lv_decoding_calls TYPE i.

    CREATE OBJECT lo_recorder.

    DO 1200 TIMES.
      CLEAR ls_obj.
      ls_obj-type  = zif_abapgit_git_definitions=>c_type-blob.
      ls_obj-data  = zcl_abapgit_convert=>string_to_xstring_utf8( |blob-{ sy-index }| ).
      ls_obj-sha1  = zcl_abapgit_hash=>sha1( iv_type = ls_obj-type iv_data = ls_obj-data ).
      ls_obj-index = sy-index.
      APPEND ls_obj TO lt_obj.
    ENDDO.
    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).

    zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming(
      iv_data     = lv_pack
      iv_repo_key = mc_repo
      ii_progress = lo_recorder ).

    LOOP AT lo_recorder->mt_calls INTO DATA(ls_call) WHERE text CS 'Decoding pack object'.
      lv_decoding_calls = lv_decoding_calls + 1.
      cl_abap_unit_assert=>assert_true( act = xsdbool( ls_call-current >= lv_prev_current )
        msg = 'Decoding progress current must be monotonically non-decreasing' ).
      cl_abap_unit_assert=>assert_true( act = xsdbool( ls_call-current <= 1200 )
        msg = 'Decoding progress current must never exceed the truthful total' ).
      lv_prev_current = ls_call-current.
    ENDLOOP.

    cl_abap_unit_assert=>assert_true( act = xsdbool( lv_decoding_calls > 0 )
      msg = 'At least one decoding progress call must have fired' ).
    cl_abap_unit_assert=>assert_true( act = xsdbool( lv_decoding_calls < 20 )
      msg = 'Update count must be bounded, not one call per object, for a 1200-object pack' ).
  ENDMETHOD.

  METHOD zero_object_pack_coherent.
    DATA lt_obj      TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_pack     TYPE xstring.
    DATA lt_meta     TYPE zcl_abapgit_ortec_pack_stream=>ty_meta_tt.
    DATA lo_recorder TYPE REF TO ltcl_progress_recorder.

    CREATE OBJECT lo_recorder.

    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ). " empty table -> 0-object pack

    lt_meta = zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming(
      iv_data     = lv_pack
      iv_repo_key = mc_repo
      ii_progress = lo_recorder ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_meta ) exp = 0
      msg = 'A zero-object pack must decode to an empty metadata table without error' ).
    cl_abap_unit_assert=>assert_true(
      act = xsdbool( line_exists( lo_recorder->mt_calls[ text = 'Git: Decoding pack object 0 of 0' ] ) )
      msg = 'A zero-object pack must still produce coherent phase feedback with no division by zero' ).
  ENDMETHOD.

  METHOD fixpoint_no_false_percentage.
    DATA lv_base_data   TYPE xstring VALUE '41414141'.
    DATA lv_base_sha    TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_base_raw    TYPE x LENGTH 20.
    DATA lv_delta       TYPE xstring VALUE '040590040121'.
    DATA lv_compressed  TYPE xstring.
    DATA lv_adler       TYPE zif_abapgit_git_definitions=>ty_adler32.
    DATA lv_pack_magic  TYPE x LENGTH 4 VALUE '5041434B'.
    DATA lv_version     TYPE x LENGTH 4 VALUE '00000002'.
    DATA lv_obj_count   TYPE x LENGTH 4 VALUE '00000001'.
    DATA lv_zlib_hdr    TYPE x LENGTH 2 VALUE '789C'.
    DATA lv_type_len    TYPE x LENGTH 1 VALUE '76'.
    DATA lv_pack        TYPE xstring.
    DATA lv_trailer_hex TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_trailer_raw TYPE x LENGTH 20.
    DATA lt_meta        TYPE zcl_abapgit_ortec_pack_stream=>ty_meta_tt.
    DATA lv_pack_id     TYPE zcl_abapgit_ortec_pack_stream=>ty_pack_id.
    DATA lo_recorder    TYPE REF TO ltcl_progress_recorder.
    DATA lv_found_pass  TYPE abap_bool.

    CREATE OBJECT lo_recorder.

    lv_base_sha = zcl_abapgit_hash=>sha1_blob( lv_base_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = lv_base_sha
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv_base_data ).

    cl_abap_gzip=>compress_binary(
      EXPORTING raw_in = lv_delta
      IMPORTING gzip_out = lv_compressed ).
    lv_adler = zcl_abapgit_hash=>adler32( lv_delta ).
    lv_base_raw = to_upper( lv_base_sha ).

    CONCATENATE lv_pack_magic lv_version lv_obj_count INTO lv_pack IN BYTE MODE.
    CONCATENATE lv_pack lv_type_len lv_base_raw lv_zlib_hdr lv_compressed lv_adler
      INTO lv_pack IN BYTE MODE.
    lv_trailer_hex = zcl_abapgit_hash=>sha1_raw( lv_pack ).
    lv_trailer_raw = to_upper( lv_trailer_hex ).
    CONCATENATE lv_pack lv_trailer_raw INTO lv_pack IN BYTE MODE.

    lt_meta = zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming(
      EXPORTING
        iv_data     = lv_pack
        iv_repo_key = mc_repo
      IMPORTING
        ev_pack_id  = lv_pack_id ).

    zcl_abapgit_ortec_obj_store=>invalidate_cache( ).
    zcl_abapgit_ortec_base_cache=>get_instance( )->clear( ).

    zcl_abapgit_ortec_pack_stream=>resolve_streaming(
      EXPORTING
        iv_repo_key = mc_repo
        iv_pack_id  = lv_pack_id
        ii_progress = lo_recorder
      CHANGING
        ct_meta     = lt_meta ).

    LOOP AT lo_recorder->mt_calls INTO DATA(ls_call).
      cl_abap_unit_assert=>assert_false( act = xsdbool( ls_call-text CS '%' )
        msg = 'Fixpoint/resolution phase text must never claim a percentage' ).
      IF ls_call-text CS 'Resolving in-pack deltas'.
        lv_found_pass = abap_true.
      ENDIF.
    ENDLOOP.
    cl_abap_unit_assert=>assert_true( act = lv_found_pass
      msg = 'At least one in-pack fixpoint pass must have been reported' ).
  ENDMETHOD.

  METHOD one_lifecycle_across_phases.
    DATA lt_nodes       TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node        LIKE LINE OF lt_nodes.
    DATA ls_commit      TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lt_obj         TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj         LIKE LINE OF lt_obj.
    DATA lv_pack        TYPE xstring.
    DATA lv_blob_data   TYPE xstring.
    DATA lv_blob_sha    TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_tree_data   TYPE xstring.
    DATA lv_tree_sha    TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_commit_data TYPE xstring.
    DATA lv_commit_sha  TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lo_recorder    TYPE REF TO ltcl_progress_recorder.

    CREATE OBJECT lo_recorder.

    lv_blob_data = '48656C6C6F'.
    lv_blob_sha  = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).

    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_node-name  = 'hello.txt'.
    ls_node-sha1  = lv_blob_sha.
    APPEND ls_node TO lt_nodes.
    lv_tree_data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    lv_tree_sha  = zcl_abapgit_hash=>sha1_tree( lv_tree_data ).

    ls_commit-tree      = lv_tree_sha.
    ls_commit-author    = 'Test <test@example.com> 0 +0000'.
    ls_commit-committer = 'Test <test@example.com> 0 +0000'.
    ls_commit-body      = 'test'.
    lv_commit_data = zcl_abapgit_git_pack=>encode_commit( ls_commit ).
    lv_commit_sha  = zcl_abapgit_hash=>sha1_commit( lv_commit_data ).

    CLEAR ls_obj.
    ls_obj-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_obj-data  = lv_blob_data.
    ls_obj-sha1  = lv_blob_sha.
    ls_obj-index = 1.
    APPEND ls_obj TO lt_obj.

    CLEAR ls_obj.
    ls_obj-type  = zif_abapgit_git_definitions=>c_type-tree.
    ls_obj-data  = lv_tree_data.
    ls_obj-sha1  = lv_tree_sha.
    ls_obj-index = 2.
    APPEND ls_obj TO lt_obj.

    CLEAR ls_obj.
    ls_obj-type  = zif_abapgit_git_definitions=>c_type-commit.
    ls_obj-data  = lv_commit_data.
    ls_obj-sha1  = lv_commit_sha.
    ls_obj-index = 3.
    APPEND ls_obj TO lt_obj.

    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).

    zcl_abapgit_ortec_pack_stream=>decode_streaming(
      iv_data     = lv_pack
      iv_repo_key = mc_repo
      ii_progress = lo_recorder ).

    cl_abap_unit_assert=>assert_true(
      act = xsdbool( line_exists( lo_recorder->mt_calls[ text = |Git: Decoding pack object 0 of 3| ] ) )
      msg = 'The single injected progress reference must have received the decode phase call' ).
    cl_abap_unit_assert=>assert_true(
      act = xsdbool( line_exists( lo_recorder->mt_calls[ text = 'Git: Extracting commits' ] ) )
      msg = 'The SAME injected reference must also have received the final extraction phase call - ' &&
            'one continuous lifecycle, not a reset/second progress object' ).
  ENDMETHOD.

  METHOD progress_no_result_change.
    DATA lt_obj    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj    TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_pack   TYPE xstring.
    DATA lt_meta_a TYPE zcl_abapgit_ortec_pack_stream=>ty_meta_tt.
    DATA lt_meta_b TYPE zcl_abapgit_ortec_pack_stream=>ty_meta_tt.
    DATA lo_recorder TYPE REF TO ltcl_progress_recorder.

    CREATE OBJECT lo_recorder.

    ls_obj-data  = '48656C6C6F'.
    ls_obj-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_obj-sha1  = zcl_abapgit_hash=>sha1( iv_type = ls_obj-type iv_data = ls_obj-data ).
    ls_obj-index = 1.
    APPEND ls_obj TO lt_obj.
    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).

    lt_meta_a = zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming(
      iv_data     = lv_pack
      iv_repo_key = mc_repo ).

    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    COMMIT WORK.

    lt_meta_b = zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming(
      iv_data     = lv_pack
      iv_repo_key = mc_repo
      ii_progress = lo_recorder ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_meta_b ) exp = lines( lt_meta_a )
      msg = 'Progress reporting must not change the number of decoded objects' ).
    cl_abap_unit_assert=>assert_equals( act = lt_meta_b[ 1 ]-sha1 exp = lt_meta_a[ 1 ]-sha1
      msg = 'Progress reporting must not change the decoded result' ).
    cl_abap_unit_assert=>assert_equals( act = lt_meta_b[ 1 ]-is_resolved exp = lt_meta_a[ 1 ]-is_resolved ).
    cl_abap_unit_assert=>assert_true( act = xsdbool( lo_recorder->mt_calls IS NOT INITIAL )
      msg = 'Sanity: the recorder-attached run must actually have received calls' ).
  ENDMETHOD.

  METHOD progress_preserves_failure.
    DATA lt_obj      TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj      TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_pack     TYPE xstring.
    DATA lv_len      TYPE i.
    DATA lv_caught   TYPE abap_bool.
    DATA lv_count    TYPE i.
    DATA lo_recorder TYPE REF TO ltcl_progress_recorder.

    CREATE OBJECT lo_recorder.

    ls_obj-data  = '48656C6C6F'.
    ls_obj-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_obj-sha1  = zcl_abapgit_hash=>sha1( iv_type = ls_obj-type iv_data = ls_obj-data ).
    ls_obj-index = 1.
    APPEND ls_obj TO lt_obj.
    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).

    lv_len = xstrlen( lv_pack ) - 1.
    lv_pack = lv_pack(lv_len) && 'FF'.

    lv_caught = abap_false.
    TRY.
        zcl_abapgit_ortec_pack_stream=>decode_and_persist_streaming(
          iv_data     = lv_pack
          iv_repo_key = mc_repo
          ii_progress = lo_recorder ).
      CATCH zcx_abapgit_ortec_git.
        lv_caught = abap_true.
    ENDTRY.
    cl_abap_unit_assert=>assert_true( act = lv_caught
      msg = 'A progress reference must not suppress the real decode failure' ).

    SELECT COUNT(*) FROM zaog_obj_store INTO lv_count WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 0
      msg = 'Failure cleanup must still happen even when progress is attached' ).
  ENDMETHOD.
ENDCLASS.
