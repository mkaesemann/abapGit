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
      WHERE repo_key = mc_repo AND obj_sha1 = ls_meta-temp_key AND status = 'R'.
    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 1
      msg = 'The temp-keyed delta row must exist and be promoted to R after a successful pass' ).
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
ENDCLASS.
