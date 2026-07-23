"! Tests for {@link ZCL_ABAPGIT_ORTEC_PACK_DEC}.
"! Verifies the full decode_and_persist and crash-resume flow.
"! All data is isolated by MC_REPO. Tests access real DB tables
"! (no SQL test doubles needed; cleanup is done in setup/teardown).
CLASS ltcl_pack_decoder DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    "! Repo key used to isolate all test DB writes
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOG_TEST_PD'.
    "! Fixed pack ID used when simulating a crashed session
    CONSTANTS mc_pack TYPE c LENGTH 32 VALUE 'TESTPACK00000000000000000000001A'.
    METHODS setup.
    METHODS teardown.
    "! decode_and_persist with pre-decoded objects: verifies fast path writes
    "! all five tables and cleans up raw_pack on success.
    METHODS decode_populates_all FOR TESTING RAISING cx_static_check.
    "! decode_and_persist without pre-decoded objects: exercises resumable_decode
    "! (full decompression) and verifies the same post-conditions.
    METHODS decode_from_pack     FOR TESTING RAISING cx_static_check.
    "! resume_decode when active session + raw_pack exist: re-decodes and
    "! persists all objects, marks session complete, removes raw_pack.
    METHODS resume_after_partial FOR TESTING RAISING cx_static_check.
    "! resume_decode with no active session: must return empty, no side effects.
    METHODS resume_no_session    FOR TESTING RAISING cx_static_check.
    "! decode_and_persist when resumable_decode fails after temp rows were
    "! already written (corrupt trailing pack SHA1): verifies the CATCH
    "! zcx_abapgit_exception cleanup block removes every temp/meta/raw row
    "! it created, instead of leaving orphaned data for a future resume
    "! attempt to stumble over.
    METHODS cleanup_after_decode_failure FOR TESTING RAISING cx_static_check.
    "! Regression: resumable_decode's bulk external-delta-base prefetch merge
    "! never set the merged objects' -index, so every prefetched base
    "! defaulted to index = 0 and collided in resolve_all's UNIQUE-KEY
    "! obj_index side-index, causing a delta correctly found by SHA1 to be
    "! resolved against a completely different, unrelated prefetched base
    "! ("Delta base identity mismatch"). Forces two distinct external bases
    "! to be bulk-prefetched in one decode_and_persist call.
    METHODS prefetch_bases_do_not_collide FOR TESTING RAISING cx_static_check.
    "! peek_object_count must read the pack-header object count without
    "! decompressing anything, return 0 for a real zero-object pack (the
    "! "you already have everything" server response), and return -1
    "! (never 0) for data too short/malformed to contain a header, so
    "! callers cannot mistake "can't tell yet" for "confirmed empty".
    METHODS peek_object_count_cases FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_pack_decoder IMPLEMENTATION.
  METHOD setup.
    DELETE FROM zaog_obj_store  WHERE repo_key = mc_repo.
    DELETE FROM zaog_pack_meta  WHERE repo_key = mc_repo.
    DELETE FROM zaog_pack_idx   WHERE repo_key = mc_repo.
    DELETE FROM zaog_fetch_sess WHERE repo_key = mc_repo.
    DELETE FROM zaog_raw_pack   WHERE repo_key = mc_repo.
  ENDMETHOD.
  METHOD teardown.
    DELETE FROM zaog_obj_store  WHERE repo_key = mc_repo.
    DELETE FROM zaog_pack_meta  WHERE repo_key = mc_repo.
    DELETE FROM zaog_pack_idx   WHERE repo_key = mc_repo.
    DELETE FROM zaog_fetch_sess WHERE repo_key = mc_repo.
    DELETE FROM zaog_raw_pack   WHERE repo_key = mc_repo.
    ROLLBACK WORK.
  ENDMETHOD.

  METHOD decode_populates_all.
    DATA lt_obj  TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj  TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_pack TYPE xstring.
    ls_obj-sha1 = 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d'.
    ls_obj-type = 'blob'. ls_obj-data = '48656C6C6F'. ls_obj-index = 1.
    APPEND ls_obj TO lt_obj.
    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).
    DATA lt_res TYPE zif_abapgit_definitions=>ty_objects_tt.
    lt_res = zcl_abapgit_ortec_pack_dec=>decode_and_persist(
      iv_data    = lv_pack
      iv_repo_key = mc_repo
      it_objects  = lt_obj ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_res ) exp = 1 msg = '1 object returned' ).
    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = mc_repo iv_sha1 = 'aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d' )
      msg = 'Object must be in obj_store' ).
    DATA ls_meta TYPE zaog_pack_meta.
    SELECT SINGLE * FROM zaog_pack_meta INTO ls_meta WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_subrc( msg = 'pack_meta must exist' ).
    cl_abap_unit_assert=>assert_equals( act = ls_meta-status exp = 'C' msg = 'pack_meta complete' ).
    DATA lv_idx TYPE i.
    SELECT COUNT(*) FROM zaog_pack_idx INTO lv_idx WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_idx exp = 1 msg = 'pack_idx has 1 entry' ).
    DATA ls_sess TYPE zaog_fetch_sess.
    SELECT SINGLE * FROM zaog_fetch_sess INTO ls_sess WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_subrc( msg = 'fetch_sess must exist' ).
    cl_abap_unit_assert=>assert_equals( act = ls_sess-status exp = 'C' msg = 'session complete' ).
    DATA lv_raw TYPE i.
    SELECT COUNT(*) FROM zaog_raw_pack INTO lv_raw WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_raw exp = 0 msg = 'raw_pack cleaned up' ).
  ENDMETHOD.

  METHOD decode_from_pack.
    " it_objects intentionally NOT supplied: exercises resumable_decode.
    DATA lt_obj  TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj  TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_pack TYPE xstring.
    DATA lt_res  TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_sha1 TYPE zif_abapgit_git_definitions=>ty_sha1.
    ls_obj-data  = '48656C6C6F'. " ASCII: Hello
    ls_obj-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_obj-sha1  = zcl_abapgit_hash=>sha1( iv_type = ls_obj-type iv_data = ls_obj-data ).
    ls_obj-index = 1. APPEND ls_obj TO lt_obj.
    lv_sha1 = ls_obj-sha1.
    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).
    lt_res = zcl_abapgit_ortec_pack_dec=>decode_and_persist(
      iv_data     = lv_pack
      iv_repo_key = mc_repo ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_res ) exp = 1 msg = '1 object decoded from pack' ).
    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = mc_repo iv_sha1 = lv_sha1 )
      msg = 'Decoded object in obj_store' ).
    DATA ls_sess TYPE zaog_fetch_sess.
    SELECT SINGLE * FROM zaog_fetch_sess INTO ls_sess WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_subrc( msg = 'Session exists' ).
    cl_abap_unit_assert=>assert_equals( act = ls_sess-status exp = 'C' msg = 'Session complete' ).
    DATA lv_raw TYPE i.
    SELECT COUNT(*) FROM zaog_raw_pack INTO lv_raw WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_raw exp = 0 msg = 'raw_pack cleaned up after decode' ).
  ENDMETHOD.

  METHOD resume_after_partial.
    " Simulate crash: raw_pack stored + active session, nothing in obj_store yet.
    DATA lt_obj  TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj  TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_pack TYPE xstring.
    DATA ls_raw  TYPE zaog_raw_pack.
    DATA ls_sess TYPE zaog_fetch_sess.
    DATA lt_res  TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_sha1 TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_ts   TYPE timestampl.
    ls_obj-data  = '48656C6C6F'. " ASCII: Hello
    ls_obj-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_obj-sha1  = zcl_abapgit_hash=>sha1( iv_type = ls_obj-type iv_data = ls_obj-data ).
    ls_obj-index = 1. APPEND ls_obj TO lt_obj.
    lv_sha1 = ls_obj-sha1.
    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).
    " Insert crash-state into DB
    ls_raw-repo_key = mc_repo. ls_raw-pack_id = mc_pack. ls_raw-raw_data = lv_pack.
    MODIFY zaog_raw_pack FROM ls_raw.
    GET TIME STAMP FIELD lv_ts.
    ls_sess-session_id = 'RESSESTEST000000000000000000001A'.
    ls_sess-repo_key   = mc_repo. ls_sess-pack_id    = mc_pack.
    ls_sess-phase      = 'D'.     ls_sess-obj_done   = 0. ls_sess-obj_total = 1.
    ls_sess-status     = 'A'.     ls_sess-created_at = lv_ts. ls_sess-updated_at = lv_ts.
    INSERT zaog_fetch_sess FROM ls_sess.
    COMMIT WORK.
    " Resume
    lt_res = zcl_abapgit_ortec_pack_dec=>resume_decode( mc_repo ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_res ) exp = 1 msg = '1 object after resume' ).
    cl_abap_unit_assert=>assert_true(
      act = zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = mc_repo iv_sha1 = lv_sha1 )
      msg = 'Object in obj_store after resume' ).
    SELECT SINGLE * FROM zaog_fetch_sess INTO ls_sess WHERE repo_key = mc_repo AND status = 'C'.
    cl_abap_unit_assert=>assert_subrc( msg = 'Session must be complete after resume' ).
    DATA lv_raw TYPE i.
    SELECT COUNT(*) FROM zaog_raw_pack INTO lv_raw WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_raw exp = 0 msg = 'raw_pack cleaned up after resume' ).
  ENDMETHOD.

  METHOD resume_no_session.
    DATA lt_res TYPE zif_abapgit_definitions=>ty_objects_tt.
    lt_res = zcl_abapgit_ortec_pack_dec=>resume_decode( mc_repo ).
    cl_abap_unit_assert=>assert_initial( act = lt_res msg = 'No session = empty result' ).
  ENDMETHOD.

  METHOD cleanup_after_decode_failure.
    DATA lt_obj    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_obj    TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_pack   TYPE xstring.
    DATA lv_len    TYPE i.
    DATA lt_res    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_caught TYPE abap_bool.
    DATA lv_count  TYPE i.

    ls_obj-data  = '48656C6C6F'. " ASCII: Hello
    ls_obj-type  = zif_abapgit_git_definitions=>c_type-blob.
    ls_obj-sha1  = zcl_abapgit_hash=>sha1( iv_type = ls_obj-type iv_data = ls_obj-data ).
    ls_obj-index = 1.
    APPEND ls_obj TO lt_obj.

    lv_pack = zcl_abapgit_git_pack=>encode( lt_obj ).

    " Corrupt only the last byte of the trailing 20-byte pack SHA1 so every
    " object still decompresses and parses correctly, and temp rows are
    " persisted as usual; only the final trailer-integrity check fails,
    " forcing decode_and_persist into its CATCH zcx_abapgit_exception
    " cleanup path with real, already-committed temp data to remove.
    lv_len = xstrlen( lv_pack ) - 1.
    lv_pack = lv_pack(lv_len) && 'FF'.

    lv_caught = abap_false.
    TRY.
        lt_res = zcl_abapgit_ortec_pack_dec=>decode_and_persist(
          iv_data     = lv_pack
          iv_repo_key = mc_repo ).
      CATCH zcx_abapgit_exception.
        lv_caught = abap_true.
    ENDTRY.
    cl_abap_unit_assert=>assert_true( act = lv_caught msg = 'Corrupt trailer must raise zcx_abapgit_exception' ).

    SELECT COUNT(*) FROM zaog_obj_store INTO lv_count WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 0 msg = 'obj_store temp rows cleaned up after failure' ).

    SELECT COUNT(*) FROM zaog_pack_idx INTO lv_count WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 0 msg = 'pack_idx rows cleaned up after failure' ).

    SELECT COUNT(*) FROM zaog_pack_meta INTO lv_count WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 0 msg = 'pack_meta row cleaned up after failure' ).

    SELECT COUNT(*) FROM zaog_raw_pack INTO lv_count WHERE repo_key = mc_repo.
    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 0 msg = 'raw_pack cleaned up after failure' ).
  ENDMETHOD.

  METHOD prefetch_bases_do_not_collide.
    DATA lv_base1_data TYPE xstring.
    DATA lv_base2_data TYPE xstring.
    DATA lv_base1_sha   TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_base2_sha   TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_base1_raw   TYPE x LENGTH 20.
    DATA lv_base2_raw   TYPE x LENGTH 20.
    DATA lv_delta       TYPE xstring.
    DATA lv_compressed  TYPE xstring.
    DATA lv_adler       TYPE zif_abapgit_git_definitions=>ty_adler32.
    DATA lv_pack_magic  TYPE x LENGTH 4 VALUE '5041434B'.
    DATA lv_version     TYPE x LENGTH 4 VALUE '00000002'.
    DATA lv_obj_count   TYPE x LENGTH 4 VALUE '00000002'.
    DATA lv_zlib_hdr    TYPE x LENGTH 2 VALUE '789C'.
    DATA lv_type_len    TYPE x LENGTH 1 VALUE '76'. " ref_d (0x70) | length 6, no continuation
    DATA lv_pack        TYPE xstring.
    DATA lv_trailer_hex TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_trailer_raw TYPE x LENGTH 20.
    DATA lt_res         TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_expect1_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_expect2_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    lv_base1_data = '41414141'. " "AAAA"
    lv_base2_data = '42424242'. " "BBBB"
    lv_base1_sha = zcl_abapgit_hash=>sha1_blob( lv_base1_data ).
    lv_base2_sha = zcl_abapgit_hash=>sha1_blob( lv_base2_data ).

    " Both bases stored externally (as if from a prior fetch) - NEITHER is
    " included in this pack, forcing resumable_decode's bulk external-base
    " prefetch (the buggy merge loop) to fire for both.
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_base1_sha iv_type = 'blob' iv_data = lv_base1_data ).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo iv_sha1 = lv_base2_sha iv_type = 'blob' iv_data = lv_base2_data ).

    " Delta: base-size(4) result-size(5), copy(off=0,len=4), insert(1,'!')
    " -> applies identically to any 4-byte base, producing base & "!". Both
    " pack entries use this exact same delta; they are distinguished only
    " by their declared (20-byte raw) base SHA1.
    lv_delta = '040590040121'.
    cl_abap_gzip=>compress_binary( EXPORTING raw_in = lv_delta IMPORTING gzip_out = lv_compressed ).
    lv_adler = zcl_abapgit_hash=>adler32( lv_delta ).

    lv_base1_raw = to_upper( lv_base1_sha ).
    lv_base2_raw = to_upper( lv_base2_sha ).

    CONCATENATE lv_pack_magic lv_version lv_obj_count INTO lv_pack IN BYTE MODE.
    CONCATENATE lv_pack lv_type_len lv_base1_raw lv_zlib_hdr lv_compressed lv_adler
      INTO lv_pack IN BYTE MODE.
    CONCATENATE lv_pack lv_type_len lv_base2_raw lv_zlib_hdr lv_compressed lv_adler
      INTO lv_pack IN BYTE MODE.

    lv_trailer_hex = zcl_abapgit_hash=>sha1_raw( lv_pack ).
    lv_trailer_raw = to_upper( lv_trailer_hex ).
    CONCATENATE lv_pack lv_trailer_raw INTO lv_pack IN BYTE MODE.

    lt_res = zcl_abapgit_ortec_pack_dec=>decode_and_persist(
      iv_data     = lv_pack
      iv_repo_key = mc_repo ).

    lv_expect1_sha = zcl_abapgit_hash=>sha1_blob( '4141414121' ). " "AAAA!"
    lv_expect2_sha = zcl_abapgit_hash=>sha1_blob( '4242424221' ). " "BBBB!"

    READ TABLE lt_res TRANSPORTING NO FIELDS
      WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-blob sha1 = lv_expect1_sha.
    cl_abap_unit_assert=>assert_subrc(
      msg = 'The delta declaring base1 must resolve against base1 ("AAAA!"), not collide with base2' ).

    READ TABLE lt_res TRANSPORTING NO FIELDS
      WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-blob sha1 = lv_expect2_sha.
    cl_abap_unit_assert=>assert_subrc(
      msg = 'The delta declaring base2 must resolve against base2 ("BBBB!"), not collide with base1' ).
  ENDMETHOD.

  METHOD peek_object_count_cases.
    DATA lv_data TYPE xstring.

    " Too short to contain a full 12-byte header.
    lv_data = '5041434B0000'.
    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_pack_dec=>peek_object_count( lv_data )
      exp = -1
      msg = 'Data shorter than the header must return -1, never 0' ).

    " Well-formed PACK header (magic + version 2) declaring zero objects -
    " the real "you already have everything" server response shape.
    lv_data = '5041434B0000000200000000'.
    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_pack_dec=>peek_object_count( lv_data )
      exp = 0
      msg = 'A well-formed zero-object pack header must be recognized as 0' ).

    " Well-formed PACK header declaring 3 objects.
    lv_data = '5041434B0000000200000003'.
    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_pack_dec=>peek_object_count( lv_data )
      exp = 3
      msg = 'A non-zero declared object count must be read correctly' ).

    " Long enough but missing the PACK magic entirely.
    lv_data = '000000000000000000000000'.
    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_pack_dec=>peek_object_count( lv_data )
      exp = -1
      msg = 'Data without the PACK magic must return -1, never 0' ).
  ENDMETHOD.
ENDCLASS.
