CLASS zcl_abapgit_ortec_pack_stream DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    TYPES ty_repo_key TYPE c LENGTH 12.
    TYPES ty_pack_id  TYPE c LENGTH 32.

    "! In-progress row status for zaog_obj_store, used ONLY by this streaming
    "! decoder. Distinct from the existing non-streaming decoder's own 'P'
    "! (pending) status - every existing object-store read path already
    "! hardcodes status = 'R', so rows written under this status are already
    "! invisible to every existing consumer with no further filtering needed.
    CONSTANTS c_status_incomplete TYPE c LENGTH 1 VALUE 'I'.

    "! One row per object in the pack. This is the ONLY per-object state kept
    "! in memory during/after a streaming decode - never the object's actual
    "! decompressed bytes, which are persisted and freed immediately.
    TYPES: BEGIN OF ty_meta,
             obj_index   TYPE i,
             pack_offset TYPE i,
             obj_type    TYPE zif_abapgit_git_definitions=>ty_type,
             sha1        TYPE zif_abapgit_git_definitions=>ty_sha1,
             temp_key    TYPE zif_abapgit_git_definitions=>ty_sha1,
             delta_base  TYPE zif_abapgit_git_definitions=>ty_sha1,
             base_offset TYPE i,
             obj_size    TYPE i,
             is_resolved TYPE abap_bool,
           END OF ty_meta.
    TYPES ty_meta_tt TYPE STANDARD TABLE OF ty_meta WITH EMPTY KEY.

    "! Streams a single Git packfile: decodes one object at a time, persists
    "! each object's bytes to zaog_obj_store immediately (status 'I'), then
    "! frees the local decompressed buffer before moving to the next object -
    "! at no point is more than one object's decompressed bytes held in
    "! memory simultaneously. Non-delta objects (commit/tree/blob/tag) are
    "! persisted under their real SHA1 with is_resolved = abap_true. Delta
    "! objects (REF_DELTA/OFS_DELTA) cannot have their final identity known
    "! yet (that requires resolving against a base object, done by a later
    "! phase) - they are persisted under a temporary key with
    "! is_resolved = abap_false, mirroring the temporary-key convention the
    "! existing non-streaming decoder already uses for its own in-progress
    "! delta rows.
    "!
    "! On full success (including trailer SHA1 verification), every row this
    "! run wrote is promoted from 'I' to 'R' in a single set-based UPDATE,
    "! making them visible to all normal object-store reads.
    "!
    "! On any failure, every row this run wrote is removed in a single
    "! set-based DELETE before the original error is re-raised, so a partial/
    "! failed run never leaves 'I'-status rows behind for a later phase (or a
    "! fallback decoder) to trip over.
    "!
    "! Delta resolution/application is explicitly OUT OF SCOPE here - the
    "! returned metadata table's unresolved rows are the input a later phase
    "! uses to resolve deltas against their bases.
    CLASS-METHODS decode_and_persist_streaming
      IMPORTING iv_data        TYPE xstring
                iv_repo_key    TYPE ty_repo_key
      RETURNING VALUE(rt_meta) TYPE ty_meta_tt
      RAISING   zcx_abapgit_ortec_git.

  PRIVATE SECTION.
    CONSTANTS c_pack_start TYPE x LENGTH 4 VALUE '5041434B' ##NO_TEXT.
    CONSTANTS c_version    TYPE x LENGTH 4 VALUE '00000002' ##NO_TEXT.
    CONSTANTS c_zlib       TYPE x LENGTH 2 VALUE '789C' ##NO_TEXT.
    CONSTANTS c_zlib_hmm   TYPE x LENGTH 2 VALUE '7801' ##NO_TEXT.

    CLASS-METHODS build_pack_id
      IMPORTING iv_repo_key       TYPE ty_repo_key
      RETURNING VALUE(rv_pack_id) TYPE ty_pack_id.

    "! Set-based cleanup of this run's own 'I'-status rows only - never
    "! touches any other pack_id's rows, the old decoder's 'P' rows, or any
    "! of the old decoder's own bookkeeping tables (zaog_pack_meta,
    "! zaog_pack_idx, zaog_raw_pack, zaog_fetch_sess) - this streaming path
    "! deliberately does not use any of those.
    CLASS-METHODS cleanup_incomplete
      IMPORTING iv_repo_key TYPE ty_repo_key
                iv_pack_id  TYPE ty_pack_id.
ENDCLASS.


CLASS zcl_abapgit_ortec_pack_stream IMPLEMENTATION.

  METHOD build_pack_id.
    DATA lv_ts TYPE timestampl.
    GET TIME STAMP FIELD lv_ts.
    rv_pack_id = |{ iv_repo_key }{ lv_ts }|.
    rv_pack_id = rv_pack_id(32).
  ENDMETHOD.

  METHOD cleanup_incomplete.
    DELETE FROM zaog_obj_store
      WHERE repo_key = iv_repo_key
        AND pack_id  = iv_pack_id
        AND status   = zcl_abapgit_ortec_pack_stream=>c_status_incomplete.
  ENDMETHOD.

  METHOD decode_and_persist_streaming.
    DATA lv_data           TYPE xstring.
    DATA lv_xstring        TYPE xstring.
    DATA lv_objects        TYPE i.
    DATA lv_x              TYPE x LENGTH 1.
    DATA lv_type           TYPE zif_abapgit_git_definitions=>ty_type.
    DATA lv_expected       TYPE i.
    DATA lv_ref_delta      TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_zlib           TYPE x LENGTH 2.
    DATA lv_decompressed   TYPE xstring.
    DATA lv_compressed_len TYPE i.
    DATA lv_pack_id        TYPE ty_pack_id.
    DATA lv_temp_sha1      TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_uindex         TYPE sy-index.
    DATA lv_curr_offset    TYPE i.
    DATA lv_sha1           TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_len            TYPE i.
    DATA lv_base_offset    TYPE i.
    DATA ls_meta           LIKE LINE OF rt_meta.
    DATA lx_abapgit        TYPE REF TO zcx_abapgit_exception.
    DATA lx_ortec          TYPE REF TO zcx_abapgit_ortec_git.

    lv_pack_id = build_pack_id( iv_repo_key ).

    TRY.
        lv_data = iv_data.

        IF xstrlen( lv_data ) < 12.
          zcx_abapgit_ortec_git=>raise( 'Streaming pack too short' ).
        ENDIF.
        IF lv_data(4) <> c_pack_start.
          zcx_abapgit_ortec_git=>raise( |Streaming pack header mismatch { lv_data(4) }| ).
        ENDIF.
        lv_data = lv_data+4.

        IF lv_data(4) <> c_version.
          zcx_abapgit_ortec_git=>raise( |Streaming pack version mismatch { lv_data(4) }| ).
        ENDIF.
        lv_data = lv_data+4.

        lv_xstring = lv_data(4).
        lv_objects = zcl_abapgit_convert=>xstring_to_int( lv_xstring ).
        lv_data = lv_data+4.

        DO lv_objects TIMES.
          lv_uindex = sy-index.
          lv_curr_offset = xstrlen( iv_data ) - xstrlen( lv_data ).
          lv_x = lv_data(1).
          lv_type = zcl_abapgit_ortec_pack_dec=>get_type( lv_x ).

          zcl_abapgit_ortec_pack_dec=>get_length(
            IMPORTING ev_length = lv_expected
            CHANGING  cv_data   = lv_data ).

          CLEAR lv_ref_delta.
          CLEAR lv_base_offset.
          IF lv_type = zif_abapgit_git_definitions=>c_type-ref_d.
            lv_ref_delta = lv_data(20).
            TRANSLATE lv_ref_delta TO LOWER CASE.
            lv_data = lv_data+20.
          ELSEIF lv_type = zcl_abapgit_ortec_delta=>c_type_ofs_d.
            lv_base_offset = lv_curr_offset
                              - zcl_abapgit_ortec_delta=>get_offset( CHANGING cv_data = lv_data ).
          ENDIF.

          lv_zlib = lv_data(2).
          IF lv_zlib <> c_zlib AND lv_zlib <> c_zlib_hmm.
            zcx_abapgit_ortec_git=>raise( 'Streaming pack: unexpected zlib header' ).
          ENDIF.
          lv_data = lv_data+2.

          zcl_abapgit_ortec_pack_dec=>stream_decompress(
            EXPORTING iv_data           = lv_data
                      iv_expected_len   = lv_expected
            IMPORTING ev_decompressed   = lv_decompressed
                      ev_compressed_len = lv_compressed_len ).

          IF lv_expected <> xstrlen( lv_decompressed ).
            zcx_abapgit_ortec_git=>raise( 'Streaming pack: decompression failed' ).
          ENDIF.

          lv_data = lv_data+lv_compressed_len.
          lv_data = lv_data+4. " skip this object's own adler32 trailer

          CLEAR ls_meta.
          ls_meta-obj_index   = lv_uindex.
          ls_meta-pack_offset = lv_curr_offset.
          ls_meta-obj_type    = lv_type.
          ls_meta-obj_size    = xstrlen( lv_decompressed ).

          IF lv_type = zif_abapgit_git_definitions=>c_type-ref_d
              OR lv_type = zcl_abapgit_ortec_delta=>c_type_ofs_d.
            " Delta object: final identity is unknown until resolved against
            " its base (a later phase's job) - persist the raw delta bytes
            " under a temporary key, matching the existing non-streaming
            " decoder's own temp-key convention for in-progress delta rows.
            lv_temp_sha1 = |{ lv_pack_id }{ lv_uindex WIDTH = 8 PAD = '0' }|.
            zcl_abapgit_ortec_obj_store=>store_object(
              iv_repo_key = iv_repo_key
              iv_sha1     = lv_temp_sha1
              iv_type     = lv_type
              iv_data     = lv_decompressed
              iv_pack_id  = lv_pack_id
              iv_status   = c_status_incomplete ).
            CLEAR lv_decompressed.

            ls_meta-temp_key    = lv_temp_sha1.
            ls_meta-delta_base  = lv_ref_delta.
            ls_meta-base_offset = lv_base_offset.
            ls_meta-is_resolved = abap_false.
          ELSE.
            " Non-delta object: identity is already known - persist directly
            " under its real SHA1 and free the buffer immediately.
            lv_sha1 = zcl_abapgit_hash=>sha1(
              iv_type = lv_type
              iv_data = lv_decompressed ).
            zcl_abapgit_ortec_obj_store=>store_object(
              iv_repo_key = iv_repo_key
              iv_sha1     = lv_sha1
              iv_type     = lv_type
              iv_data     = lv_decompressed
              iv_pack_id  = lv_pack_id
              iv_status   = c_status_incomplete ).
            CLEAR lv_decompressed.

            ls_meta-sha1        = lv_sha1.
            ls_meta-is_resolved = abap_true.
          ENDIF.

          APPEND ls_meta TO rt_meta.
        ENDDO.

        lv_len = xstrlen( iv_data ) - 20.
        " Offset/length notation on an XSTRING cannot be used inline as a
        " method-call actual parameter - must be a plain assignment first,
        " then cleared immediately after use (see zcl_abapgit_ortec_pack_dec's
        " own trailer check for the same, already-documented rule).
        lv_xstring = iv_data(lv_len).
        lv_sha1 = zcl_abapgit_hash=>sha1_raw( lv_xstring ).
        CLEAR lv_xstring.
        IF to_upper( lv_sha1 ) <> lv_data.
          zcx_abapgit_ortec_git=>raise( |Streaming pack: trailer SHA1 doesn't match| ).
        ENDIF.

        UPDATE zaog_obj_store SET status = 'R'
          WHERE repo_key = iv_repo_key
            AND pack_id  = lv_pack_id
            AND status   = c_status_incomplete.
        COMMIT WORK.

      CATCH zcx_abapgit_exception INTO lx_abapgit.
        cleanup_incomplete( iv_repo_key = iv_repo_key iv_pack_id = lv_pack_id ).
        COMMIT WORK.
        CLEAR rt_meta.
        zcx_abapgit_ortec_git=>raise( lx_abapgit->get_text( ) ).
      CATCH zcx_abapgit_ortec_git INTO lx_ortec.
        cleanup_incomplete( iv_repo_key = iv_repo_key iv_pack_id = lv_pack_id ).
        COMMIT WORK.
        CLEAR rt_meta.
        RAISE EXCEPTION lx_ortec.
    ENDTRY.
  ENDMETHOD.
ENDCLASS.
