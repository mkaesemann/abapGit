"! <p class="shorttext synchronized">ORTEC Incremental Pack Decoder with Persistence</p>
"! Decodes a Git packfile object by object, persisting intermediate state so that
"! a timeout or crash can be resumed without re-downloading from the remote.
"! <ul>
"!   <li>Raw packfile → <em>ZAOG_RAW_PACK</em> (survives HTTP timeout)</li>
"!   <li>Per-object progress → <em>ZAOG_PACK_IDX</em> + <em>ZAOG_FETCH_SESS</em></li>
"!   <li>Fully-resolved objects → <em>ZAOG_OBJ_STORE</em></li>
"! </ul>
CLASS zcl_abapgit_ortec_pack_dec DEFINITION
  PUBLIC FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    TYPES ty_repo_key   TYPE c LENGTH 12.
    TYPES ty_session_id TYPE c LENGTH 32.
    TYPES ty_pack_id    TYPE c LENGTH 32.

    "! Decode a raw packfile and persist all results for crash-safe resume.
    "! <p>If <em>it_objects</em> is supplied the decode step is skipped and the
    "! pre-decoded objects are persisted directly (the raw packfile is still
    "! stored so a future resume can re-decode if needed).</p>
    "! <p>Delta bases for thin-pack resolution are fetched automatically via a
    "! targeted bulk SELECT on {@link TABL:zaog_obj_store} scoped to the SHA1s
    "! actually referenced in this pack — no full-store load on the normal path.
    "! Full packs trigger no SELECT at all. A full-store fallback SELECT fires
    "! only if a delta base is absent (edge case after incomplete earlier fetches).</p>
    "! @parameter iv_data |
    "! Raw packfile bytes received from the Git server
    "! @parameter iv_repo_key |
    "! Repository key (12-char identifier)
    "! @parameter iv_commit_interval |
    "! Commit every N objects during the persist phase (default 50)
    "! @parameter it_objects |
    "! Pre-decoded objects; if supplied the pack decode is skipped
    "! @parameter rt_objects |
    "! Fully decoded and delta-resolved objects (base + new)
    "! @raising zcx_abapgit_exception |
    "! On decode or persistence error
    CLASS-METHODS decode_and_persist
      IMPORTING iv_data            TYPE xstring
                iv_repo_key        TYPE ty_repo_key
                iv_commit_interval TYPE i                                      DEFAULT 50
                it_objects         TYPE zif_abapgit_definitions=>ty_objects_tt OPTIONAL
      RETURNING VALUE(rt_objects)  TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_exception.

    "! Resume an incomplete decode session started by a previous call to
    "! {@link METH:decode_and_persist} that was aborted (timeout/short dump).
    "! Loads the raw packfile from ZAOG_RAW_PACK, re-decodes it in memory,
    "! and persists only the objects that were not yet stored.
    "! Returns an empty table if no active session exists.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter rt_objects |
    "! Decoded objects (all, including already-stored ones)
    "! @raising zcx_abapgit_exception |
    "! On decode error
    CLASS-METHODS resume_decode
      IMPORTING iv_repo_key       TYPE ty_repo_key
      RETURNING VALUE(rt_objects) TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_exception.

    CLASS-METHODS decode_commits_only
      IMPORTING iv_data           TYPE xstring
      RETURNING VALUE(rt_objects) TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_exception.

  PROTECTED SECTION.
    CONSTANTS c_pack_start             TYPE x LENGTH 4 VALUE '5041434B' ##NO_TEXT.
    CONSTANTS c_zlib                   TYPE x LENGTH 2 VALUE '789C' ##NO_TEXT.
    CONSTANTS c_zlib_hmm               TYPE x LENGTH 2 VALUE '7801' ##NO_TEXT.
    CONSTANTS c_version                TYPE x LENGTH 4 VALUE '00000002' ##NO_TEXT.
    "! Interval in seconds between timeout-avoidance checks inside the decode loop.
    "! Default 300 s = 5 minutes.  Increase for systems with longer WP timeouts.
    CONSTANTS c_redispatch_interval    TYPE i          VALUE 300 ##NO_TEXT.
    "! Optimization #1: Use kernel decompress + Adler32-scan to find compressed
    "! stream boundaries instead of pure-ABAP zlib inflate.
    "! Toggle: abap_true = enabled (fast kernel path), abap_false = disabled (legacy).
    "! NOTE: Adler32 scan has a small false-positive risk if the 4-byte checksum
    "! value appears in the compressed data before the actual trailer.
    CONSTANTS c_opt1_kernel_adler_scan TYPE abap_bool  VALUE abap_false ##NO_TEXT.
    "! Optimization #6: CL_ABAP_UNGZIP_BINARY_STREAM-based streaming decompression.
    "! Kernel-backed, returns exact consumed-length via gzip_in_off — eliminates
    "! both re-compress trick and Adler32 guessing. Works for 789C and 7801.
    "! When enabled, takes priority over #1 and legacy paths.
    "! Uses a fixed 64 KB TYPE X output buffer to prevent unbounded allocation
    "! (SET_OUT_BUF reliably derives ME->OUT_BUF_LEN from DESCRIBE FIELD LENGTH
    "! for TYPE X, independent of EXPORTING-param runtime semantics).
    CONSTANTS c_opt6_stream_decompress TYPE abap_bool  VALUE abap_true ##NO_TEXT.
    "! Fixed output buffer size for opt6 streaming decompression (bytes).
    "! The kernel fills this buffer per chunk and calls the output handler.
    "! 65535 = max TYPE X flat field length; good trade-off between memory
    "! and callback frequency.
    CONSTANTS c_opt6_out_buf_size      TYPE i          VALUE 65535 ##NO_TEXT.

    "! Decode a raw packfile into an in-memory object table.
    "! All delta references are resolved before returning.
    "! @parameter iv_data |
    "! Raw packfile bytes (full PACK stream including header and trailing SHA1)
    "! @parameter iv_repo_key |
    "! Repository key used for persistence and resume checkpoints
    "! @parameter iv_pack_id |
    "! Pack identifier linked to ZAOG_RAW_PACK / ZAOG_PACK_IDX / ZAOG_OBJ_STORE
    "! @parameter iv_session_id |
    "! Active fetch session that stores curr_offset and decoded-object progress
    "! @parameter iv_commit_interval |
    "! Commit frequency for checkpoint writes during the decode loop
    "! @parameter rt_objects |
    "! Decoded and delta-resolved objects; may include base objects fetched for
    "! delta resolution — only new objects are persisted
    "! @raising zcx_abapgit_exception |
    "! On format or checksum error
    CLASS-METHODS resumable_decode
      IMPORTING iv_data            TYPE xstring
                iv_repo_key        TYPE ty_repo_key
                iv_pack_id         TYPE ty_pack_id
                iv_session_id      TYPE ty_session_id
                iv_commit_interval TYPE i DEFAULT 50
      RETURNING VALUE(rt_objects)  TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_exception.

  PRIVATE SECTION.
    CLASS-DATA gv_resume_branch TYPE string.
    CLASS-DATA gv_resume_deepen TYPE i.

    CLASS-METHODS acquire_repo_lock
      IMPORTING iv_repo_key       TYPE ty_repo_key
                iv_max_attempts   TYPE i DEFAULT 7
                iv_base_wait_ms   TYPE i DEFAULT 50
      RETURNING VALUE(rv_lock_id) TYPE ty_session_id
      RAISING   zcx_abapgit_exception.

    CLASS-METHODS release_repo_lock
      IMPORTING iv_lock_id TYPE ty_session_id.

    CLASS-METHODS get_type
      IMPORTING iv_x           TYPE x
      RETURNING VALUE(rv_type) TYPE zif_abapgit_git_definitions=>ty_type
      RAISING   zcx_abapgit_exception.

    CLASS-METHODS get_length
      EXPORTING ev_length TYPE i
      CHANGING  cv_data   TYPE xstring.

    CLASS-METHODS zlib_decompress
      CHANGING cv_data           TYPE xstring
               cv_decompressed   TYPE xstring
               cv_compressed_len TYPE i OPTIONAL
      RAISING  zcx_abapgit_exception.

    "! Kernel-based streaming decompression using CL_ABAP_UNGZIP_BINARY_STREAM.
    "! Returns both decompressed data and exact consumed compressed byte count.
    "! Works for any DEFLATE stream (789C / 7801) without header-specific tricks.
    "!
    "! @parameter iv_data |
    "! @parameter iv_expected_len |
    "! @parameter ev_decompressed |
    "! @parameter ev_compressed_len |
    "! @raising zcx_abapgit_exception |
    CLASS-METHODS stream_decompress
      IMPORTING iv_data           TYPE xstring
                iv_expected_len   TYPE i
      EXPORTING ev_decompressed   TYPE xstring
                ev_compressed_len TYPE i
      RAISING   zcx_abapgit_exception.

    CLASS-METHODS create_session
      IMPORTING iv_repo_key          TYPE ty_repo_key
                iv_obj_total         TYPE i
                iv_pack_id           TYPE ty_pack_id
      RETURNING VALUE(rv_session_id) TYPE ty_session_id.

    CLASS-METHODS update_session_progress
      IMPORTING iv_session_id  TYPE ty_session_id
                iv_obj_done    TYPE i
                iv_curr_offset TYPE i OPTIONAL.

    CLASS-METHODS fail_session
      IMPORTING iv_session_id TYPE ty_session_id
                iv_obj_done   TYPE i.

    CLASS-METHODS complete_session
      IMPORTING iv_session_id TYPE ty_session_id.

    CLASS-METHODS find_active_session
      IMPORTING iv_repo_key     TYPE ty_repo_key
      EXPORTING ev_session_id   TYPE ty_session_id
                ev_pack_id      TYPE ty_pack_id
                ev_obj_done     TYPE i
                ev_branch_name  TYPE string
                ev_deepen_level TYPE i.

    CLASS-METHODS persist_objects
      IMPORTING iv_repo_key        TYPE ty_repo_key
                iv_pack_id         TYPE ty_pack_id
                iv_session_id      TYPE ty_session_id
                iv_skip_count      TYPE i DEFAULT 0
                iv_commit_interval TYPE i DEFAULT 50
                it_objects         TYPE zif_abapgit_definitions=>ty_objects_tt.

    CLASS-METHODS complete_pack
      IMPORTING iv_repo_key TYPE ty_repo_key
                iv_pack_id  TYPE ty_pack_id
                iv_count    TYPE i.

ENDCLASS.


CLASS zcl_abapgit_ortec_pack_dec IMPLEMENTATION.
  METHOD decode_commits_only.
    " Decode a pack and return only commit objects, using stream_decompress.
    " Designed for filter tree:0 responses: small pack, commits only.

    IF c_opt6_stream_decompress = abap_false.
      zcx_abapgit_exception=>raise(
        'decode_commits_only requires kernel streaming support (opt6)' ).
    ENDIF.

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
    DATA ls_object         TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_uindex         TYPE sy-index.

    lv_data = iv_data.

    IF xstrlen( lv_data ) < 12.
      zcx_abapgit_exception=>raise( 'decode_commits_only: pack too short' ).
    ENDIF.
    IF lv_data(4) <> c_pack_start.
      zcx_abapgit_exception=>raise(
        |decode_commits_only: bad PACK header { lv_data(4) }| ).
    ENDIF.
    lv_data = lv_data+4.

    IF lv_data(4) <> c_version.
      zcx_abapgit_exception=>raise(
        |decode_commits_only: unsupported pack version { lv_data(4) }| ).
    ENDIF.
    lv_data = lv_data+4.

    lv_xstring = lv_data(4).
    lv_objects = zcl_abapgit_convert=>xstring_to_int( lv_xstring ).
    lv_data = lv_data+4.

    DO lv_objects TIMES.
      lv_uindex = sy-index.
      lv_x = lv_data(1).
      lv_type = get_type( lv_x ).

      get_length(
        IMPORTING ev_length = lv_expected
        CHANGING  cv_data   = lv_data ).

      IF lv_type = zif_abapgit_git_definitions=>c_type-ref_d.
        lv_ref_delta = lv_data(20).
        TRANSLATE lv_ref_delta TO LOWER CASE.
        lv_data = lv_data+20.
      ELSE.
        CLEAR lv_ref_delta.
      ENDIF.

      " Strip 2-byte zlib header (CMF + FLG)
      lv_zlib = lv_data(2).
      IF lv_zlib <> c_zlib AND lv_zlib <> c_zlib_hmm.
        zcx_abapgit_exception=>raise(
          |decode_commits_only: unexpected zlib header { lv_zlib }| ).
      ENDIF.
      lv_data = lv_data+2.

      " Kernel streaming decompress; ev_compressed_len = DEFLATE bytes only
      stream_decompress(
        EXPORTING iv_data         = lv_data
                  iv_expected_len = lv_expected
        IMPORTING ev_decompressed   = lv_decompressed
                  ev_compressed_len = lv_compressed_len ).

      lv_data = lv_data+lv_compressed_len.  " advance past DEFLATE
      lv_data = lv_data+4.                  " skip 4-byte Adler32

      " Accumulate all objects; non-commits needed for potential delta bases
      CLEAR ls_object.
      ls_object-type  = lv_type.
      ls_object-data  = lv_decompressed.
      ls_object-index = lv_uindex.
      IF lv_type = zif_abapgit_git_definitions=>c_type-ref_d.
        ls_object-sha1 = lv_ref_delta.  " already lowercased
      ELSE.
        ls_object-sha1 = zcl_abapgit_hash=>sha1(
          iv_type = lv_type
          iv_data = lv_decompressed ).
      ENDIF.
      APPEND ls_object TO rt_objects.
    ENDDO.

    " Resolve REF_DELTA objects in-place
    zcl_abapgit_git_delta=>decode_deltas( CHANGING ct_objects = rt_objects ).

    " Discard non-commit objects
    DELETE rt_objects WHERE type <> zif_abapgit_git_definitions=>c_type-commit.

  ENDMETHOD.

  METHOD decode_and_persist.
    DATA lv_repo_lock_id TYPE zcl_abapgit_ortec_pack_raw=>ty_session_id.
    DATA lv_pack_id      TYPE ty_pack_id.
    DATA lv_obj_count_x  TYPE xstring.
    DATA lv_obj_count    TYPE i.
    DATA lv_ts           TYPE timestampl.
    DATA ls_meta         TYPE zaog_pack_meta.
    DATA lv_session_id   TYPE ty_session_id.

    lv_repo_lock_id = acquire_repo_lock( iv_repo_key = iv_repo_key ).

    TRY.

        " Generate a unique pack ID for this packfile
        TRY.
            lv_pack_id = cl_system_uuid=>create_uuid_c32_static( ).
          CATCH cx_uuid_error.
            zcx_abapgit_exception=>raise( 'Failed to generate pack UUID' ).
        ENDTRY.

        " STEP 1: Store raw packfile immediately.
        "   This is the most critical step: if a timeout occurs during decode or
        "   persist, resume_decode can reload this instead of repeating the HTTP call.
        zcl_abapgit_ortec_pack_raw=>store(
            iv_repo_key = iv_repo_key
            iv_pack_id  = lv_pack_id
            iv_raw_data = iv_data ).
        COMMIT WORK.

        " STEP 2: Decode the packfile (or use pre-decoded objects if supplied).
        "   For raw pack decode we resume from persisted checkpoints in the same
        "   session (curr_offset + already decoded temp objects).
        IF it_objects IS SUPPLIED AND it_objects IS NOT INITIAL.
          rt_objects = it_objects.
        ENDIF.

        IF rt_objects IS INITIAL AND xstrlen( iv_data ) >= 12.
          lv_obj_count_x = iv_data+8(4).
          lv_obj_count = zcl_abapgit_convert=>xstring_to_int( lv_obj_count_x ).
        ELSE.
          lv_obj_count = lines( rt_objects ).
        ENDIF.

        " STEP 3: Register pack metadata (status P = in progress)
        GET TIME STAMP FIELD lv_ts.
        ls_meta-repo_key    = iv_repo_key.
        ls_meta-pack_id     = lv_pack_id.
        ls_meta-obj_count   = lv_obj_count.
        ls_meta-obj_decoded = 0.
        ls_meta-total_size  = xstrlen( iv_data ).
        ls_meta-status      = 'P'.
        ls_meta-raw_stored  = abap_true.
        ls_meta-received_at = lv_ts.
        MODIFY zaog_pack_meta FROM ls_meta.

        " STEP 4: Create fetch session for crash-resume tracking
        lv_session_id = create_session(
                            iv_repo_key  = iv_repo_key
                            iv_obj_total = lv_obj_count
                            iv_pack_id   = lv_pack_id ).
        COMMIT WORK.

        IF rt_objects IS INITIAL.
          rt_objects = resumable_decode(
                           iv_data            = iv_data
                           iv_repo_key        = iv_repo_key
                           iv_pack_id         = lv_pack_id
                           iv_session_id      = lv_session_id
                           iv_commit_interval = iv_commit_interval ).
          " Keep lv_obj_count as read from the pack header above.  Do NOT override
          " with lines( rt_objects ) since rt_objects may include base objects
          " fetched for delta resolution and would give an inflated count.
        ENDIF.

        " STEP 5: Persist decoded objects.
        "   - raw decode path: persisted incrementally inside resumable_decode
        "   - pre-decoded path: persist here
        IF it_objects IS SUPPLIED AND it_objects IS NOT INITIAL.
          persist_objects(
              iv_repo_key        = iv_repo_key
              iv_pack_id         = lv_pack_id
              iv_session_id      = lv_session_id
              iv_skip_count      = 0
              iv_commit_interval = iv_commit_interval
              it_objects         = rt_objects ).
        ENDIF.

        " STEP 6: Mark pack and session as complete
        complete_pack(
            iv_repo_key = iv_repo_key
            iv_pack_id  = lv_pack_id
            iv_count    = lv_obj_count ).
        complete_session( lv_session_id ).

        " STEP 7: Raw packfile no longer needed — all objects are in OBJ_STORE
        zcl_abapgit_ortec_pack_raw=>delete(
            iv_repo_key = iv_repo_key
            iv_pack_id  = lv_pack_id ).
        COMMIT WORK.

        release_repo_lock( lv_repo_lock_id ).
      CATCH zcx_abapgit_exception INTO DATA(lx_decode).
        release_repo_lock( lv_repo_lock_id ).
        RAISE EXCEPTION lx_decode.
    ENDTRY.

    " Invalidate the in-memory obj_store cache: we just wrote new rows to
    " zaog_obj_store directly (bypassing store_object/store_objects which
    " would have called invalidate_cache themselves). Subsequent callers of
    " get_object/get_all_objects must re-read from DB to see the new data.
    zcl_abapgit_ortec_obj_store=>invalidate_cache( ).

  ENDMETHOD.

  METHOD resume_decode.
    DATA lv_repo_lock_id TYPE zcl_abapgit_ortec_pack_raw=>ty_session_id.
    DATA lv_session_id   TYPE ty_session_id.
    DATA lv_pack_id      TYPE ty_pack_id.
    DATA lv_obj_done     TYPE i.
    DATA lv_raw          TYPE xstring.

    lv_repo_lock_id = acquire_repo_lock( iv_repo_key = iv_repo_key ).

    TRY.
        " Find an active (incomplete) session for this repository
        find_active_session(
          EXPORTING
            iv_repo_key     = iv_repo_key
          IMPORTING
            ev_session_id   = lv_session_id
            ev_pack_id      = lv_pack_id
            ev_obj_done     = lv_obj_done
          " TODO: variable is assigned but never used (ABAP cleaner)
            ev_branch_name  = DATA(lv_branch_name)
          " TODO: variable is assigned but never used (ABAP cleaner)
            ev_deepen_level = DATA(lv_deepen_level) ).
        IF lv_session_id IS INITIAL.
          release_repo_lock( lv_repo_lock_id ).
          RETURN. " Nothing to resume
        ENDIF.

        " Load the stored raw packfile (avoids re-downloading from remote)
        TRY.
            lv_raw = zcl_abapgit_ortec_pack_raw=>load(
                         iv_repo_key = iv_repo_key
                         iv_pack_id  = lv_pack_id ).
          CATCH zcx_abapgit_exception.
            " Raw pack was lost — cannot resume; mark session failed
            fail_session( iv_session_id = lv_session_id iv_obj_done = lv_obj_done ).
            COMMIT WORK.
            release_repo_lock( lv_repo_lock_id ).
            RETURN.
        ENDTRY.

        " Continue decoding from last checkpoint (curr_offset + temp decoded rows)
        rt_objects = resumable_decode(
                         iv_data       = lv_raw
                         iv_repo_key   = iv_repo_key
                         iv_pack_id    = lv_pack_id
                         iv_session_id = lv_session_id ).

        complete_pack(
            iv_repo_key = iv_repo_key
            iv_pack_id  = lv_pack_id
            iv_count    = lines( rt_objects ) ).
        complete_session( lv_session_id ).

        " Raw pack no longer needed
        zcl_abapgit_ortec_pack_raw=>delete(
            iv_repo_key = iv_repo_key
            iv_pack_id  = lv_pack_id ).
        COMMIT WORK.

        release_repo_lock( lv_repo_lock_id ).
      CATCH zcx_abapgit_exception INTO DATA(lx_resume).
        release_repo_lock( lv_repo_lock_id ).
        RAISE EXCEPTION lx_resume.
    ENDTRY.

    " Invalidate the in-memory obj_store cache (see decode_and_persist).
    zcl_abapgit_ortec_obj_store=>invalidate_cache( ).
  ENDMETHOD.

  METHOD acquire_repo_lock.
    " Uses SAP enqueue server lock object EZAOG_REPO_LOCK (SE11).
    " SESSION_ID is set to iv_repo_key (C12 padded to C32) — one unique lock
    " entry per repository.  _SCOPE = '2': survives COMMIT WORK inside the
    " decode loop but is automatically released by the enqueue server if the
    " work process ends (crash, timeout, short dump) — no stale lock possible.

    DATA lv_jitter_ms TYPE i.
    DATA lv_wait_s    TYPE f.

    " Return the repo key as the lock token; release_repo_lock passes it back
    " as SESSION_ID to DEQUEUE (C12 → C32 left-aligned, same value).
    rv_lock_id = iv_repo_key.

    IF iv_max_attempts <= 0.
      zcx_abapgit_exception=>raise( 'Repo lock: invalid max attempts' ).
    ENDIF.

    DO iv_max_attempts TIMES.
      CALL FUNCTION 'ENQUEUE_EZAOG_REPO_LOCK'
        EXPORTING
          mode_zaog_fetch_sess = 'E'
          session_id           = rv_lock_id
          _scope               = '2'
          _wait                = space
          _collect             = space
        EXCEPTIONS
          foreign_lock         = 1
          system_failure       = 2
          OTHERS               = 3.

      CASE sy-subrc.
        WHEN 0.
          RETURN. " Lock acquired
        WHEN 1.   " foreign_lock — another process holds the lock; back off and retry
          lv_jitter_ms = ( sy-index * 31 + strlen( iv_repo_key ) * 17 ) MOD 41.
          lv_wait_s = ( iv_base_wait_ms * ( 2 ** ( sy-index - 1 ) ) + lv_jitter_ms ) / 1000.
          IF lv_wait_s > 2.
            lv_wait_s = 2.
          ENDIF.
          WAIT UP TO lv_wait_s SECONDS.
        WHEN OTHERS. " system_failure or unexpected return code
          zcx_abapgit_exception=>raise( |Repo lock system failure for key { iv_repo_key } (sy-subrc={ sy-subrc })| ).
      ENDCASE.
    ENDDO.

    zcx_abapgit_exception=>raise( |Repo lock timeout for key { iv_repo_key }| ).
  ENDMETHOD.

  METHOD release_repo_lock.
    " Releases the enqueue server lock set by acquire_repo_lock.
    " Safe to call even if the lock was already released (DEQUEUE is idempotent).
    IF iv_lock_id IS INITIAL.
      RETURN.
    ENDIF.

    CALL FUNCTION 'DEQUEUE_EZAOG_REPO_LOCK'
      EXPORTING
        mode_zaog_fetch_sess = 'E'
        session_id           = iv_lock_id
        _scope               = '2'
        _synchron            = space
        _collect             = space.
  ENDMETHOD.

  METHOD persist_objects.
    DATA lv_ts    TYPE timestampl.
    DATA lv_count TYPE i.
    DATA ls_row   TYPE zaog_obj_store.
    DATA lt_rows  TYPE STANDARD TABLE OF zaog_obj_store.
    DATA ls_idx   TYPE zcl_abapgit_ortec_pack_index=>ty_index_entry.
    DATA lt_idx   TYPE zcl_abapgit_ortec_pack_index=>tty_index_entries.
    FIELD-SYMBOLS <ls_obj> LIKE LINE OF it_objects.

    GET TIME STAMP FIELD lv_ts.

    LOOP AT it_objects ASSIGNING <ls_obj>.
      lv_count += 1.
      " Skip objects that were already persisted in a previous run
      IF lv_count <= iv_skip_count.
        CONTINUE.
      ENDIF.

      " 1. Accumulate object store row
      CLEAR ls_row.
      ls_row-repo_key   = iv_repo_key.
      ls_row-obj_sha1   = <ls_obj>-sha1.
      ls_row-obj_type   = <ls_obj>-type.
      ls_row-obj_data   = <ls_obj>-data.
      ls_row-obj_size   = xstrlen( <ls_obj>-data ).
      ls_row-pack_id    = iv_pack_id.
      ls_row-created_at = lv_ts.
      ls_row-status     = 'R'. " R = resolved
      APPEND ls_row TO lt_rows.

      " 2. Accumulate pack index entry (obj_index = sequential position in pack)
      CLEAR ls_idx.
      ls_idx-obj_index  = <ls_obj>-index.
      ls_idx-obj_sha1   = <ls_obj>-sha1.
      ls_idx-obj_type   = <ls_obj>-type.
      ls_idx-uncomp_len = xstrlen( <ls_obj>-data ).
      ls_idx-adler32    = <ls_obj>-adler32.
      ls_idx-dec_status = 'D'. " D = decoded
      APPEND ls_idx TO lt_idx.

      " 3. Periodic commit: flush batched rows, update session progress, commit
      IF lv_count MOD iv_commit_interval = 0.
        MODIFY zaog_obj_store FROM TABLE lt_rows.
        CLEAR lt_rows.
        TRY.
            zcl_abapgit_ortec_pack_index=>store_entries(
                iv_repo_key = iv_repo_key
                iv_pack_id  = iv_pack_id
                it_entries  = lt_idx ).
          CATCH zcx_abapgit_ortec_git. " non-critical; index is for resume only
        ENDTRY.
        CLEAR lt_idx.
        update_session_progress(
            iv_session_id = iv_session_id
            iv_obj_done   = lv_count ).
        COMMIT WORK.
        GET TIME STAMP FIELD lv_ts.
      ENDIF.
    ENDLOOP.

    " Flush remaining rows
    IF lt_rows IS NOT INITIAL.
      MODIFY zaog_obj_store FROM TABLE lt_rows.
    ENDIF.
    IF lt_idx IS NOT INITIAL.
      TRY.
          zcl_abapgit_ortec_pack_index=>store_entries(
              iv_repo_key = iv_repo_key
              iv_pack_id  = iv_pack_id
              it_entries  = lt_idx ).
        CATCH zcx_abapgit_ortec_git.
      ENDTRY.
    ENDIF.

    update_session_progress(
        iv_session_id = iv_session_id
        iv_obj_done   = lv_count ).
    COMMIT WORK.
  ENDMETHOD.

  METHOD complete_pack.
    UPDATE zaog_pack_meta
      SET status = 'C' obj_decoded = iv_count
      WHERE repo_key = iv_repo_key AND pack_id = iv_pack_id.
  ENDMETHOD.

  METHOD create_session.
    IF gv_resume_deepen IS INITIAL.
      gv_resume_deepen = 1.
    ENDIF.

    rv_session_id = zcl_abapgit_ortec_pack_raw=>create_session(
                        iv_repo_key     = iv_repo_key
                        iv_pack_id      = iv_pack_id
                        iv_obj_total    = iv_obj_total
                        iv_branch_name  = gv_resume_branch
                        iv_deepen_level = gv_resume_deepen ).
  ENDMETHOD.

  METHOD update_session_progress.
    zcl_abapgit_ortec_pack_raw=>update_session_progress(
        iv_session_id  = iv_session_id
        iv_obj_done    = iv_obj_done
        iv_curr_offset = iv_curr_offset ).
  ENDMETHOD.

  METHOD fail_session.
    zcl_abapgit_ortec_pack_raw=>fail_session(
        iv_session_id = iv_session_id
        iv_obj_done   = iv_obj_done ).
  ENDMETHOD.

  METHOD complete_session.
    zcl_abapgit_ortec_pack_raw=>complete_session( iv_session_id ).
  ENDMETHOD.

  METHOD find_active_session.
    DATA ls_sess TYPE zcl_abapgit_ortec_pack_raw=>ty_session_info.

    CLEAR: ev_session_id,
           ev_pack_id,
           ev_obj_done,
           ev_branch_name,
           ev_deepen_level.
    ls_sess = zcl_abapgit_ortec_pack_raw=>find_active_session( iv_repo_key ).
    IF ls_sess-session_id IS NOT INITIAL.
      ev_session_id = ls_sess-session_id.
      ev_pack_id    = ls_sess-pack_id.
      ev_obj_done   = ls_sess-obj_done.
      ev_branch_name = ls_sess-branch_name.
      ev_deepen_level = ls_sess-deepen_level.
    ENDIF.
  ENDMETHOD.

  METHOD resumable_decode.
    DATA lv_commit_interval TYPE i.
    DATA lv_obj_done        TYPE i.
    DATA lv_start_offset    TYPE i.
    DATA lv_data            TYPE xstring.
    DATA lv_xstring         TYPE xstring.
    DATA lv_objects         TYPE i.
    DATA lt_done_idx        TYPE STANDARD TABLE OF zaog_pack_idx.
    DATA ls_done_idx        TYPE zaog_pack_idx.
    DATA ls_tmp_obj         TYPE zaog_obj_store.
    DATA lv_last_redispatch TYPE timestampl.
    DATA lv_uindex          TYPE sy-index.
    DATA lv_curr_offset     TYPE i.
    DATA lv_x               TYPE x LENGTH 1.
    DATA lv_type            TYPE zif_abapgit_git_definitions=>ty_type.
    DATA lv_expected        TYPE i.
    DATA lv_ref_delta       TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_zlib            TYPE x LENGTH 2.
    DATA lv_decompressed    TYPE xstring.
    DATA lv_compressed_len  TYPE i.
    DATA lv_decompress_len  TYPE i.
    DATA lv_compressed      TYPE xstring.
    DATA lv_adler_scan      TYPE zif_abapgit_git_definitions=>ty_adler32.
    DATA lv_scan_start      TYPE i.
    DATA lv_scan_found      TYPE abap_bool.
    DATA lv_scan_limit      TYPE i.
    DATA lv_scan_offset     TYPE i.
    DATA lv_temp_sha1       TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_ts              TYPE timestampl.
    DATA ls_row             TYPE zaog_obj_store.
    DATA lt_obj_batch       TYPE STANDARD TABLE OF zaog_obj_store.
    DATA ls_idx             TYPE zcl_abapgit_ortec_pack_index=>ty_index_entry.
    DATA lt_idx             TYPE zcl_abapgit_ortec_pack_index=>tty_index_entries.
    DATA lv_redispatch_now  TYPE timestampl.
    DATA lv_elapsed         TYPE decfloat34.
    DATA lv_len             TYPE i.
    DATA lv_sha1            TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lt_final_rows      TYPE STANDARD TABLE OF zaog_obj_store.
    DATA ls_object          LIKE LINE OF rt_objects.
    " SHA1 set of base objects to suppress re-persisting them in the final promote step.
    DATA lt_base_shas       TYPE HASHED TABLE OF zif_abapgit_git_definitions=>ty_sha1
                                  WITH UNIQUE KEY table_line.
    DATA lt_delta_bases     TYPE SORTED TABLE OF zif_abapgit_git_definitions=>ty_sha1
                                  WITH UNIQUE KEY table_line.

    lv_commit_interval = iv_commit_interval.
    IF lv_commit_interval <= 0.
      lv_commit_interval = 50.
    ENDIF.

    " Resume checkpoint from active decode session
    DATA(ls_session) = zcl_abapgit_ortec_pack_raw=>get_session( iv_session_id ).
    IF ls_session-session_id IS INITIAL.
      CLEAR: lv_obj_done,
             lv_start_offset.
    ELSE.
      lv_obj_done = ls_session-obj_done.
      lv_start_offset = ls_session-curr_offset.
    ENDIF.

    lv_data = iv_data.

    IF xstrlen( lv_data ) < 12.
      zcx_abapgit_exception=>raise( |Unexpected pack header, short reply| ).
    ENDIF.
    IF lv_data(4) <> c_pack_start.
      zcx_abapgit_exception=>raise( |Unexpected pack header, { lv_data(4) }| ).
    ENDIF.
    lv_data = lv_data+4.

    IF lv_data(4) <> c_version.
      zcx_abapgit_exception=>raise( |Version not supported, { lv_data(4) }| ).
    ENDIF.
    lv_data = lv_data+4.

    lv_xstring = lv_data(4).
    lv_objects = zcl_abapgit_convert=>xstring_to_int( lv_xstring ).
    lv_data = lv_data+4.

    " Rehydrate already decoded objects for delta resolution at finalization
    IF lv_obj_done > 0.
      SELECT * FROM zaog_pack_idx
        INTO TABLE lt_done_idx
        WHERE repo_key    = iv_repo_key
          AND pack_id     = iv_pack_id
          AND dec_status  = 'P'
          AND obj_index  <= lv_obj_done
        ORDER BY obj_index.

      LOOP AT lt_done_idx INTO ls_done_idx.
        SELECT SINGLE * FROM zaog_obj_store
          INTO ls_tmp_obj
          WHERE repo_key = iv_repo_key
            AND pack_id  = iv_pack_id
            AND obj_sha1 = ls_done_idx-obj_sha1
            AND status   = 'P'.
        IF sy-subrc <> 0.
          CONTINUE.
        ENDIF.

        CLEAR ls_object.
        ls_object-type    = ls_done_idx-obj_type.
        ls_object-data    = ls_tmp_obj-obj_data.
        ls_object-index   = ls_done_idx-obj_index.
        ls_object-adler32 = ls_done_idx-adler32.

        IF ls_object-type = zif_abapgit_git_definitions=>c_type-ref_d.
          ls_object-sha1 = ls_done_idx-delta_base.
        ELSE.
          ls_object-sha1 = zcl_abapgit_hash=>sha1(
                               iv_type = ls_object-type
                               iv_data = ls_object-data ).
        ENDIF.
        APPEND ls_object TO rt_objects.
      ENDLOOP.
    ENDIF.

    IF lv_start_offset <= 0.
      lv_start_offset = 12.
    ENDIF.
    IF lv_start_offset > xstrlen( iv_data ) - 20.
      zcx_abapgit_exception=>raise( |Invalid decode checkpoint offset| ).
    ENDIF.

    lv_data = iv_data+lv_start_offset.

    " Capture baseline timestamp for periodic WP-timeout prevention
    GET TIME STAMP FIELD lv_last_redispatch.

    DO lv_objects - lv_obj_done TIMES.

      lv_uindex = lv_obj_done + sy-index.
      lv_curr_offset = xstrlen( iv_data ) - xstrlen( lv_data ).
      lv_x = lv_data(1).
      lv_type = get_type( lv_x ).

      get_length(
        IMPORTING
          ev_length = lv_expected
        CHANGING
          cv_data   = lv_data ).

      IF lv_type = zif_abapgit_git_definitions=>c_type-ref_d.
        lv_ref_delta = lv_data(20).
        lv_data = lv_data+20.
      ELSE.
        CLEAR lv_ref_delta.
      ENDIF.

      lv_zlib = lv_data(2).
      IF lv_zlib <> c_zlib AND lv_zlib <> c_zlib_hmm.
        zcx_abapgit_exception=>raise( |Unexpected zlib header| ).
      ENDIF.
      lv_data = lv_data+2.

      IF c_opt6_stream_decompress = abap_true.
        " Optimization #6: kernel streaming decompress — works for all zlib headers.
        " Feeds raw DEFLATE bytes (after 2-byte zlib header) to the kernel stream
        " inflater which returns decompressed data + consumed byte count.
        stream_decompress(
          EXPORTING
            iv_data           = lv_data
            iv_expected_len   = lv_expected
          IMPORTING
            ev_decompressed   = lv_decompressed
            ev_compressed_len = lv_compressed_len ).

        IF lv_expected <> xstrlen( lv_decompressed ).
          zcx_abapgit_exception=>raise( |Decompression failed (stream path)| ).
        ENDIF.

        " Advance past compressed data; Adler32 is at lv_compressed_len position
        lv_data = lv_data+lv_compressed_len.

      ELSE.
        CASE lv_zlib.
          WHEN c_zlib.
            cl_abap_gzip=>decompress_binary(
              EXPORTING
                gzip_in     = lv_data
              IMPORTING
                raw_out     = lv_decompressed
                raw_out_len = lv_decompress_len ).

            IF lv_expected <> lv_decompress_len.
              zcx_abapgit_exception=>raise( |Decompression failed| ).
            ENDIF.

            cl_abap_gzip=>compress_binary(
              EXPORTING
                raw_in       = lv_decompressed
              IMPORTING
                gzip_out     = lv_compressed
                gzip_out_len = lv_compressed_len ).

            IF    xstrlen( lv_data )               <= lv_compressed_len
               OR lv_compressed(lv_compressed_len) <> lv_data(lv_compressed_len).
              IF c_opt1_kernel_adler_scan = abap_true.
                " Opt #1 fallback: scan for Adler32 instead of pure-ABAP inflate
                lv_adler_scan = zcl_abapgit_hash=>adler32( lv_decompressed ).
                lv_scan_start = nmax( val1 = 1 val2 = lv_decompress_len / 1032 ).
                lv_scan_found = abap_false.
                lv_scan_limit = xstrlen( lv_data ) - 4.
                lv_scan_offset = lv_scan_start.
                WHILE lv_scan_offset <= lv_scan_limit.
                  IF lv_data+lv_scan_offset(4) = lv_adler_scan.
                    lv_data = lv_data+lv_scan_offset.
                    lv_scan_found = abap_true.
                    EXIT.
                  ENDIF.
                  lv_scan_offset += 1.
                ENDWHILE.
                IF lv_scan_found = abap_false.
                  zlib_decompress(
                    CHANGING
                      cv_data         = lv_data
                      cv_decompressed = lv_decompressed ).
                ENDIF.
              ELSE.
                zlib_decompress(
                  CHANGING
                    cv_data         = lv_data
                    cv_decompressed = lv_decompressed ).
              ENDIF.
            ELSE.
              lv_data = lv_data+lv_compressed_len.
            ENDIF.

          WHEN c_zlib_hmm.
            IF c_opt1_kernel_adler_scan = abap_true.
              " Optimization #1: kernel decompress + Adler32 boundary scan.
              " cl_abap_gzip works for raw DEFLATE regardless of zlib header byte.
              " The only missing piece is consumed-length — recovered by scanning
              " for the known 4-byte Adler32 trailer in the compressed stream.
              cl_abap_gzip=>decompress_binary(
                EXPORTING
                  gzip_in     = lv_data
                IMPORTING
                  raw_out     = lv_decompressed
                  raw_out_len = lv_decompress_len ).

              IF lv_expected <> lv_decompress_len.
                zcx_abapgit_exception=>raise( |Decompression failed (7801 kernel path)| ).
              ENDIF.

              lv_adler_scan = zcl_abapgit_hash=>adler32( lv_decompressed ).

              " Scan for the Adler32 trailer. Start from a minimum offset to avoid
              " false positives in the header area. DEFLATE minimum ratio ~ 1:1032.
              lv_scan_start = nmax( val1 = 1 val2 = lv_decompress_len / 1032 ).
              lv_scan_found = abap_false.
              lv_scan_limit = xstrlen( lv_data ) - 4.

              lv_scan_offset = lv_scan_start.
              WHILE lv_scan_offset <= lv_scan_limit.
                IF lv_data+lv_scan_offset(4) = lv_adler_scan.
                  lv_data = lv_data+lv_scan_offset.
                  lv_scan_found = abap_true.
                  EXIT.
                ENDIF.
                lv_scan_offset += 1.
              ENDWHILE.

              IF lv_scan_found = abap_false.
                " Extremely rare fallback: Adler32 not found — use pure ABAP inflate
                zlib_decompress(
                  CHANGING
                    cv_data         = lv_data
                    cv_decompressed = lv_decompressed ).
              ENDIF.
            ELSE.
              " Legacy path: pure ABAP zlib inflate for 7801 streams
              zlib_decompress(
                CHANGING
                  cv_data         = lv_data
                  cv_decompressed = lv_decompressed ).
            ENDIF.

          WHEN OTHERS.
            zcx_abapgit_exception=>raise( |Unexpected zlib header| ).
        ENDCASE.
      ENDIF. " c_opt6_stream_decompress

      CLEAR ls_object.
      ls_object-adler32 = lv_data(4).
      lv_data = lv_data+4.

      IF lv_type = zif_abapgit_git_definitions=>c_type-ref_d.
        ls_object-sha1 = lv_ref_delta.
        TRANSLATE ls_object-sha1 TO LOWER CASE.
      ELSE.
        ls_object-sha1 = zcl_abapgit_hash=>sha1(
                             iv_type = lv_type
                             iv_data = lv_decompressed ).
      ENDIF.
      ls_object-type  = lv_type.
      ls_object-data  = lv_decompressed.
      ls_object-index = lv_uindex.
      APPEND ls_object TO rt_objects.

      " Persist parsed object payload with a temporary key for resume
      lv_temp_sha1 = |{ iv_pack_id }{ lv_uindex WIDTH = 8 PAD = '0' }|.
      GET TIME STAMP FIELD lv_ts.

      CLEAR ls_row.
      ls_row-repo_key   = iv_repo_key.
      ls_row-pack_id    = iv_pack_id.
      ls_row-obj_sha1   = lv_temp_sha1.
      ls_row-obj_type   = lv_type.
      ls_row-obj_data   = lv_decompressed.
      ls_row-obj_size   = xstrlen( lv_decompressed ).
      ls_row-created_at = lv_ts.
      ls_row-status     = 'P'.
      APPEND ls_row TO lt_obj_batch.

      CLEAR ls_idx.
      ls_idx-obj_index   = lv_uindex.
      ls_idx-obj_sha1    = lv_temp_sha1.
      ls_idx-obj_type    = lv_type.
      ls_idx-pack_offset = lv_curr_offset.
      ls_idx-uncomp_len  = xstrlen( lv_decompressed ).
      ls_idx-adler32     = ls_object-adler32.
      ls_idx-dec_status  = 'P'.
      ls_idx-delta_base  = lv_ref_delta.
      APPEND ls_idx TO lt_idx.

      IF lv_uindex MOD lv_commit_interval = 0.
        " Batch flush: write accumulated temp objects in one DB roundtrip
        IF lt_obj_batch IS NOT INITIAL.
          MODIFY zaog_obj_store FROM TABLE lt_obj_batch.
          CLEAR lt_obj_batch.
        ENDIF.
        TRY.
            zcl_abapgit_ortec_pack_index=>store_entries(
                iv_repo_key = iv_repo_key
                iv_pack_id  = iv_pack_id
                it_entries  = lt_idx ).
          CATCH zcx_abapgit_ortec_git.
        ENDTRY.
        CLEAR lt_idx.

        lv_curr_offset = xstrlen( iv_data ) - xstrlen( lv_data ).
        update_session_progress(
            iv_session_id  = iv_session_id
            iv_obj_done    = lv_uindex
            iv_curr_offset = lv_curr_offset ).
        COMMIT WORK.
      ENDIF.

      " Run centralized timeout avoidance every c_redispatch_interval seconds.
      " The switch can suppress TH_REDISPATCH for SAT tracing.
      GET TIME STAMP FIELD lv_redispatch_now.
      lv_elapsed = cl_abap_tstmp=>subtract(
                       tstmp1 = lv_redispatch_now
                       tstmp2 = lv_last_redispatch ).
      IF lv_elapsed >= c_redispatch_interval.
        zcl_abapgit_ortec_git_switch=>avoid_timeout( ).
        lv_last_redispatch = lv_redispatch_now.
      ENDIF.

    ENDDO.

    " Flush remaining batched temp object rows
    IF lt_obj_batch IS NOT INITIAL.
      MODIFY zaog_obj_store FROM TABLE lt_obj_batch.
      CLEAR lt_obj_batch.
    ENDIF.

    IF lt_idx IS NOT INITIAL.
      zcl_abapgit_ortec_pack_index=>store_entries(
          iv_repo_key = iv_repo_key
          iv_pack_id  = iv_pack_id
          it_entries  = lt_idx ).
    ENDIF.

    lv_curr_offset = xstrlen( iv_data ) - xstrlen( lv_data ).
    update_session_progress(
        iv_session_id  = iv_session_id
        iv_obj_done    = lv_objects
        iv_curr_offset = lv_curr_offset ).
    COMMIT WORK.

    lv_len = xstrlen( iv_data ) - 20.
    lv_xstring = iv_data(lv_len).
    lv_sha1 = zcl_abapgit_hash=>sha1_raw( lv_xstring ).
    IF to_upper( lv_sha1 ) <> lv_data.
      zcx_abapgit_exception=>raise( |SHA1 at end of pack doesn't match| ).
    ENDIF.

    " ── Targeted delta-base prefetch ─────────────────────────────────────────
    " Collect the SHA1s of every OBJ_REF_DELTA base referenced in this pack.
    " Full packs contain no ref_delta objects so lt_delta_bases stays empty.
    LOOP AT rt_objects INTO ls_object.
      IF ls_object-type = zif_abapgit_git_definitions=>c_type-ref_d.
        INSERT ls_object-sha1 INTO TABLE lt_delta_bases.
      ENDIF.
    ENDLOOP.

    " FOR ALL ENTRIES crashes on an empty driving table — skip SELECT for full packs.
    IF lt_delta_bases IS NOT INITIAL.
      SELECT *
        FROM zaog_obj_store
        FOR ALL ENTRIES IN @lt_delta_bases
        WHERE repo_key = @iv_repo_key
          AND obj_sha1 = @lt_delta_bases-table_line
          AND status   = 'R'
        INTO TABLE @DATA(lt_base_fetch).
      LOOP AT lt_base_fetch INTO ls_row.
        CLEAR ls_object.
        ls_object-sha1 = ls_row-obj_sha1.
        ls_object-type = ls_row-obj_type.
        ls_object-data = ls_row-obj_data.
        INSERT ls_object INTO TABLE rt_objects.
        INSERT ls_object-sha1 INTO TABLE lt_base_shas.
      ENDLOOP.

      " Edge-case guard: if any expected base was not found (incomplete store),
      " fall back to a full-repo SELECT and merge only the missing entries.
      " This path is taken at most once per decode — EXIT after full load.
      LOOP AT lt_delta_bases INTO DATA(lv_need).
        READ TABLE lt_base_shas WITH TABLE KEY table_line = lv_need
          TRANSPORTING NO FIELDS.
        IF sy-subrc <> 0.
          SELECT *
            FROM zaog_obj_store
            INTO TABLE @DATA(lt_full_fetch)
            WHERE repo_key = @iv_repo_key
              AND status   = 'R'.
          LOOP AT lt_full_fetch INTO ls_row.
            READ TABLE lt_base_shas WITH TABLE KEY table_line = ls_row-obj_sha1
              TRANSPORTING NO FIELDS.
            IF sy-subrc <> 0.
              CLEAR ls_object.
              ls_object-sha1 = ls_row-obj_sha1.
              ls_object-type = ls_row-obj_type.
              ls_object-data = ls_row-obj_data.
              INSERT ls_object INTO TABLE rt_objects.
              INSERT ls_object-sha1 INTO TABLE lt_base_shas.
            ENDIF.
          ENDLOOP.
          EXIT. " One full load is sufficient
        ENDIF.
      ENDLOOP.
    ENDIF.

    zcl_abapgit_git_delta=>decode_deltas( CHANGING ct_objects = rt_objects ).

    " Promote temp rows to resolved object store rows (batched for performance).
    " Skip base objects (lt_base_shas): they already exist in DB with status 'R'.
    GET TIME STAMP FIELD lv_ts.
    LOOP AT rt_objects INTO ls_object.
      " Skip base objects that were merged for delta resolution only.
      IF lt_base_shas IS NOT INITIAL.
        READ TABLE lt_base_shas WITH TABLE KEY table_line = ls_object-sha1
          TRANSPORTING NO FIELDS.
        IF sy-subrc = 0. CONTINUE. ENDIF.
      ENDIF.
      CLEAR ls_row.
      ls_row-repo_key   = iv_repo_key.
      ls_row-pack_id    = iv_pack_id.
      ls_row-obj_sha1   = ls_object-sha1.
      ls_row-obj_type   = ls_object-type.
      ls_row-obj_data   = ls_object-data.
      ls_row-obj_size   = xstrlen( ls_object-data ).
      ls_row-created_at = lv_ts.
      ls_row-status     = 'R'.
      APPEND ls_row TO lt_final_rows.
    ENDLOOP.
    IF lt_final_rows IS NOT INITIAL.
      MODIFY zaog_obj_store FROM TABLE lt_final_rows.
    ENDIF.

    " Batch update pack index status to 'D' (decoded) — new objects only.
    " Base objects (lt_base_shas) have no entries in this pack's index; skip them.
    DATA lt_idx_upd TYPE zcl_abapgit_ortec_pack_index=>tty_index_entries_upd.
    DATA ls_idx_upd TYPE zcl_abapgit_ortec_pack_index=>ty_index_entries_upd.
    LOOP AT rt_objects INTO ls_object.
      IF lt_base_shas IS NOT INITIAL.
        READ TABLE lt_base_shas WITH TABLE KEY table_line = ls_object-sha1
          TRANSPORTING NO FIELDS.
        IF sy-subrc = 0. CONTINUE. ENDIF.
      ENDIF.
      CLEAR ls_idx_upd.
      ls_idx_upd-dec_status          = 'D'.
      ls_idx_upd-obj_sha1            = ls_object-sha1.
      ls_idx_upd-_control-dec_status = if_abap_behv=>mk-on.
      ls_idx_upd-_control-obj_sha1   = if_abap_behv=>mk-on.
      APPEND ls_idx_upd TO lt_idx_upd.
    ENDLOOP.
    zcl_abapgit_ortec_pack_index=>update_entries(
        iv_repo_key = iv_repo_key
        iv_pack_id  = iv_pack_id
        it_entries  = lt_idx_upd ).

    DELETE FROM zaog_obj_store
      WHERE repo_key = iv_repo_key
        AND pack_id  = iv_pack_id
        AND status   = 'P'.
    COMMIT WORK.

  ENDMETHOD.

  METHOD get_length.

    " https://github.com/git/git/blob/master/Documentation/technical/pack-format.txt
    " Variable-length size encoding: first byte = type(3 bits) + size bits(4),
    " subsequent bytes = 7 more size bits each while MSB is set.

    CONSTANTS lc_msb  TYPE x LENGTH 1 VALUE '80'.
    CONSTANTS lc_low4 TYPE x LENGTH 1 VALUE '0F'.
    CONSTANTS lc_low7 TYPE x LENGTH 1 VALUE '7F'.
    CONSTANTS lc_zero TYPE x LENGTH 1 VALUE '00'.

    DATA lv_byte     TYPE x LENGTH 1.
    DATA lv_bits     TYPE x LENGTH 1.
    DATA lv_factor   TYPE i.
    DATA lv_bits_int TYPE i.

    lv_byte = cv_data(1).
    cv_data = cv_data+1.

    lv_bits   = lv_byte BIT-AND lc_low4.
    ev_length = lv_bits.
    lv_factor = 16.

    WHILE lv_byte BIT-AND lc_msb <> lc_zero.
      IF sy-index > 1.
        lv_factor *= 128.
      ENDIF.
      IF xstrlen( cv_data ) = 0.
        EXIT.
      ENDIF.
      lv_byte = cv_data(1).
      cv_data = cv_data+1.
      lv_bits = lv_byte BIT-AND lc_low7.
      lv_bits_int = lv_bits.
      ev_length += lv_bits_int * lv_factor.
    ENDWHILE.

  ENDMETHOD.

  METHOD get_type.

    CONSTANTS lc_mask TYPE x LENGTH 1 VALUE 112.

    DATA lv_xtype TYPE x LENGTH 1.

    lv_xtype = iv_x BIT-AND lc_mask.

    CASE lv_xtype.
      WHEN 16.
        rv_type = zif_abapgit_git_definitions=>c_type-commit.
      WHEN 32.
        rv_type = zif_abapgit_git_definitions=>c_type-tree.
      WHEN 48.
        rv_type = zif_abapgit_git_definitions=>c_type-blob.
      WHEN 64.
        rv_type = zif_abapgit_git_definitions=>c_type-tag.
      WHEN 112.
        rv_type = zif_abapgit_git_definitions=>c_type-ref_d.
      WHEN OTHERS.
        zcx_abapgit_exception=>raise( |Todo, unknown git pack type| ).
    ENDCASE.

  ENDMETHOD.

  METHOD zlib_decompress.

    DATA ls_data    TYPE zcl_abapgit_zlib=>ty_decompress.
    DATA lv_adler32 TYPE zif_abapgit_git_definitions=>ty_adler32.

    ls_data = zcl_abapgit_zlib=>decompress( cv_data ).
    cv_compressed_len = ls_data-compressed_len.
    cv_decompressed = ls_data-raw.

    IF cv_compressed_len IS INITIAL.
      zcx_abapgit_exception=>raise( |Decompression failed :o/| ).
    ENDIF.

    cv_data = cv_data+cv_compressed_len.

    lv_adler32 = zcl_abapgit_hash=>adler32( cv_decompressed ).
    IF cv_data(4) <> lv_adler32.
      cv_data = cv_data+1.
    ENDIF.
    IF cv_data(4) <> lv_adler32.
      cv_data = cv_data+1.
    ENDIF.
    IF cv_data(4) <> lv_adler32.
      zcx_abapgit_exception=>raise( |Wrong Adler checksum| ).
    ENDIF.
  ENDMETHOD.

  METHOD stream_decompress.
    " Optimization #6: CL_ABAP_UNGZIP_BINARY_STREAM kernel-backed streaming inflate.
    " Uses the enhanced method DECOMPRESS_BINARY_STREAM_GIT which exposes the
    " consumed input byte count (gzip_in_off) directly from the kernel —
    " eliminating the expensive Adler32 computation + scan entirely.
    " Fixed 64 KB TYPE X output buffer prevents unbounded allocation (SYSTEM_NO_ROLL).

    DATA lv_in_len  TYPE i.
    DATA lv_input   TYPE xstring.
    " TODO: variable is assigned but never used (ABAP cleaner)
    DATA lv_buf_len TYPE i.
    DATA lo_handler TYPE REF TO lcl_ungzip_handler.
    DATA lo_stream  TYPE REF TO cl_abap_ungzip_binary_stream.
    " TODO: variable is assigned but never used (ABAP cleaner)
    DATA lv_out_buf TYPE x LENGTH 65535.

    " Bound the input slice to avoid copying the entire remaining packfile
    " into the method's internal l_xstr variable.
    lv_in_len = nmin(
                    val1 = xstrlen( iv_data )
                    val2 = iv_expected_len * 2 + 4096 ).
    IF lv_in_len < 256 AND xstrlen( iv_data ) >= 256.
      lv_in_len = 256.
    ENDIF.
    lv_input = iv_data(lv_in_len).

    " Use -1 to instruct SET_OUT_BUF to derive buffer size from the
    " declared X field length via DESCRIBE FIELD (always 65535 here).
    lv_buf_len = -1.

    lcl_ungzip_handler=>reset( ).

    lo_handler = NEW #( ).
    TRY.
        lo_stream = NEW #( output_handler = lo_handler ).

        lo_stream->set_out_buf(
          IMPORTING
            out_buf     = lv_out_buf
            out_buf_len = lv_buf_len ).

        " Enhanced method: exposes gzip_in_off as ev_compressed_len.
        " No Adler32 computation or scan needed.
        lo_stream->decompress_binary_stream_git(
          EXPORTING
            gzip_in           = lv_input
            gzip_in_len       = lv_in_len
          IMPORTING
            ev_compressed_len = ev_compressed_len ).

      CATCH cx_parameter_invalid_range
            cx_sy_buffer_overflow
            cx_sy_compression_error
            cx_parameter_invalid INTO DATA(lx_decomp).
        zcx_abapgit_exception=>raise_with_text( lx_decomp ).
    ENDTRY.

    ev_decompressed = lcl_ungzip_handler=>get_data( ).
  ENDMETHOD.
ENDCLASS.
