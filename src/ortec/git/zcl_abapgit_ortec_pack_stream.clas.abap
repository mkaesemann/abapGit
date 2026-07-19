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
      EXPORTING ev_pack_id     TYPE ty_pack_id
      RETURNING VALUE(rt_meta) TYPE ty_meta_tt
      RAISING   zcx_abapgit_ortec_git.

    "! Resolves every REF_DELTA/OFS_DELTA row left unresolved by
    "! decode_and_persist_streaming, mutating ct_meta in place (is_resolved,
    "! sha1, obj_type). Ports zcl_abapgit_ortec_delta=>resolve_all/resolve_one's
    "! proven multi-pass/chain/thin-fetch algorithm onto the metadata-only
    "! contract: instead of keeping every object's bytes resident in one big
    "! ct_objects table, a delta's raw bytes are read from zaog_obj_store
    "! (via its temp_key) only for the duration of ONE apply() call, and the
    "! resolved result is persisted + freed immediately afterwards - never
    "! more than one resolved delta's bytes held in memory at a time.
    "! <p>Pass 1: repeated ascending sweeps, resolving only bases already
    "! reconstructable purely from OTHER rows in this same ct_meta (no object-
    "! store round-trip for genuinely external bases yet), converging on any
    "! delta-onto-later-delta chain regardless of topological/SHA1/pack order,
    "! exactly like resolve_all's own phase 1.</p>
    "! <p>Pass 2: one final ascending pass allowing genuinely external/thin
    "! bases to be fetched from zaog_obj_store (via the Phase 1 LRU base
    "! cache, zcl_abapgit_ortec_base_cache, to avoid re-reading a base shared
    "! by multiple deltas) and raising on a truly missing base.</p>
    "! @raising zcx_abapgit_ortec_git |
    "! On an unresolvable base, a chain deeper than
    "! zcl_abapgit_ortec_delta=>c_max_chain_depth, an invalid OFS base_offset,
    "! or a delta stream that fails to apply. Callers must treat this as "the
    "! whole resolve failed" - ct_meta may be partially mutated and any
    "! already-resolved rows already persisted, but this call has not
    "! committed, so a caller that wants a clean rollback should issue
    "! ROLLBACK WORK on catch.
    CLASS-METHODS resolve_streaming
      IMPORTING iv_repo_key TYPE ty_repo_key
                iv_pack_id  TYPE ty_pack_id
      CHANGING  ct_meta     TYPE ty_meta_tt
      RAISING   zcx_abapgit_ortec_git.

    "! Phase 4 routing entry point: decodes and fully resolves a packfile via
    "! the streaming path, then returns the SPARSE object set that standard
    "! abapGit's pull()/walk() actually need resident in memory - the commit
    "! object(s) only (pull()'s one hard requirement:
    "! `READ TABLE it_objects WITH KEY type = commit`). Trees and blobs
    "! deliberately stay OUT of the returned table: every decoded/resolved
    "! object (commits, trees, AND blobs) is already durably persisted in
    "! zaog_obj_store under its real SHA1 by the time this returns, and H4
    "! (zcl_abapgit_ortec_walk_prep/zcl_abapgit_ortec_porcelain) already knows
    "! how to pull trees on demand and serve blobs in bounded batches straight
    "! from the store - merging them into rt_objects here would reintroduce
    "! the exact rt_objects-class memory ceiling this whole effort exists to
    "! remove. See target_design's "sparse rt_objects contract" note.
    "! @raising zcx_abapgit_ortec_git |
    "! On any decode or resolve failure - propagated as-is from
    "! decode_and_persist_streaming (which has already cleaned up its own
    "! partial 'I'-status rows and committed) or from resolve_streaming
    "! (rolled back here first, so no partially-resolved delta work from
    "! THIS call survives - any already-fully-decoded/resolved objects from
    "! an EARLIER, independent call remain, which is safe: they are real,
    "! valid, content-addressed rows). Callers implementing the DR-001
    "! fallback cascade should catch this and retry via the old
    "! zcl_abapgit_ortec_pack_dec=>decode_and_persist on the SAME pack bytes.
    CLASS-METHODS decode_streaming
      IMPORTING iv_data          TYPE xstring
                iv_repo_key      TYPE ty_repo_key
      RETURNING VALUE(rt_objects) TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_ortec_git.

  PRIVATE SECTION.
    CONSTANTS c_pack_start TYPE x LENGTH 4 VALUE '5041434B' ##NO_TEXT.
    CONSTANTS c_version    TYPE x LENGTH 4 VALUE '00000002' ##NO_TEXT.
    CONSTANTS c_zlib       TYPE x LENGTH 2 VALUE '789C' ##NO_TEXT.
    CONSTANTS c_zlib_hmm   TYPE x LENGTH 2 VALUE '7801' ##NO_TEXT.

    "! Maps an OFS_DELTA row's base_offset (an absolute byte offset into the
    "! ORIGINAL pack, computed at decode time) to the ct_meta tabix of the
    "! object that started at that offset. OFS bases are always earlier in
    "! the SAME pack byte stream (the format guarantees offsets point
    "! strictly backwards), so - unlike REF_DELTA - this map is static: built
    "! once from every row's own pack_offset, never mutated during resolve.
    TYPES: BEGIN OF ty_tabix_by_offset,
             pack_offset TYPE i,
             tabix       TYPE i,
           END OF ty_tabix_by_offset.
    TYPES ty_tabix_by_offset_tt TYPE HASHED TABLE OF ty_tabix_by_offset WITH UNIQUE KEY pack_offset.

    "! Maps a row's FINAL (real) sha1 to its ct_meta tabix, populated only for
    "! rows with is_resolved = abap_true. Unlike zcl_abapgit_ortec_delta's
    "! ct_objects (where an unresolved delta's -sha1 is OVERLOADED to hold its
    "! own declared base as a placeholder, forcing an explicit "skip self /
    "! skip other unresolved rows" workaround), ty_meta's sha1 field is BLANK
    "! for every unresolved row - so this index can never ambiguously match an
    "! unresolved sibling, and needs no such workaround.
    TYPES: BEGIN OF ty_sha_idx,
             sha1  TYPE zif_abapgit_git_definitions=>ty_sha1,
             tabix TYPE i,
           END OF ty_sha_idx.
    TYPES ty_sha_idx_tt TYPE HASHED TABLE OF ty_sha_idx WITH UNIQUE KEY sha1.

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

    "! Reads one base object's bytes, transparently using the Phase 1 LRU
    "! base cache (zcl_abapgit_ortec_base_cache) to avoid a repeat DB read
    "! when several deltas in the same resolve pass share the same base.
    CLASS-METHODS get_base_bytes
      IMPORTING iv_repo_key    TYPE ty_repo_key
                iv_sha1        TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rv_data) TYPE xstring
      RAISING   zcx_abapgit_ortec_git.

    "! Resolves exactly one ct_meta row (recursing onto its base first if the
    "! base is itself an unresolved delta - a chain). See resolve_streaming's
    "! doc for the overall two-pass strategy this is called from.
    CLASS-METHODS resolve_one_meta
      IMPORTING iv_tabix            TYPE i
                iv_depth            TYPE i
                iv_allow_thin_fetch TYPE abap_bool DEFAULT abap_true
                iv_repo_key         TYPE ty_repo_key
                iv_pack_id          TYPE ty_pack_id
      CHANGING  ct_meta             TYPE ty_meta_tt
                ct_tabix_by_offset  TYPE ty_tabix_by_offset_tt
                ct_sha_idx          TYPE ty_sha_idx_tt
      RAISING   zcx_abapgit_ortec_git.
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

  METHOD get_base_bytes.
    DATA ls_object TYPE zif_abapgit_definitions=>ty_object.

    rv_data = zcl_abapgit_ortec_base_cache=>get_instance( )->get( iv_sha1 ).
    IF rv_data IS NOT INITIAL.
      RETURN.
    ENDIF.

    TRY.
        ls_object = zcl_abapgit_ortec_obj_store=>get_object(
          iv_repo_key = iv_repo_key
          iv_sha1     = iv_sha1 ).
      CATCH zcx_abapgit_ortec_git.
        zcx_abapgit_ortec_git=>raise( |Delta base not found, { iv_sha1 }| ).
    ENDTRY.

    rv_data = ls_object-data.
    zcl_abapgit_ortec_base_cache=>get_instance( )->put(
      iv_sha1 = iv_sha1
      iv_data = rv_data ).
  ENDMETHOD.

  METHOD resolve_one_meta.
    DATA ls_delta_obj  TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_base_obj   TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_base_tabix TYPE i.
    DATA lv_base_type  TYPE zif_abapgit_git_definitions=>ty_type.
    DATA lv_base_data  TYPE xstring.
    DATA lv_external   TYPE abap_bool.
    DATA lv_result     TYPE xstring.
    DATA lv_final_sha1 TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA ls_off        TYPE ty_tabix_by_offset.
    DATA ls_sha        TYPE ty_sha_idx.
    DATA lx_apply      TYPE REF TO zcx_abapgit_exception.
    DATA lx_missing    TYPE REF TO zcx_abapgit_ortec_git.
    DATA lv_base_sha_diag         TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_base_pack_offset_diag TYPE i.

    FIELD-SYMBOLS <ls_row>  TYPE ty_meta.
    FIELD-SYMBOLS <ls_base> TYPE ty_meta.

    READ TABLE ct_meta ASSIGNING <ls_row> INDEX iv_tabix.
    IF sy-subrc <> 0.
      zcx_abapgit_ortec_git=>raise( |Delta resolve: internal index { iv_tabix } out of range| ).
    ENDIF.

    IF <ls_row>-is_resolved = abap_true.
      RETURN. " Already resolved (memoized base case, mirrors resolve_one).
    ENDIF.

    IF iv_depth > zcl_abapgit_ortec_delta=>c_max_chain_depth.
      zcx_abapgit_ortec_git=>raise(
        |Delta chain exceeds maximum depth { zcl_abapgit_ortec_delta=>c_max_chain_depth } (possible cycle)| ).
    ENDIF.

    CLEAR lv_external.
    IF <ls_row>-delta_base IS NOT INITIAL.
      " REF_DELTA: base identified by declared content SHA1. Only rows already
      " resolved THIS pass are indexed in ct_sha_idx (see its type doc) - a
      " miss here genuinely means "not resolved yet", never a false match.
      READ TABLE ct_sha_idx INTO ls_sha WITH TABLE KEY sha1 = <ls_row>-delta_base.
      IF sy-subrc = 0.
        lv_base_tabix = ls_sha-tabix.
      ELSE.
        IF iv_allow_thin_fetch = abap_false.
          RETURN. " Not yet resolvable in-pack; resolve_streaming's next sweep retries.
        ENDIF.
        " Genuinely external base (from a prior pack/pull) - or truly missing.
        TRY.
            ls_base_obj = zcl_abapgit_ortec_obj_store=>get_object(
              iv_repo_key = iv_repo_key
              iv_sha1     = <ls_row>-delta_base ).
          CATCH zcx_abapgit_ortec_git.
            zcx_abapgit_ortec_git=>raise( |Delta base not found, { <ls_row>-delta_base }| ).
        ENDTRY.
        lv_base_type = ls_base_obj-type.
        lv_base_data = get_base_bytes( iv_repo_key = iv_repo_key iv_sha1 = <ls_row>-delta_base ).
        lv_external  = abap_true.
        lv_base_sha_diag         = <ls_row>-delta_base.
        lv_base_pack_offset_diag = -1. " external - no in-pack offset
      ENDIF.
    ELSE.
      " OFS_DELTA: base is always earlier in the SAME pack (format guarantee),
      " so it must already be a row in ct_meta - locate it via its byte offset.
      IF <ls_row>-base_offset < 0.
        zcx_abapgit_ortec_git=>raise(
          |OFS delta: base offset { <ls_row>-base_offset } is negative (obj_index | &&
          |{ <ls_row>-obj_index }, pack_offset { <ls_row>-pack_offset })| ).
      ENDIF.
      READ TABLE ct_tabix_by_offset INTO ls_off WITH TABLE KEY pack_offset = <ls_row>-base_offset.
      IF sy-subrc <> 0.
        zcx_abapgit_ortec_git=>raise(
          |OFS delta: no object at base offset { <ls_row>-base_offset } (obj_index | &&
          |{ <ls_row>-obj_index }, pack_offset { <ls_row>-pack_offset })| ).
      ENDIF.
      lv_base_tabix = ls_off-tabix.
    ENDIF.

    IF lv_external = abap_false.
      READ TABLE ct_meta ASSIGNING <ls_base> INDEX lv_base_tabix.
      IF sy-subrc <> 0.
        zcx_abapgit_ortec_git=>raise( |Delta resolve: internal index { lv_base_tabix } out of range| ).
      ENDIF.

      IF <ls_base>-is_resolved = abap_false.
        resolve_one_meta(
          EXPORTING
            iv_tabix            = lv_base_tabix
            iv_depth            = iv_depth + 1
            iv_allow_thin_fetch = iv_allow_thin_fetch
            iv_repo_key         = iv_repo_key
            iv_pack_id          = iv_pack_id
          CHANGING
            ct_meta            = ct_meta
            ct_tabix_by_offset = ct_tabix_by_offset
            ct_sha_idx         = ct_sha_idx ).
        READ TABLE ct_meta ASSIGNING <ls_base> INDEX lv_base_tabix.
        IF sy-subrc <> 0.
          zcx_abapgit_ortec_git=>raise( |Delta resolve: internal index { lv_base_tabix } out of range| ).
        ENDIF.
        IF <ls_base>-is_resolved = abap_false.
          IF iv_allow_thin_fetch = abap_false.
            RETURN. " Base itself not yet resolvable this sweep; retry next sweep.
          ENDIF.
          zcx_abapgit_ortec_git=>raise( |Delta, base still unresolved| ).
        ENDIF.
      ENDIF.

      " Defensive sanity check, mirroring the proven pattern in
      " zcl_abapgit_ortec_delta=>resolve_one: catch a wrong-base match
      " immediately and unambiguously here rather than as a generic apply()
      " failure later.
      IF <ls_row>-delta_base IS NOT INITIAL AND <ls_base>-sha1 <> <ls_row>-delta_base.
        zcx_abapgit_ortec_git=>raise(
          |Delta base identity mismatch: declared { <ls_row>-delta_base }, | &&
          |resolved { <ls_base>-sha1 }| ).
      ENDIF.

      lv_base_type = <ls_base>-obj_type.
      lv_base_data = get_base_bytes( iv_repo_key = iv_repo_key iv_sha1 = <ls_base>-sha1 ).
      lv_base_sha_diag         = <ls_base>-sha1.
      lv_base_pack_offset_diag = <ls_base>-pack_offset.
    ENDIF.

    " Read the delta's own raw (pre-application) bytes, persisted under its
    " temp key by decode_and_persist_streaming.
    TRY.
        ls_delta_obj = zcl_abapgit_ortec_obj_store=>get_object(
          iv_repo_key = iv_repo_key
          iv_sha1     = <ls_row>-temp_key ).
      CATCH zcx_abapgit_ortec_git INTO lx_missing.
        zcx_abapgit_ortec_git=>raise( |Delta temp data missing: { lx_missing->get_text( ) }| ).
    ENDTRY.

    TRY.
        lv_result = zcl_abapgit_ortec_delta=>apply(
          iv_base  = lv_base_data
          iv_delta = ls_delta_obj-data ).
        lv_final_sha1 = zcl_abapgit_hash=>sha1( iv_type = lv_base_type iv_data = lv_result ).
      CATCH zcx_abapgit_exception INTO lx_apply.
        " Diagnostic detail deliberately includes BOTH sides' identifying
        " info (not just byte counts) - obj_index/pack_offset pin down
        " EXACTLY which delta and which resolved base were involved, so a
        " wrong-base-picked-for-this-delta bug (as opposed to a genuinely
        " corrupt/unexpected delta stream) can be told apart on sight instead
        " of requiring another live round-trip to re-diagnose.
        zcx_abapgit_ortec_git=>raise(
          |{ lx_apply->get_text( ) } - delta obj_index { <ls_row>-obj_index } | &&
          |pack_offset { <ls_row>-pack_offset } declared_base_sha1 '{ <ls_row>-delta_base }' | &&
          |declared_base_offset { <ls_row>-base_offset }  resolved base obj_type | &&
          |{ lv_base_type } sha1 '{ lv_base_sha_diag }' pack_offset { lv_base_pack_offset_diag } | &&
          |{ xstrlen( lv_base_data ) } bytes  delta { xstrlen( ls_delta_obj-data ) } bytes, | &&
          |depth { iv_depth }| ).
    ENDTRY.

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = iv_repo_key
      iv_sha1     = lv_final_sha1
      iv_type     = lv_base_type
      iv_data     = lv_result
      iv_pack_id  = iv_pack_id
      iv_status   = 'R' ).
    CLEAR lv_result.

    " The temp-keyed raw-delta row is now superseded by the real, resolved
    " object above - remove it (uncommitted; resolve_streaming issues one
    " COMMIT WORK at the very end, so this rolls back together with
    " everything else if a LATER object in the same pass fails).
    DELETE FROM zaog_obj_store
      WHERE repo_key = iv_repo_key
        AND obj_sha1 = <ls_row>-temp_key.

    <ls_row>-sha1        = lv_final_sha1.
    <ls_row>-obj_type    = lv_base_type.
    <ls_row>-is_resolved = abap_true.

    ls_sha-sha1  = lv_final_sha1.
    ls_sha-tabix = iv_tabix.
    INSERT ls_sha INTO TABLE ct_sha_idx.
  ENDMETHOD.

  METHOD resolve_streaming.
    DATA lv_tabix    TYPE i.
    DATA lv_progress TYPE abap_bool.
    DATA ls_sha      TYPE ty_sha_idx.
    DATA ls_off      TYPE ty_tabix_by_offset.
    DATA lt_tabix_by_offset TYPE ty_tabix_by_offset_tt.
    DATA lt_sha_idx         TYPE ty_sha_idx_tt.

    FIELD-SYMBOLS <ls_row> TYPE ty_meta.

    LOOP AT ct_meta ASSIGNING <ls_row>.
      lv_tabix = sy-tabix.

      ls_off-pack_offset = <ls_row>-pack_offset.
      ls_off-tabix       = lv_tabix.
      INSERT ls_off INTO TABLE lt_tabix_by_offset.

      IF <ls_row>-is_resolved = abap_true.
        ls_sha-sha1  = <ls_row>-sha1.
        ls_sha-tabix = lv_tabix.
        INSERT ls_sha INTO TABLE lt_sha_idx.
      ENDIF.
    ENDLOOP.

    " Pass 1: repeated ascending sweeps, in-pack only (no object-store round-
    " trip, no raise on "not found yet") - converges on any delta-onto-later-
    " delta chain regardless of topological/SHA1/pack order. Bounded by the
    " pack's actual maximum delta-chain depth, not by object count.
    DO.
      lv_progress = abap_false.
      LOOP AT ct_meta ASSIGNING <ls_row>.
        lv_tabix = sy-tabix.
        IF <ls_row>-is_resolved = abap_true.
          CONTINUE.
        ENDIF.
        resolve_one_meta(
          EXPORTING
            iv_tabix            = lv_tabix
            iv_depth            = 1
            iv_allow_thin_fetch = abap_false
            iv_repo_key         = iv_repo_key
            iv_pack_id          = iv_pack_id
          CHANGING
            ct_meta            = ct_meta
            ct_tabix_by_offset = lt_tabix_by_offset
            ct_sha_idx         = lt_sha_idx ).
        READ TABLE ct_meta ASSIGNING <ls_row> INDEX lv_tabix.
        IF sy-subrc = 0 AND <ls_row>-is_resolved = abap_true.
          lv_progress = abap_true.
        ENDIF.
      ENDLOOP.
      IF lv_progress = abap_false.
        EXIT.
      ENDIF.
    ENDDO.

    " Pass 2: one final ascending pass, now allowing the object-store fetch
    " and the precise "Delta base not found" raise.
    LOOP AT ct_meta ASSIGNING <ls_row>.
      lv_tabix = sy-tabix.
      IF <ls_row>-is_resolved = abap_true.
        CONTINUE.
      ENDIF.
      resolve_one_meta(
        EXPORTING
          iv_tabix    = lv_tabix
          iv_depth    = 1
          iv_repo_key = iv_repo_key
          iv_pack_id  = iv_pack_id
        CHANGING
          ct_meta            = ct_meta
          ct_tabix_by_offset = lt_tabix_by_offset
          ct_sha_idx         = lt_sha_idx ).
    ENDLOOP.

    COMMIT WORK.
  ENDMETHOD.

  METHOD decode_streaming.
    DATA lt_meta       TYPE ty_meta_tt.
    DATA lv_pack_id    TYPE ty_pack_id.
    DATA ls_meta       LIKE LINE OF lt_meta.
    DATA ls_commit_obj TYPE zif_abapgit_definitions=>ty_object.
    DATA lx_resolve    TYPE REF TO zcx_abapgit_ortec_git.
    DATA lx_missing    TYPE REF TO zcx_abapgit_ortec_git.

    lt_meta = decode_and_persist_streaming(
      EXPORTING iv_data     = iv_data
                iv_repo_key = iv_repo_key
      IMPORTING ev_pack_id  = lv_pack_id ).

    TRY.
        resolve_streaming(
          EXPORTING iv_repo_key = iv_repo_key
                    iv_pack_id  = lv_pack_id
          CHANGING  ct_meta     = lt_meta ).
      CATCH zcx_abapgit_ortec_git INTO lx_resolve.
        " resolve_streaming does not commit on failure (see its own doc) -
        " roll back any of THIS call's own uncommitted resolve work so a
        " retry (or the DR-001 fallback to the old decoder) starts clean.
        " Phase 2's own objects (already committed as status 'R' before
        " resolve_streaming ever ran) are unaffected either way - they are
        " real, valid, content-addressed rows regardless of this outcome.
        ROLLBACK WORK.
        RAISE EXCEPTION lx_resolve.
    ENDTRY.

    LOOP AT lt_meta INTO ls_meta WHERE obj_type = zif_abapgit_git_definitions=>c_type-commit.
      TRY.
          ls_commit_obj = zcl_abapgit_ortec_obj_store=>get_object(
            iv_repo_key = iv_repo_key
            iv_sha1     = ls_meta-sha1 ).
        CATCH zcx_abapgit_ortec_git INTO lx_missing.
          zcx_abapgit_ortec_git=>raise(
            |Streaming decode: commit object missing after resolve: { lx_missing->get_text( ) }| ).
      ENDTRY.
      APPEND ls_commit_obj TO rt_objects.
    ENDLOOP.
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
    ev_pack_id = lv_pack_id.

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
