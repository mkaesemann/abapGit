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
                iv_url      TYPE string OPTIONAL
      CHANGING  ct_meta     TYPE ty_meta_tt
      RAISING   zcx_abapgit_ortec_git.

    "! Resets the thin-pack completion budget (see complete_missing_base)
    "! to 0. Callers that own a TRUE top-level fetch attempt - currently
    "! zcl_abapgit_ortec_fastpath=>upload_pack_by_branch/upload_pack_by_commit
    "! only - must call this ONCE before their own retry cascade begins, so
    "! the budget is shared across every tier/progressive step of that ONE
    "! user-initiated fetch. decode_streaming deliberately does NOT reset
    "! this itself, since it is also called re-entrantly for a completion
    "! fetch's own (possibly further-incomplete) small pack.
    CLASS-METHODS reset_completion_budget.

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
                iv_url           TYPE string OPTIONAL
      RETURNING VALUE(rt_objects) TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_ortec_git.

  PRIVATE SECTION.
    CONSTANTS c_pack_start TYPE x LENGTH 4 VALUE '5041434B' ##NO_TEXT.
    CONSTANTS c_version    TYPE x LENGTH 4 VALUE '00000002' ##NO_TEXT.
    CONSTANTS c_zlib       TYPE x LENGTH 2 VALUE '789C' ##NO_TEXT.
    CONSTANTS c_zlib_hmm   TYPE x LENGTH 2 VALUE '7801' ##NO_TEXT.
    "! Objects (decode) or resolved deltas (resolve) are batched in memory up
    "! to this many entries before a single bulk DB write - a live SAT trace
    "! on a 100k+ object pack showed 82% of total runtime was pure DB
    "! connection open/close overhead from one MODIFY per object; batching
    "! trades a small, bounded amount of extra memory (a few hundred objects'
    "! worth at a time, never the whole pack) for orders-of-magnitude fewer
    "! round trips, while keeping the core streaming memory-bounding property
    "! intact (never the full object graph resident at once).
    CONSTANTS c_batch_size TYPE i VALUE 500.

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

    "! Caps the total number of thin-pack completion fetches (see
    "! complete_missing_base) attempted per decode_streaming( ) call - reset
    "! to 0 at the start of every decode_streaming( ) call. Bounds worst-case
    "! extra network/DB cost for a pathological pack (many distinct missing
    "! bases, or a flaky network causing repeated failed attempts) instead of
    "! allowing unbounded nested completion fetches.
    CONSTANTS c_max_completion_attempts TYPE i VALUE 20.
    CLASS-DATA gv_completion_attempts TYPE i.

    "! Approved bounded observability counter (Package D1 diff-audit
    "! disposition, same as zcl_abapgit_ortec_delta=>gv_thin_fetch_calls -
    "! see that class's doc for the full lifecycle/reset/concurrency/
    "! production-semantics justification and why a pure counter-free test
    "! seam was evaluated and rejected as impractical here). Counts calls
    "! to resolve_one_meta's external-base on-demand fallback branch, which
    "! resolve_streaming's own phase 1.5 bulk merge is designed to make
    "! unreachable in the normal path. Private CLASS-DATA, incremented at
    "! exactly one call site, reset only by the LOCAL FRIEND test class's
    "! setup, never read or branched on by productive code - purely a
    "! call-shape verification seam for D1's no_thin_fetch_for_ext_base
    "! test (Package D design §16).
    CLASS-DATA gv_thin_fetch_calls TYPE i.

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
    "! When iv_url is supplied and the base is not found locally, attempts
    "! thin-pack completion (see complete_missing_base) before giving up.
    CLASS-METHODS get_base_bytes
      IMPORTING iv_repo_key    TYPE ty_repo_key
                iv_sha1        TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_url         TYPE string OPTIONAL
      RETURNING VALUE(rv_data) TYPE xstring
      RAISING   zcx_abapgit_ortec_git.

    "! Thin-pack completion: DISABLED as of Slice 2C's correctness
    "! correction (finding F-2C-001, .memory/logs/variant_b_slice2c_migration_map.md).
    "! One MATERIALIZE_BLOBS request per missing delta base is a genuine
    "! one-request-per-object repair - exactly the pattern the Variant B
    "! architecture forbids (no one-request-per-object repair; external
    "! bases must be collected, deduplicated, and bulk-loaded). This method
    "! is now a permanent no-op (always returns abap_false, never calls
    "! zcl_abapgit_ortec_fastpath=>complete_missing_object) until Slice 7
    "! implements real collect/deduplicate/bulk external-base resolution.
    "! Callers (get_base_bytes, resolve_one_meta) already treat
    "! rv_attempted = abap_false as "completion not attempted" and correctly
    "! raise with iv_retry_without_haves = abap_true, so a genuinely missing
    "! external base now always escalates to the caller's existing bounded
    "! RECOVERY_BRANCH_FULL/self-contained retry instead of a targeted
    "! per-object fetch. c_max_completion_attempts/gv_completion_attempts/
    "! reset_completion_budget are left physically present (unused while
    "! disabled) for Slice 7 to reactivate against a bulk-resolution design.
    "! Historical doc (kept for Slice 7 context, NOT current behavior): this
    "! used to fetch exactly one missing object by SHA1 via a targeted,
    "! minimal "want <sha1>" request - see .memory/state.md's "Architecture
    "! hardening plan" / Phase 1 incident log (2026-07-20) for why a plain
    "! deepen widening is not a reliable substitute for fetching the exact
    "! missing piece; that need still exists, but must be met in bulk, not
    "! one HTTP call per object.
    "! @parameter rv_attempted |
    "! Always abap_false while disabled.
    CLASS-METHODS complete_missing_base
      IMPORTING iv_repo_key       TYPE ty_repo_key
                iv_sha1           TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_url            TYPE string
      RETURNING VALUE(rv_attempted) TYPE abap_bool.

    "! Bulk-persists ct_batch (status = c_status_incomplete) in ONE DB call
    "! and clears it - the decode-side counterpart of resolve_one_meta's
    "! ct_write_batch/ct_delete_batch flushing. A no-op if ct_batch is empty.
    CLASS-METHODS flush_batch
      IMPORTING iv_repo_key TYPE ty_repo_key
                iv_pack_id  TYPE ty_pack_id
      CHANGING  ct_batch    TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_ortec_git.

    "! Bulk-persists every resolved object in ct_write_batch (status 'R') and
    "! bulk-deletes every superseded temp key in ct_delete_batch, in ONE DB
    "! call each, then clears both. A no-op if both are empty.
    CLASS-METHODS flush_resolve_batch
      IMPORTING iv_repo_key      TYPE ty_repo_key
                iv_pack_id       TYPE ty_pack_id
      CHANGING  ct_write_batch   TYPE zif_abapgit_definitions=>ty_objects_tt
                ct_delete_batch  TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RAISING   zcx_abapgit_ortec_git.

    "! Resolves exactly one ct_meta row (recursing onto its base first if the
    "! base is itself an unresolved delta - a chain). See resolve_streaming's
    "! doc for the overall two-pass strategy this is called from. Resolved
    "! objects/superseded temp keys are appended to ct_write_batch/
    "! ct_delete_batch rather than written individually - resolve_streaming
    "! flushes these in bulk periodically and once more at the end.
    "! Warm current-pack delta temp rows in bounded SQL packages before
    "! resolution, avoiding one database round-trip per delta row.
    CLASS-METHODS preload_delta_rows
      IMPORTING iv_repo_key TYPE ty_repo_key
                it_meta     TYPE ty_meta_tt
      RAISING   zcx_abapgit_ortec_git.

    "! Collect and bulk-read locally available external REF_DELTA bases.
    "! Missing candidates remain the final resolver pass's responsibility.
    CLASS-METHODS preload_external_bases
      IMPORTING iv_repo_key TYPE ty_repo_key
                it_meta     TYPE ty_meta_tt
                it_sha_idx  TYPE ty_sha_idx_tt
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS resolve_one_meta
      IMPORTING iv_tabix            TYPE i
                iv_depth            TYPE i
                iv_allow_thin_fetch TYPE abap_bool DEFAULT abap_true
                iv_repo_key         TYPE ty_repo_key
                iv_pack_id          TYPE ty_pack_id
                iv_url              TYPE string OPTIONAL
      CHANGING  ct_meta             TYPE ty_meta_tt
                ct_tabix_by_offset  TYPE ty_tabix_by_offset_tt
                ct_sha_idx          TYPE ty_sha_idx_tt
                ct_write_batch      TYPE zif_abapgit_definitions=>ty_objects_tt
                ct_delete_batch     TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RAISING   zcx_abapgit_ortec_git.

    "! Diagnostic-only helper (not on any hot path - only called right before
    "! raising a "Delta base not found" error): counts how many rows in
    "! ct_meta are still unresolved at that moment, to help tell "a genuinely
    "! external/missing base" apart from "a broader in-pack resolution bug
    "! that left many other rows stuck too".
    CLASS-METHODS count_unresolved
      IMPORTING it_meta         TYPE ty_meta_tt
      RETURNING VALUE(rv_count) TYPE i.
ENDCLASS.


CLASS zcl_abapgit_ortec_pack_stream IMPLEMENTATION.

  METHOD build_pack_id.
    DATA lv_ts TYPE timestampl.
    GET TIME STAMP FIELD lv_ts.
    rv_pack_id = |{ iv_repo_key }{ lv_ts }|.
    rv_pack_id = rv_pack_id(32).
  ENDMETHOD.

  METHOD reset_completion_budget.
    gv_completion_attempts = 0.
  ENDMETHOD.

  METHOD cleanup_incomplete.
    DELETE FROM zaog_obj_store
      WHERE repo_key = iv_repo_key
        AND pack_id  = iv_pack_id
        AND status   = zcl_abapgit_ortec_pack_stream=>c_status_incomplete.
  ENDMETHOD.

  METHOD get_base_bytes.
    DATA ls_object TYPE zif_abapgit_definitions=>ty_object.
    DATA lo_cache  TYPE REF TO zcl_abapgit_ortec_base_cache.

    lo_cache = zcl_abapgit_ortec_base_cache=>get_instance( ).

    " has( ) - not "get( ) IS NOT INITIAL" - a genuinely cached 0-byte object
    " (a valid Git object, e.g. an empty blob) also returns an initial
    " xstring from get( ). Checking IS NOT INITIAL would treat that as a
    " permanent cache miss, causing a redundant DB fetch AND a redundant
    " put( ) on every single call for that sha1.
    IF lo_cache->has( iv_sha1 ) = abap_true.
      rv_data = lo_cache->get( iv_sha1 ).
      RETURN.
    ENDIF.

    TRY.
        ls_object = zcl_abapgit_ortec_obj_store=>get_object(
          iv_repo_key = iv_repo_key
          iv_sha1     = iv_sha1 ).
      CATCH zcx_abapgit_ortec_git.
        " Thin-pack completion: try fetching exactly this missing object
        " before giving up - see complete_missing_base's doc for why this
        " is often the correct recovery, not stale haves.
        IF complete_missing_base( iv_repo_key = iv_repo_key iv_sha1 = iv_sha1 iv_url = iv_url ) = abap_true.
          TRY.
              ls_object = zcl_abapgit_ortec_obj_store=>get_object(
                iv_repo_key = iv_repo_key
                iv_sha1     = iv_sha1 ).
            CATCH zcx_abapgit_ortec_git.
              " Completion was attempted but still didn't supply the object -
              " fall through to the same raise as if completion had never
              " been attempted at all.
              zcx_abapgit_ortec_git=>raise(
                iv_text                = |Delta base not found, { iv_sha1 }|
                iv_retry_without_haves = abap_true ).
          ENDTRY.
        ELSE.
          " See the matching comment in resolve_one_meta's REF_DELTA branch -
          " same "our verified haves lied" recovery signal.
          zcx_abapgit_ortec_git=>raise(
            iv_text                = |Delta base not found, { iv_sha1 }|
            iv_retry_without_haves = abap_true ).
        ENDIF.
    ENDTRY.

    rv_data = ls_object-data.
    lo_cache->put(
      iv_sha1 = iv_sha1
      iv_data = rv_data ).
  ENDMETHOD.

  METHOD complete_missing_base.
    " F-2C-001: permanently disabled - see this method's doc comment. Never
    " calls zcl_abapgit_ortec_fastpath=>complete_missing_object, never
    " issues an HTTP request, never increments gv_completion_attempts.
    " Callers already handle rv_attempted = abap_false correctly.
    RETURN.
  ENDMETHOD.

  METHOD count_unresolved.
    rv_count = REDUCE i( INIT n = 0
                         FOR ls_row IN it_meta
                         NEXT n = n + COND i( WHEN ls_row-is_resolved = abap_false THEN 1 ELSE 0 ) ).
  ENDMETHOD.

  METHOD flush_batch.
    IF ct_batch IS INITIAL.
      RETURN.
    ENDIF.
    zcl_abapgit_ortec_obj_store=>store_objects(
      iv_repo_key = iv_repo_key
      it_objects  = ct_batch
      iv_pack_id  = iv_pack_id
      iv_status   = c_status_incomplete ).
    CLEAR ct_batch.
  ENDMETHOD.

  METHOD flush_resolve_batch.
    DATA lt_obj_store TYPE SORTED TABLE OF zaog_obj_store
           WITH UNIQUE KEY repo_key obj_sha1.

    IF ct_write_batch IS NOT INITIAL.
      zcl_abapgit_ortec_obj_store=>store_objects(
          iv_repo_key = iv_repo_key
          it_objects  = ct_write_batch
          iv_pack_id  = iv_pack_id
          iv_status   = 'R' ).
      CLEAR ct_write_batch.
    ENDIF.

    IF ct_delete_batch IS NOT INITIAL.
      lt_obj_store = VALUE #( FOR lv_sha1 IN ct_delete_batch
                              ( repo_key = iv_repo_key
                                obj_sha1 = lv_sha1 ) ).
      DELETE zaog_obj_store FROM TABLE @lt_obj_store.
      CLEAR ct_delete_batch.
    ENDIF.
  ENDMETHOD.

  METHOD preload_delta_rows.
    DATA lt_temp_keys TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_unique TYPE HASHED TABLE OF zif_abapgit_git_definitions=>ty_sha1
      WITH UNIQUE KEY table_line.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.

    FIELD-SYMBOLS <ls_meta> TYPE ty_meta.

    LOOP AT it_meta ASSIGNING <ls_meta>
         WHERE is_resolved = abap_false
           AND temp_key IS NOT INITIAL.
      INSERT <ls_meta>-temp_key INTO TABLE lt_unique.
      IF sy-subrc = 0.
        APPEND <ls_meta>-temp_key TO lt_temp_keys.
      ENDIF.
    ENDLOOP.

    IF lt_temp_keys IS INITIAL.
      RETURN.
    ENDIF.

    " Strict: current-pack temp rows must all exist. GET_OBJECTS reads them
    " in bounded packages and warms the normal object-store session cache.
    lt_objects = zcl_abapgit_ortec_obj_store=>get_objects(
      iv_repo_key   = iv_repo_key
      it_sha1s      = lt_temp_keys
      iv_bulk_fetch = abap_false ).
  ENDMETHOD.


  METHOD preload_external_bases.
    DATA lt_candidates TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_unique TYPE HASHED TABLE OF zif_abapgit_git_definitions=>ty_sha1
      WITH UNIQUE KEY table_line.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lo_cache TYPE REF TO zcl_abapgit_ortec_base_cache.

    FIELD-SYMBOLS <ls_meta> TYPE ty_meta.
    FIELD-SYMBOLS <ls_object> LIKE LINE OF lt_objects.

    lo_cache = zcl_abapgit_ortec_base_cache=>get_instance( ).

    LOOP AT it_meta ASSIGNING <ls_meta>
         WHERE is_resolved = abap_false
           AND delta_base IS NOT INITIAL.
      READ TABLE it_sha_idx
        WITH TABLE KEY sha1 = <ls_meta>-delta_base
        TRANSPORTING NO FIELDS.
      IF sy-subrc = 0.
        CONTINUE.
      ENDIF.

      IF lo_cache->has( <ls_meta>-delta_base ) = abap_true.
        CONTINUE.
      ENDIF.

      INSERT <ls_meta>-delta_base INTO TABLE lt_unique.
      IF sy-subrc = 0.
        APPEND <ls_meta>-delta_base TO lt_candidates.
      ENDIF.
    ENDLOOP.

    IF lt_candidates IS INITIAL.
      RETURN.
    ENDIF.

    lt_objects = zcl_abapgit_ortec_obj_store=>get_available_objects(
      iv_repo_key = iv_repo_key
      it_sha1s    = lt_candidates ).

    LOOP AT lt_objects ASSIGNING <ls_object>.
      lo_cache->put(
        iv_sha1 = <ls_object>-sha1
        iv_data = <ls_object>-data ).
    ENDLOOP.
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
        " resolve_streaming's phase 1.5 already bulk-loads every genuinely
        " external base BEFORE this final pass ever calls resolve_one_meta
        " with iv_allow_thin_fetch = abap_true, merging it straight into
        " ct_sha_idx so the READ TABLE above finds it directly - this branch
        " is dead in the normal path. gv_thin_fetch_calls exists purely so
        " D1's tests can assert it stays at zero (see this class's
        " testclasses include). PRELOAD_EXTERNAL_BASES also still runs as a
        " defense-in-depth cache warmer, so GET_BASE_BYTES remains
        " cache-only in the rare case this branch is still reached, and
        " preserves retry-without-haves for a true miss.
        gv_thin_fetch_calls = gv_thin_fetch_calls + 1.
        TRY.
            lv_base_data = get_base_bytes(
              iv_repo_key = iv_repo_key
              iv_sha1     = <ls_row>-delta_base
              iv_url      = iv_url ).

            ls_base_obj = zcl_abapgit_ortec_obj_store=>get_object(
              iv_repo_key = iv_repo_key
              iv_sha1     = <ls_row>-delta_base ).
          CATCH zcx_abapgit_ortec_git.
            zcx_abapgit_ortec_git=>raise(
              iv_text                = |Delta base not found, { <ls_row>-delta_base } - | &&
                |declaring obj_index { <ls_row>-obj_index } pack_offset { <ls_row>-pack_offset }, | &&
                |pack has { lines( ct_meta ) } objects, { count_unresolved( ct_meta ) } still unresolved|
              iv_retry_without_haves = abap_true ).
        ENDTRY.
        lv_base_type = ls_base_obj-type.
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
            iv_url              = iv_url
          CHANGING
            ct_meta            = ct_meta
            ct_tabix_by_offset = ct_tabix_by_offset
            ct_sha_idx         = ct_sha_idx
            ct_write_batch     = ct_write_batch
            ct_delete_batch    = ct_delete_batch ).
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

      " The base may have been resolved just now (this pass, this call
      " stack) and is therefore correct/authoritative in ct_write_batch
      " even though it is not yet persisted to zaog_obj_store (batched
      " flushing - see flush_resolve_batch). get_base_bytes's own
      " cache-then-object-store lookup is NOT a substitute for this: the
      " Phase 1 LRU base cache is a byte-budgeted PERFORMANCE optimization,
      " not a correctness guarantee - relying on it alone to serve an
      " unflushed base risks a false "Delta base not found" whenever the
      " cache does not (or, under budget pressure in a large real pack,
      " cannot) still hold that exact entry. Checking ct_write_batch first
      " is the smallest, authoritative fix: it is the same in-memory table
      " this row's bytes were appended to at the moment it was resolved,
      " bounded by c_batch_size, so this lookup is O(batch_size) worst case,
      " not O(repo size).
      READ TABLE ct_write_batch WITH KEY sha1 = <ls_base>-sha1 INTO DATA(ls_base_batch).
      IF sy-subrc = 0.
        lv_base_data = ls_base_batch-data.
      ELSE.
        lv_base_data = get_base_bytes( iv_repo_key = iv_repo_key iv_sha1 = <ls_base>-sha1 iv_url = iv_url ).
      ENDIF.
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

    zcl_abapgit_ortec_base_cache=>get_instance( )->put(
      iv_sha1 = lv_final_sha1
      iv_data = lv_result ).

    " Batched, not written individually - resolve_streaming flushes
    " ct_write_batch/ct_delete_batch in bulk periodically and once more at
    " the end (see flush_resolve_batch). The LRU put() just above is what
    " makes a just-resolved object immediately available as another delta's
    " base WITHIN the same pass, even before this batch is actually flushed
    " to the DB - get_base_bytes always checks the cache first.
    APPEND VALUE #( sha1 = lv_final_sha1 type = lv_base_type data = lv_result )
      TO ct_write_batch.
    APPEND <ls_row>-temp_key TO ct_delete_batch.
    CLEAR lv_result.

    IF lines( ct_write_batch ) >= c_batch_size.
      flush_resolve_batch(
        EXPORTING iv_repo_key = iv_repo_key
                  iv_pack_id  = iv_pack_id
        CHANGING  ct_write_batch  = ct_write_batch
                  ct_delete_batch = ct_delete_batch ).
    ENDIF.

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
    DATA lt_write_batch     TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_delete_batch    TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_external_bases  TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_loaded_bases    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_new_tabix       TYPE i.
    DATA lx_bulk_missing    TYPE REF TO zcx_abapgit_exception.

    FIELD-SYMBOLS <ls_row>       TYPE ty_meta.
    "! Dedicated field symbol for the post-call progress check below - never
    "! reused to reassign the active LOOP's own <ls_row> iterator (that
    "! aliasing pattern is an ATC finding: reassigning a field symbol that
    "! is currently driving a LOOP ... ASSIGNING is unsafe/confusing even
    "! when provably harmless in one specific case, since resolve_one_meta
    "! already mutates ct_meta in place at the same tabix via pass-by-
    "! reference).
    FIELD-SYMBOLS <ls_row_after> TYPE ty_meta.
    FIELD-SYMBOLS <ls_pending>   TYPE ty_meta.
    FIELD-SYMBOLS <ls_loaded>    TYPE zif_abapgit_definitions=>ty_object.

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

    " Warm every unresolved current-pack delta temp row before entering the
    " fixpoint loop. Subsequent GET_OBJECT(temp_key) calls are cache hits.
    preload_delta_rows(
      iv_repo_key = iv_repo_key
      it_meta     = ct_meta ).

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
            ct_sha_idx         = lt_sha_idx
            ct_write_batch     = lt_write_batch
            ct_delete_batch    = lt_delete_batch ).
        READ TABLE ct_meta ASSIGNING <ls_row_after> INDEX lv_tabix.
        IF sy-subrc = 0 AND <ls_row_after>-is_resolved = abap_true.
          lv_progress = abap_true.
        ENDIF.
      ENDLOOP.
      IF lv_progress = abap_false.
        EXIT.
      ENDIF.
    ENDDO.

    " Phase 1.5 (Package D / D1): pass 1's fixpoint has now converged - any
    " row still unresolved with a declared delta_base and no lt_sha_idx entry
    " is either genuinely external (a prior pack/pull) or truly missing.
    " Bulk-load the full deduplicated set in ONE call via the shared
    " zcl_abapgit_ortec_delta=>bulk_resolve_external_bases helper (also used
    " by the non-streaming resolver, zcl_abapgit_ortec_delta=>resolve_all)
    " instead of relying on pass 2's per-base get_base_bytes/get_object
    " fallback (Package D design §4/§5.1). Every loaded base is merged as a
    " new, already-resolved ct_meta row, indexed into lt_sha_idx so pass 2's
    " ordinary REF_DELTA lookup finds it exactly like an in-pack base, and
    " warmed into the Phase 1 LRU base cache so the base BYTES themselves
    " (fetched via ct_write_batch-miss -> get_base_bytes in pass 2) are also
    " a cache hit, never a second per-base DB read. preload_external_bases
    " below still runs unmodified as a defense-in-depth cache warmer (its own
    " it_sha_idx guard already skips anything this merge just indexed - see
    " .memory/logs/variant_b_package_d_concurrent_commit_impact.md).
    LOOP AT ct_meta ASSIGNING <ls_pending>
        WHERE is_resolved = abap_false
          AND delta_base IS NOT INITIAL.
      READ TABLE lt_sha_idx WITH TABLE KEY sha1 = <ls_pending>-delta_base
        TRANSPORTING NO FIELDS.
      IF sy-subrc <> 0.
        APPEND <ls_pending>-delta_base TO lt_external_bases.
      ENDIF.
    ENDLOOP.

    IF lt_external_bases IS NOT INITIAL.
      TRY.
          lt_loaded_bases = zcl_abapgit_ortec_delta=>bulk_resolve_external_bases(
            iv_repo_key = iv_repo_key
            it_sha1s    = lt_external_bases ).
        CATCH zcx_abapgit_exception INTO lx_bulk_missing.
          " Preserve the existing recovery-tier contract (verified by
          " ltcl_ortec_git~missing_base_no_http_retry): callers of
          " resolve_streaming must still see zcx_abapgit_ortec_git with
          " iv_retry_without_haves = abap_true, never the shared helper's
          " own zcx_abapgit_exception type.
          zcx_abapgit_ortec_git=>raise(
            iv_text                = |{ lx_bulk_missing->get_text( ) }|
            iv_retry_without_haves = abap_true ).
      ENDTRY.

      LOOP AT lt_loaded_bases ASSIGNING <ls_loaded>.
        APPEND INITIAL LINE TO ct_meta ASSIGNING <ls_pending>.
        lv_new_tabix             = sy-tabix.
        <ls_pending>-obj_index   = lv_new_tabix.
        <ls_pending>-obj_type    = <ls_loaded>-type.
        <ls_pending>-sha1        = <ls_loaded>-sha1.
        <ls_pending>-obj_size    = xstrlen( <ls_loaded>-data ).
        <ls_pending>-is_resolved = abap_true.

        ls_sha-sha1  = <ls_loaded>-sha1.
        ls_sha-tabix = lv_new_tabix.
        INSERT ls_sha INTO TABLE lt_sha_idx.

        zcl_abapgit_ortec_base_cache=>get_instance( )->put(
          iv_sha1 = <ls_loaded>-sha1
          iv_data = <ls_loaded>-data ).
      ENDLOOP.
    ENDIF.

    " Pass 1 exhausted all currently in-pack-resolvable dependencies. Bulk
    " preload every locally available external REF_DELTA base before the final
    " pass; missing bases retain the existing retry-without-haves behavior.
    preload_external_bases(
      iv_repo_key = iv_repo_key
      it_meta     = ct_meta
      it_sha_idx  = lt_sha_idx ).

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
          iv_url      = iv_url
        CHANGING
          ct_meta            = ct_meta
          ct_tabix_by_offset = lt_tabix_by_offset
          ct_sha_idx         = lt_sha_idx
          ct_write_batch     = lt_write_batch
          ct_delete_batch    = lt_delete_batch ).
    ENDLOOP.

    flush_resolve_batch(
      EXPORTING iv_repo_key = iv_repo_key
                iv_pack_id  = iv_pack_id
      CHANGING  ct_write_batch  = lt_write_batch
                ct_delete_batch = lt_delete_batch ).

    COMMIT WORK.
  ENDMETHOD.

  METHOD decode_streaming.
    DATA lt_meta       TYPE ty_meta_tt.
    DATA lv_pack_id    TYPE ty_pack_id.
    DATA ls_meta       LIKE LINE OF lt_meta.
    DATA ls_commit_obj TYPE zif_abapgit_definitions=>ty_object.
    DATA lx_resolve    TYPE REF TO zcx_abapgit_ortec_git.
    DATA lx_missing    TYPE REF TO zcx_abapgit_ortec_git.
    DATA lv_original_count TYPE i.

    " NOTE: the thin-pack completion budget (gv_completion_attempts) is
    " deliberately NOT reset here - decode_streaming is also called
    " re-entrantly by zcl_abapgit_ortec_fastpath=>complete_missing_object
    " itself (a completion fetch's own small pack goes through the exact
    " same decode/resolve pipeline, and may itself need a NESTED
    " completion). Resetting the counter here would let each nesting level
    " restart its own fresh budget, defeating the whole point of a shared
    " cap. The budget is reset once per TRUE top-level fetch attempt, in
    " zcl_abapgit_ortec_fastpath=>upload_pack_by_branch/upload_pack_by_commit
    " via reset_completion_budget( ) - see that method's doc.

    lt_meta = decode_and_persist_streaming(
      EXPORTING iv_data     = iv_data
                iv_repo_key = iv_repo_key
      IMPORTING ev_pack_id  = lv_pack_id ).

    " Captured BEFORE resolve_streaming mutates ct_meta - its phase 1.5 can
    " append externally-merged base rows (e.g. a thin REF_DELTA whose
    " declared base is itself a prior commit object) strictly AFTER every
    " row decode_and_persist_streaming actually produced for THIS pack. The
    " commit-extraction loop below must never mistake such a merged row for
    " one of this pack's own new commits.
    lv_original_count = lines( lt_meta ).

    TRY.
        resolve_streaming(
          EXPORTING iv_repo_key = iv_repo_key
                    iv_pack_id  = lv_pack_id
                    iv_url      = iv_url
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
      IF sy-tabix > lv_original_count.
        CONTINUE. " Externally-merged base row (phase 1.5) - not this pack's own commit.
      ENDIF.
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
    DATA lt_batch          TYPE zif_abapgit_definitions=>ty_objects_tt.
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
            " Explicit ALIGN = RIGHT is required: without it, WIDTH/PAD
            " string-template formatting pads on the RIGHT with trailing
            " zeros instead (e.g. index 1, 10, and 100 all produced the
            " IDENTICAL 8-character suffix "10000000"), causing catastrophic
            " temp-key collisions between unrelated delta objects that share
            " a common leading digit. A later object's write then silently
            " overwrote an earlier, different object's delta bytes under the
            " same colliding key - the real root cause of the live
            " "Delta copy instruction exceeds base length" failures.
            " (UNPACK is NOT the right tool here - it targets BCD/packed
            " decimal source fields, not a generic numeric-to-char pad.)
            lv_temp_sha1 = |{ lv_pack_id }{ lv_uindex WIDTH = 8 ALIGN = RIGHT PAD = '0' }|.
            APPEND VALUE #( sha1 = lv_temp_sha1 type = lv_type data = lv_decompressed ) TO lt_batch.
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
            APPEND VALUE #( sha1 = lv_sha1 type = lv_type data = lv_decompressed ) TO lt_batch.
            CLEAR lv_decompressed.

            ls_meta-sha1        = lv_sha1.
            ls_meta-is_resolved = abap_true.
          ENDIF.

          APPEND ls_meta TO rt_meta.

          IF lines( lt_batch ) >= c_batch_size.
            flush_batch(
              EXPORTING iv_repo_key = iv_repo_key
                        iv_pack_id  = lv_pack_id
              CHANGING  ct_batch    = lt_batch ).
          ENDIF.
        ENDDO.

        flush_batch(
          EXPORTING iv_repo_key = iv_repo_key
                    iv_pack_id  = lv_pack_id
          CHANGING  ct_batch    = lt_batch ).

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

