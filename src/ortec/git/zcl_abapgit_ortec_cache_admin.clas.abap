"! <p class="shorttext synchronized">ORTEC Cache Admin - Size/Count Overview + Manual Clear</p>
"! Backing class for the ZABAPGIT_ORTEC_CACHE_ADMIN report (Phase 6, D5).
"! Off the hot path: never called from Stage/Diff/Patch/fetch. Provides a
"! read-only per-repository size/count overview and a safeguarded manual
"! clear action that reuses {@link METH:zcl_abapgit_ortec_git_switch.clear_repo_cache}.
"! <p>Compact / ref-cleanup / stale-session cleanup (also scoped for Phase 6 in the
"! design) are intentionally NOT implemented here: computing a provably-complete
"! delta-base-safe reachable set for compaction is a substantial, higher-risk
"! undertaking that deserves its own dedicated pass. Per the design's own
"! documented fallback, this class offers only the always-safe size report and
"! whole-repo manual clear until that follow-up is done.</p>
CLASS zcl_abapgit_ortec_cache_admin DEFINITION
  PUBLIC FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    TYPES:
      BEGIN OF ty_overview,
        repo_key      TYPE zcl_abapgit_ortec_repo_state=>ty_repo_key,
        remote_url    TYPE zaog_repo_state-remote_url,
        branch_name   TYPE zaog_repo_state-branch_name,
        curr_commit   TYPE zaog_repo_state-curr_commit,
        fetch_commit  TYPE zaog_repo_state-fetch_commit,
        fetch_ts      TYPE zaog_repo_state-fetch_ts,
        is_shallow    TYPE zaog_repo_state-is_shallow,
        obj_count     TYPE i,
        obj_size_mb   TYPE p LENGTH 8                                DECIMALS 2,
        idx_entries   TYPE i,
        pack_count    TYPE i,
        pack_mb_disk  TYPE p LENGTH 8                                DECIMALS 2,
        commit_count  TYPE i,
        open_sessions TYPE i,
      END OF ty_overview.
    TYPES ty_overview_tt TYPE STANDARD TABLE OF ty_overview WITH DEFAULT KEY.

    TYPES: BEGIN OF ty_repo_f4,
             repo_key    TYPE zcl_abapgit_ortec_repo_state=>ty_repo_key,
             branch_name TYPE c LENGTH 255,
             remote_url  TYPE c LENGTH 255,
           END OF ty_repo_f4.
    TYPES ty_repo_f4_tt TYPE STANDARD TABLE OF ty_repo_f4 WITH DEFAULT KEY.
    TYPES ty_repo_key_tt TYPE STANDARD TABLE OF zcl_abapgit_ortec_repo_state=>ty_repo_key WITH EMPTY KEY.

    TYPES:
      BEGIN OF ty_clear_result,
        repo_key    TYPE zcl_abapgit_ortec_repo_state=>ty_repo_key,
        obj_store   TYPE i,
        obj_index   TYPE i,
        pack_idx    TYPE i,
        pack_meta   TYPE i,
        raw_pack    TYPE i,
        fetch_sess  TYPE i,
        commit_hist TYPE i,
        repo_state  TYPE i,
      END OF ty_clear_result.

    "! Build a read-only size/count overview, one row per (repo_key, branch).
    "! Aggregated counts (object/index/pack/commit/session) are computed once
    "! per repo_key and repeated on every branch row of that repository, since
    "! the underlying ZAOG_* tables (other than ZAOG_REPO_STATE) key on
    "! REPO_KEY only, not branch.
    "! @parameter rt_overview |
    "! One row per (repo_key, branch_name) with size/count columns
    CLASS-METHODS get_overview
      RETURNING VALUE(rt_overview) TYPE ty_overview_tt.

    "! Return repository keys currently known in ZAOG_REPO_STATE for F4 help.
    "! @parameter rt_repo_f4 |
    "! Repository-key value help rows with repo key, branch and remote URL
    CLASS-METHODS get_repo_f4_values
      RETURNING VALUE(rt_repo_f4) TYPE ty_repo_f4_tt.

    "! Delete all persistent ORTEC cache and materialization state for one
    "! repository key.
    "!
    "! The operation is independent of REMOTE_URL because partially created
    "! or legacy repository-state rows may not contain URL metadata.
    "!
    "! Acquires the repository administration lock, deletes all repository-
    "! scoped cache rows, invalidates the internal object-store cache and
    "! performs one final COMMIT WORK AND WAIT.
    CLASS-METHODS clear_repo
      IMPORTING
        iv_repo_key      TYPE zcl_abapgit_ortec_repo_state=>ty_repo_key
      RETURNING
        VALUE(rs_result) TYPE ty_clear_result
      RAISING
        zcx_abapgit_ortec_git.

    CLASS-METHODS format_clear_result
      IMPORTING
        is_result         TYPE ty_clear_result
      RETURNING
        VALUE(rv_message) TYPE string.

  PRIVATE SECTION.
    CLASS-METHODS acquire_lock
      IMPORTING iv_repo_key      TYPE zcl_abapgit_ortec_repo_state=>ty_repo_key
      RETURNING VALUE(rv_locked) TYPE abap_bool.

    CLASS-METHODS release_lock
      IMPORTING iv_repo_key TYPE zcl_abapgit_ortec_repo_state=>ty_repo_key.

ENDCLASS.


CLASS zcl_abapgit_ortec_cache_admin IMPLEMENTATION.
  METHOD get_overview.
    TYPES: BEGIN OF ty_obj_agg,
             repo_key  TYPE zaog_obj_store-repo_key,
             obj_count TYPE i,
             obj_size  TYPE p LENGTH 15 DECIMALS 0,
           END OF ty_obj_agg.
    TYPES: BEGIN OF ty_idx_agg,
             repo_key    TYPE zaog_obj_index-repo_key,
             idx_entries TYPE i,
           END OF ty_idx_agg.
    TYPES: BEGIN OF ty_pack_agg,
             repo_key   TYPE zaog_pack_meta-repo_key,
             pack_count TYPE i,
           END OF ty_pack_agg.
    TYPES: BEGIN OF ty_pack_disk_agg,
             repo_key  TYPE zaog_pack_meta-repo_key,
             pack_size TYPE p LENGTH 15 DECIMALS 0,
           END OF ty_pack_disk_agg.
    TYPES: BEGIN OF ty_commit_agg,
             repo_key     TYPE zaog_commit_hist-repo_key,
             commit_count TYPE i,
           END OF ty_commit_agg.
    TYPES: BEGIN OF ty_sess_agg,
             repo_key      TYPE zaog_fetch_sess-repo_key,
             open_sessions TYPE i,
           END OF ty_sess_agg.

    DATA lt_state         TYPE STANDARD TABLE OF zaog_repo_state.
    DATA lt_obj_agg       TYPE STANDARD TABLE OF ty_obj_agg.
    DATA lt_idx_agg       TYPE STANDARD TABLE OF ty_idx_agg.
    DATA lt_pack_agg      TYPE STANDARD TABLE OF ty_pack_agg.
    DATA lt_pack_disk_agg TYPE STANDARD TABLE OF ty_pack_disk_agg.
    DATA lt_commit_agg    TYPE STANDARD TABLE OF ty_commit_agg.
    DATA lt_sess_agg      TYPE STANDARD TABLE OF ty_sess_agg.
    DATA ls_overview      TYPE ty_overview.

    FIELD-SYMBOLS <ls_state> LIKE LINE OF lt_state.

    SELECT * FROM zaog_repo_state
      ORDER BY repo_key, branch_name
      INTO TABLE @lt_state.
    IF lt_state IS INITIAL.
      RETURN.
    ENDIF.

    SELECT repo_key,
           COUNT(*)        AS obj_count,
           SUM( CAST( obj_size AS DEC( 31, 0 ) ) ) AS obj_size
      FROM zaog_obj_store
      GROUP BY repo_key
      INTO TABLE @lt_obj_agg.

    SELECT repo_key,
           COUNT(*) AS idx_entries
      FROM zaog_obj_index
      GROUP BY repo_key
      INTO TABLE @lt_idx_agg.

    SELECT repo_key,
           COUNT(*) AS pack_count
      FROM zaog_pack_meta
      GROUP BY repo_key
      INTO TABLE @lt_pack_agg.

    SELECT repo_key,
           SUM( CAST( total_size AS DEC( 31, 0 ) ) ) AS pack_size
      FROM zaog_pack_meta
      WHERE raw_stored = @abap_true
      GROUP BY repo_key
      INTO TABLE @lt_pack_disk_agg.

    SELECT repo_key,
           COUNT(*) AS commit_count
      FROM zaog_commit_hist
      GROUP BY repo_key
      INTO TABLE @lt_commit_agg.

    SELECT repo_key,
           COUNT(*) AS open_sessions
      FROM zaog_fetch_sess
      WHERE status = 'A'
      GROUP BY repo_key
      INTO TABLE @lt_sess_agg.

    LOOP AT lt_state ASSIGNING <ls_state>.
      CLEAR ls_overview.
      ls_overview-repo_key     = <ls_state>-repo_key.
      ls_overview-remote_url   = <ls_state>-remote_url.
      ls_overview-branch_name  = <ls_state>-branch_name.
      ls_overview-curr_commit  = <ls_state>-curr_commit.
      ls_overview-fetch_commit = <ls_state>-fetch_commit.
      ls_overview-fetch_ts     = <ls_state>-fetch_ts.
      ls_overview-is_shallow   = <ls_state>-is_shallow.

      ASSIGN lt_obj_agg[ repo_key = <ls_state>-repo_key ] TO FIELD-SYMBOL(<ls_obj_agg>).
      IF sy-subrc = 0.
        ls_overview-obj_count   = <ls_obj_agg>-obj_count.
        ls_overview-obj_size_mb = <ls_obj_agg>-obj_size / 1048576.
      ENDIF.

      ASSIGN lt_idx_agg[ repo_key = <ls_state>-repo_key ] TO FIELD-SYMBOL(<ls_idx_agg>).
      IF sy-subrc = 0.
        ls_overview-idx_entries = <ls_idx_agg>-idx_entries.
      ENDIF.

      ASSIGN lt_pack_agg[ repo_key = <ls_state>-repo_key ] TO FIELD-SYMBOL(<ls_pack_agg>).
      IF sy-subrc = 0.
        ls_overview-pack_count = <ls_pack_agg>-pack_count.
      ENDIF.

      ASSIGN lt_pack_disk_agg[ repo_key = <ls_state>-repo_key ] TO FIELD-SYMBOL(<ls_pack_disk_agg>).
      IF sy-subrc = 0.
        ls_overview-pack_mb_disk = <ls_pack_disk_agg>-pack_size / 1048576.
      ENDIF.

      ASSIGN lt_commit_agg[ repo_key = <ls_state>-repo_key ] TO FIELD-SYMBOL(<ls_commit_agg>).
      IF sy-subrc = 0.
        ls_overview-commit_count = <ls_commit_agg>-commit_count.
      ENDIF.

      ASSIGN lt_sess_agg[ repo_key = <ls_state>-repo_key ] TO FIELD-SYMBOL(<ls_sess_agg>).
      IF sy-subrc = 0.
        ls_overview-open_sessions = <ls_sess_agg>-open_sessions.
      ENDIF.

      APPEND ls_overview TO rt_overview.
    ENDLOOP.
  ENDMETHOD.

  METHOD get_repo_f4_values.

    TYPES ty_repo_key_set TYPE HASHED TABLE OF
      zcl_abapgit_ortec_repo_state=>ty_repo_key
      WITH UNIQUE KEY table_line.

    DATA lt_keys     TYPE ty_repo_key_set.
    DATA lt_state    TYPE STANDARD TABLE OF zaog_repo_state.
    DATA ls_repo_f4  TYPE ty_repo_f4.
    DATA lv_repo_key TYPE zcl_abapgit_ortec_repo_state=>ty_repo_key.

    SELECT *
      FROM zaog_repo_state
      INTO TABLE @lt_state.

    LOOP AT lt_state ASSIGNING FIELD-SYMBOL(<ls_state>).

      INSERT <ls_state>-repo_key INTO TABLE lt_keys.

      CLEAR ls_repo_f4.
      ls_repo_f4-repo_key   = <ls_state>-repo_key.
      ls_repo_f4-branch_name = <ls_state>-branch_name.
      ls_repo_f4-remote_url = <ls_state>-remote_url.

      IF ls_repo_f4-branch_name CP 'refs/heads/*'.
        DATA(lv_prefix_length) = strlen( 'refs/heads/' ).
        SHIFT ls_repo_f4-branch_name
          BY lv_prefix_length PLACES.
      ENDIF.

      APPEND ls_repo_f4 TO rt_repo_f4.

    ENDLOOP.

    SELECT DISTINCT repo_key
      FROM zaog_obj_store
      INTO TABLE @DATA(lt_object_keys).

    LOOP AT lt_object_keys INTO lv_repo_key.

      IF line_exists( lt_keys[ table_line = lv_repo_key ] ).
        CONTINUE.
      ENDIF.

      INSERT lv_repo_key INTO TABLE lt_keys.

      APPEND VALUE #(
        repo_key   = lv_repo_key
        branch_name = '<orphaned cache>'
        remote_url = '<no repository state>' )
        TO rt_repo_f4.

    ENDLOOP.

    SELECT DISTINCT repo_key
      FROM zaog_commit_hist
      INTO TABLE @DATA(lt_history_keys).

    LOOP AT lt_history_keys INTO lv_repo_key.

      IF line_exists( lt_keys[ table_line = lv_repo_key ] ).
        CONTINUE.
      ENDIF.

      INSERT lv_repo_key INTO TABLE lt_keys.

      APPEND VALUE #(
        repo_key   = lv_repo_key
        branch_name = '<orphaned certificate>'
        remote_url = '<no repository state>' )
        TO rt_repo_f4.

    ENDLOOP.

    SORT rt_repo_f4 BY repo_key branch_name.

  ENDMETHOD.

  METHOD clear_repo.

    IF iv_repo_key IS INITIAL.
      zcx_abapgit_ortec_git=>raise(
        'Cache admin: repository key required' ).
    ENDIF.

    " Do not require REMOTE_URL. A repository may have object/cache state
    " even when its repository-state URL metadata is incomplete.
    SELECT SINGLE repo_key
      FROM zaog_repo_state
      INTO @DATA(lv_state_key)
      WHERE repo_key = @iv_repo_key.

    IF sy-subrc <> 0.

      SELECT SINGLE repo_key
        FROM zaog_obj_store
        INTO @DATA(lv_object_key)
        WHERE repo_key = @iv_repo_key.

      IF sy-subrc <> 0.

        SELECT SINGLE repo_key
          FROM zaog_commit_hist
          INTO @DATA(lv_hist_key)
          WHERE repo_key = @iv_repo_key.

        IF sy-subrc <> 0.
          zcx_abapgit_ortec_git=>raise(
            |Cache admin: no cached data for repo key { iv_repo_key }| ).
        ENDIF.

      ENDIF.

    ENDIF.

    IF acquire_lock( iv_repo_key ) = abap_false.
      zcx_abapgit_ortec_git=>raise(
        |Cache admin: repository { iv_repo_key } is locked by a concurrent operation| ).
    ENDIF.

    rs_result-repo_key = iv_repo_key.

    TRY.

        " Delete dependent/derived data before parent-like repository state.
        DELETE FROM zaog_obj_index
          WHERE repo_key = iv_repo_key.
        rs_result-obj_index = sy-dbcnt.

        DELETE FROM zaog_pack_idx
          WHERE repo_key = iv_repo_key.
        rs_result-pack_idx = sy-dbcnt.

        DELETE FROM zaog_raw_pack
          WHERE repo_key = iv_repo_key.
        rs_result-raw_pack = sy-dbcnt.

        DELETE FROM zaog_pack_meta
          WHERE repo_key = iv_repo_key.
        rs_result-pack_meta = sy-dbcnt.

        DELETE FROM zaog_fetch_sess
          WHERE repo_key = iv_repo_key.
        rs_result-fetch_sess = sy-dbcnt.

        DELETE FROM zaog_commit_hist
          WHERE repo_key = iv_repo_key.
        rs_result-commit_hist = sy-dbcnt.

        DELETE FROM zaog_obj_store
          WHERE repo_key = iv_repo_key.
        rs_result-obj_store = sy-dbcnt.

        DELETE FROM zaog_repo_state
          WHERE repo_key = iv_repo_key.
        rs_result-repo_state = sy-dbcnt.

        zcl_abapgit_ortec_obj_store=>invalidate_cache( ).

        COMMIT WORK AND WAIT.

      CATCH cx_root INTO DATA(lx_error).

        ROLLBACK WORK.
        zcl_abapgit_ortec_obj_store=>invalidate_cache( ).
        release_lock( iv_repo_key ).

        RAISE EXCEPTION TYPE zcx_abapgit_ortec_git
          EXPORTING
            iv_text  =
                       |Cache admin: failed to clear repository { iv_repo_key }: { lx_error->get_text( ) }|
            previous = lx_error.

    ENDTRY.

    release_lock( iv_repo_key ).

  ENDMETHOD.

  METHOD acquire_lock.
    DATA lv_session_id TYPE zaog_fetch_sess-session_id.

    lv_session_id = iv_repo_key.

    CALL FUNCTION 'ENQUEUE_EZAOG_REPO_LOCK'
      EXPORTING
        mode_zaog_fetch_sess = 'E'
        session_id           = lv_session_id
        _scope               = '2'
        _wait                = space
        _collect             = space
      EXCEPTIONS
        foreign_lock         = 1
        system_failure       = 2
        OTHERS               = 3.
    rv_locked = xsdbool( sy-subrc = 0 ).
  ENDMETHOD.

  METHOD release_lock.
    DATA lv_session_id TYPE zaog_fetch_sess-session_id.

    lv_session_id = iv_repo_key.

    CALL FUNCTION 'DEQUEUE_EZAOG_REPO_LOCK'
      EXPORTING
        mode_zaog_fetch_sess = 'E'
        session_id           = lv_session_id
        _scope               = '2'
        _synchron            = space
        _collect             = space.
  ENDMETHOD.

  METHOD format_clear_result.

    DATA lv_total TYPE i.

    lv_total =
        is_result-obj_store
      + is_result-obj_index
      + is_result-pack_idx
      + is_result-pack_meta
      + is_result-raw_pack
      + is_result-fetch_sess
      + is_result-commit_hist
      + is_result-repo_state.

    rv_message =
      |ORTEC cache cleared for repo key { is_result-repo_key }: | &&
      |{ lv_total } row(s) removed | &&
      |[OBJ_STORE={ is_result-obj_store }, | &&
      |OBJ_INDEX={ is_result-obj_index }, | &&
      |PACK_IDX={ is_result-pack_idx }, | &&
      |PACK_META={ is_result-pack_meta }, | &&
      |RAW_PACK={ is_result-raw_pack }, | &&
      |FETCH_SESS={ is_result-fetch_sess }, | &&
      |COMMIT_HIST={ is_result-commit_hist }, | &&
      |REPO_STATE={ is_result-repo_state }]|.

  ENDMETHOD.
ENDCLASS.

