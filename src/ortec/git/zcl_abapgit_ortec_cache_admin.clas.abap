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
        obj_size_mb   TYPE p LENGTH 8 DECIMALS 2,
        idx_entries   TYPE i,
        pack_count    TYPE i,
        pack_mb_disk  TYPE p LENGTH 8 DECIMALS 2,
        commit_count  TYPE i,
        open_sessions TYPE i,
      END OF ty_overview.
    TYPES ty_overview_tt TYPE STANDARD TABLE OF ty_overview WITH DEFAULT KEY.

    "! Build a read-only size/count overview, one row per (repo_key, branch).
    "! Aggregated counts (object/index/pack/commit/session) are computed once
    "! per repo_key and repeated on every branch row of that repository, since
    "! the underlying ZAOG_* tables (other than ZAOG_REPO_STATE) key on
    "! REPO_KEY only, not branch.
    "! @parameter rt_overview |
    "! One row per (repo_key, branch_name) with size/count columns
    CLASS-METHODS get_overview
      RETURNING VALUE(rt_overview) TYPE ty_overview_tt.

    "! Manually clear all cached ZAOG_* rows for one repository.
    "! Acquires the repo-scoped enqueue lock (same lock object as the
    "! fetch/decode path) so a clear can never race a concurrent fetch;
    "! raises immediately (no retry/backoff - this is a foreground,
    "! user-initiated action) if the repo is currently locked.
    "! @parameter iv_repo_key |
    "! Repository key to clear
    "! @parameter rv_message |
    "! Per-table deleted-row summary
    "! @raising zcx_abapgit_ortec_git |
    "! Raised if the repo key is blank, unknown, or currently locked
    CLASS-METHODS clear_repo
      IMPORTING iv_repo_key       TYPE zcl_abapgit_ortec_repo_state=>ty_repo_key
      RETURNING VALUE(rv_message) TYPE string
      RAISING   zcx_abapgit_ortec_git.

  PRIVATE SECTION.
    CLASS-METHODS acquire_lock
      IMPORTING iv_repo_key      TYPE zcl_abapgit_ortec_repo_state=>ty_repo_key
      RETURNING VALUE(rv_locked) TYPE abap_bool.

    CLASS-METHODS release_lock
      IMPORTING iv_repo_key TYPE zcl_abapgit_ortec_repo_state=>ty_repo_key.

ENDCLASS.


CLASS zcl_abapgit_ortec_cache_admin IMPLEMENTATION.

  METHOD get_overview.
    TYPES:
      BEGIN OF ty_obj_agg,
        repo_key  TYPE zaog_obj_store-repo_key,
        obj_count TYPE i,
        obj_size  TYPE p LENGTH 15 DECIMALS 0,
      END OF ty_obj_agg,
      BEGIN OF ty_idx_agg,
        repo_key    TYPE zaog_obj_index-repo_key,
        idx_entries TYPE i,
      END OF ty_idx_agg,
      BEGIN OF ty_pack_agg,
        repo_key   TYPE zaog_pack_meta-repo_key,
        pack_count TYPE i,
      END OF ty_pack_agg,
      BEGIN OF ty_pack_disk_agg,
        repo_key  TYPE zaog_pack_meta-repo_key,
        pack_size TYPE p LENGTH 15 DECIMALS 0,
      END OF ty_pack_disk_agg,
      BEGIN OF ty_commit_agg,
        repo_key     TYPE zaog_commit_hist-repo_key,
        commit_count TYPE i,
      END OF ty_commit_agg,
      BEGIN OF ty_sess_agg,
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

    SELECT *
      FROM zaog_repo_state
      ORDER BY repo_key, branch_name
      INTO TABLE @lt_state.
    IF lt_state IS INITIAL.
      RETURN.
    ENDIF.

    SELECT repo_key, COUNT(*) AS obj_count, SUM( obj_size ) AS obj_size
      FROM zaog_obj_store
      GROUP BY repo_key
      INTO TABLE @lt_obj_agg.

    SELECT repo_key, COUNT(*) AS idx_entries
      FROM zaog_obj_index
      GROUP BY repo_key
      INTO TABLE @lt_idx_agg.

    SELECT repo_key, COUNT(*) AS pack_count
      FROM zaog_pack_meta
      GROUP BY repo_key
      INTO TABLE @lt_pack_agg.

    SELECT repo_key, SUM( total_size ) AS pack_size
      FROM zaog_pack_meta
      WHERE raw_stored = @abap_true
      GROUP BY repo_key
      INTO TABLE @lt_pack_disk_agg.

    SELECT repo_key, COUNT(*) AS commit_count
      FROM zaog_commit_hist
      GROUP BY repo_key
      INTO TABLE @lt_commit_agg.

    SELECT repo_key, COUNT(*) AS open_sessions
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

      READ TABLE lt_obj_agg ASSIGNING FIELD-SYMBOL(<ls_obj_agg>) WITH KEY repo_key = <ls_state>-repo_key.
      IF sy-subrc = 0.
        ls_overview-obj_count   = <ls_obj_agg>-obj_count.
        ls_overview-obj_size_mb = <ls_obj_agg>-obj_size / 1048576.
      ENDIF.

      READ TABLE lt_idx_agg ASSIGNING FIELD-SYMBOL(<ls_idx_agg>) WITH KEY repo_key = <ls_state>-repo_key.
      IF sy-subrc = 0.
        ls_overview-idx_entries = <ls_idx_agg>-idx_entries.
      ENDIF.

      READ TABLE lt_pack_agg ASSIGNING FIELD-SYMBOL(<ls_pack_agg>) WITH KEY repo_key = <ls_state>-repo_key.
      IF sy-subrc = 0.
        ls_overview-pack_count = <ls_pack_agg>-pack_count.
      ENDIF.

      READ TABLE lt_pack_disk_agg ASSIGNING FIELD-SYMBOL(<ls_pack_disk_agg>) WITH KEY repo_key = <ls_state>-repo_key.
      IF sy-subrc = 0.
        ls_overview-pack_mb_disk = <ls_pack_disk_agg>-pack_size / 1048576.
      ENDIF.

      READ TABLE lt_commit_agg ASSIGNING FIELD-SYMBOL(<ls_commit_agg>) WITH KEY repo_key = <ls_state>-repo_key.
      IF sy-subrc = 0.
        ls_overview-commit_count = <ls_commit_agg>-commit_count.
      ENDIF.

      READ TABLE lt_sess_agg ASSIGNING FIELD-SYMBOL(<ls_sess_agg>) WITH KEY repo_key = <ls_state>-repo_key.
      IF sy-subrc = 0.
        ls_overview-open_sessions = <ls_sess_agg>-open_sessions.
      ENDIF.

      APPEND ls_overview TO rt_overview.
    ENDLOOP.
  ENDMETHOD.

  METHOD clear_repo.
    DATA lv_url    TYPE string.
    DATA ls_result TYPE zcl_abapgit_ortec_git_switch=>ty_clear_result.

    IF iv_repo_key IS INITIAL.
      zcx_abapgit_ortec_git=>raise( 'Cache admin: repository key required' ).
    ENDIF.

    SELECT SINGLE remote_url
      FROM zaog_repo_state
      WHERE repo_key = @iv_repo_key
      INTO @lv_url.
    IF sy-subrc <> 0 OR lv_url IS INITIAL.
      zcx_abapgit_ortec_git=>raise( |Cache admin: no cached state for repo key { iv_repo_key }| ).
    ENDIF.

    IF acquire_lock( iv_repo_key ) = abap_false.
      zcx_abapgit_ortec_git=>raise(
        |Cache admin: repository { iv_repo_key } is locked by a concurrent fetch - try again shortly| ).
    ENDIF.

    TRY.
        ls_result = zcl_abapgit_ortec_git_switch=>clear_repo_cache( lv_url ).
      CATCH zcx_abapgit_ortec_git INTO DATA(lx_error).
        release_lock( iv_repo_key ).
        RAISE EXCEPTION lx_error.
    ENDTRY.

    release_lock( iv_repo_key ).

    rv_message = zcl_abapgit_ortec_git_switch=>format_clear_result( ls_result ).
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

ENDCLASS.
