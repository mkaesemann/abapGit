"! <p class="shorttext synchronized">ORTEC Git FastPath Orchestrator</p>
"! Entry point called from standard abapGit hooks.
"! Orchestrates incremental fetch with persistent object store.
CLASS zcl_abapgit_ortec_fastpath DEFINITION
  PUBLIC FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.
    "! Attempt ORTEC fast-path pull by branch.
    "! Returns INITIAL result if fast-path cannot be applied
    "! (no stored state, first fetch, etc.).
    "! @parameter iv_url |
    "! Remote URL
    "! @parameter iv_branch_name |
    "! Branch ref name
    "! @parameter iv_deepen_level |
    "! Deepen level
    "! @parameter rs_result |
    "! Pull result (INITIAL if fast-path not applicable)
    "! @raising zcx_abapgit_ortec_git |
    "! On ORTEC-specific error (caller falls back)
    CLASS-METHODS pull_by_branch
      IMPORTING iv_url           TYPE string
                iv_branch_name   TYPE string
                iv_deepen_level  TYPE i DEFAULT 1
      RETURNING VALUE(rs_result) TYPE zcl_abapgit_git_porcelain=>ty_pull_result
      RAISING   zcx_abapgit_ortec_git
                zcx_abapgit_exception.

    "! ORTEC-aware upload-pack by branch.
    CLASS-METHODS upload_pack_by_branch
      IMPORTING
        iv_url          TYPE string
        iv_branch_name  TYPE string
        iv_deepen_level TYPE i DEFAULT 1
        it_branches     TYPE zif_abapgit_git_definitions=>ty_git_branch_list_tt OPTIONAL
      EXPORTING
        et_objects      TYPE zif_abapgit_definitions=>ty_objects_tt
        ev_branch       TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING
        zcx_abapgit_ortec_git
        zcx_abapgit_exception.

    "! ORTEC-aware upload-pack by commit.
    CLASS-METHODS upload_pack_by_commit
      IMPORTING
        iv_url          TYPE string
        iv_hash         TYPE zif_abapgit_git_definitions=>ty_sha1
        iv_deepen_level TYPE i DEFAULT 0
      EXPORTING
        et_objects      TYPE zif_abapgit_definitions=>ty_objects_tt
        ev_commit       TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING
        zcx_abapgit_ortec_git
        zcx_abapgit_exception.

    "! Persist objects and state after a successful pull.
    "! Called as post-pull hook. Silently ignored on error.
    "! @parameter iv_url |
    "! Remote URL
    "! @parameter iv_branch_name |
    "! Branch ref name
    "! @parameter iv_commit |
    "! Fetched commit SHA1
    "! @parameter it_objects |
    "! Decoded objects
    "! @parameter iv_repo_key |
    "! Optional repo key (if known)
    "! @raising zcx_abapgit_ortec_git |
    "! On error
    CLASS-METHODS persist_pull_result
      IMPORTING iv_url         TYPE string
                iv_branch_name TYPE string
                iv_commit      TYPE zif_abapgit_git_definitions=>ty_sha1
                it_objects     TYPE zif_abapgit_definitions=>ty_objects_tt
                iv_repo_key    TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key OPTIONAL
      RAISING   zcx_abapgit_ortec_git.

    "! Fetch tip commit objects for branch metadata — read-only, no ORTEC fastpath routing.
    "! Used by the branch picker to retrieve last-changed dates without touching
    "! the persistent object store or delta-negotiation state.
    "! @parameter iv_url |
    "! Remote URL
    "! @parameter iv_branch |
    "! Any branch name (used to initialise the v1 connection)
    "! @parameter it_branches |
    "! Branch list whose tip SHAs should be fetched
    "! @parameter et_objects |
    "! Decoded git objects (commits only, filtered by caller)
    "! @raising zcx_abapgit_exception |
    "! On network or protocol error
    "! @raising zcx_abapgit_ortec_git |
    "! On ORTEC Git transport error
    CLASS-METHODS fetch_tip_commits
      IMPORTING iv_url      TYPE string
                iv_branch   TYPE string
                it_branches TYPE zif_abapgit_git_definitions=>ty_git_branch_list_tt
      EXPORTING et_objects  TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_exception
                zcx_abapgit_ortec_git.

    "! Attempt to make iv_commit's commit + tree structure available in the
    "! persistent object store via a `filter blob:none` negotiated fetch -
    "! commit and every reachable tree object (proportional to directory
    "! structure), but deliberately NO blob content. Intended for a filtered
    "! Stage/Diff resolution (zcl_abapgit_ortec_filter_walk) on a commit that
    "! has no usable warm index yet: once commit+trees are locally available,
    "! zcl_abapgit_ortec_obj_index=>get_files_for_filter can build its index
    "! and resolve the caller's actual filtered file set via its own
    "! existing best-effort blob top-up (zcl_abapgit_ortec_missing_obj=>
    "! ensure_available) - which is a no-op whenever those specific blobs
    "! are already known from another buffered branch (blobs are
    "! content-addressed and commonly shared across sibling branches/
    "! commits) - instead of this caller ever decoding/persisting every
    "! object reachable from iv_commit.
    "! Never raises: returns abap_false whenever the server does not
    "! advertise `filter`, the commit is already available locally, or
    "! anything else prevents the attempt, so the caller can unconditionally
    "! fall through to its existing safe path (a normal full/deepen fetch).
    "! @parameter iv_url |
    "! Remote URL
    "! @parameter iv_branch_name |
    "! Any branch name, used only to initialise the v1 connection
    "! @parameter iv_commit |
    "! Target commit SHA1 (already resolved by the caller)
    "! @parameter iv_repo_key |
    "! ORTEC repository key
    "! @parameter rv_applicable |
    "! ABAP_TRUE if the commit + its trees are now available locally
    CLASS-METHODS try_filtered_commit_fetch
      IMPORTING iv_url             TYPE string
                iv_branch_name     TYPE string
                iv_commit          TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_repo_key        TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
      RETURNING VALUE(rv_applicable) TYPE abap_bool.

  PRIVATE SECTION.
    "! Resolve repo key from URL. Creates new key if none found.
    "! @parameter iv_url |
    "! Remote URL
    "! @parameter rv_key |
    "! Repository key (always non-empty)
    CLASS-METHODS resolve_repo_key
      IMPORTING iv_url        TYPE string
      RETURNING VALUE(rv_key) TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.

    CLASS-METHODS upload_pack
      IMPORTING
        io_client       TYPE REF TO zcl_abapgit_http_client
        iv_url          TYPE string
        iv_deepen_level TYPE i DEFAULT 0
        it_hashes       TYPE zif_abapgit_git_definitions=>ty_sha1_tt
        iv_allow_thin   TYPE abap_bool DEFAULT abap_false
      RETURNING
        VALUE(rt_objects) TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING
        zcx_abapgit_ortec_git
        zcx_abapgit_exception.

    CLASS-METHODS parse
      EXPORTING
        ev_pack TYPE xstring
      CHANGING
        cv_data TYPE xstring
      RAISING
        zcx_abapgit_ortec_git
        zcx_abapgit_exception.

    "! Serve already-known-complete objects from the local store when the
    "! server's response confirms our have-set already covers everything
    "! reachable from it_hashes (either no pack section at all, or a
    "! well-formed pack that decodes to zero objects - both carry the same
    "! meaning: the server has nothing new to send).
    "! @raising zcx_abapgit_ortec_git |
    "! Raised whenever the local store cannot safely stand in for the
    "! missing pack (multi-want request, incomplete tree, or repo key
    "! unresolved) - callers should treat this like any other cascade
    "! failure.
    CLASS-METHODS serve_cached_when_nothing_new
      IMPORTING
        iv_url    TYPE string
        it_hashes TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RETURNING
        VALUE(rt_objects) TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING
        zcx_abapgit_ortec_git.

ENDCLASS.


CLASS zcl_abapgit_ortec_fastpath IMPLEMENTATION.
  METHOD fetch_tip_commits.

    DATA lo_client     TYPE REF TO zcl_abapgit_http_client.
    DATA lv_buffer     TYPE string.
    DATA lv_line       TYPE string.
    DATA lv_capa       TYPE string.
    DATA lv_xstring    TYPE xstring.
    DATA lv_pack       TYPE xstring.
    DATA lt_hashes     TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv_ref_data   TYPE string.
    DATA lv_null       TYPE c LENGTH 1.
    DATA lv_has_filter TYPE abap_bool.
    DATA lv_null_pos   TYPE i.
    DATA lv_nl_pos     TYPE i.
    DATA lv_caps       TYPE string.
    DATA lv_offset     TYPE i.

    FIELD-SYMBOLS <ls_branch> LIKE LINE OF it_branches.
    FIELD-SYMBOLS <lv_sha1>   LIKE LINE OF lt_hashes.

    " Collect unique tip SHAs from the branch list
    LOOP AT it_branches ASSIGNING <ls_branch>
        WHERE sha1 IS NOT INITIAL.
      APPEND <ls_branch>-sha1 TO lt_hashes.
    ENDLOOP.
    SORT lt_hashes.
    DELETE ADJACENT DUPLICATES FROM lt_hashes.

    IF lt_hashes IS INITIAL.
      RETURN.
    ENDIF.

    " Open a standard v1 upload-pack connection — no ORTEC fastpath routing
    zcl_abapgit_git_transport=>find_branch_ortec(
      EXPORTING
        iv_url         = iv_url
        iv_service     = 'upload'
        iv_branch_name = iv_branch
      IMPORTING
        eo_client = lo_client ).

    " Check server capabilities for 'filter' (BEFORE set_headers to preserve response)
    lv_ref_data = lo_client->get_cdata( ).
    lv_null = zcl_abapgit_git_utils=>get_null( ).
    FIND FIRST OCCURRENCE OF lv_null IN lv_ref_data MATCH OFFSET lv_null_pos.
    IF sy-subrc = 0.
      TRY.
          lv_offset = lv_null_pos + 1.
          lv_caps = lv_ref_data+lv_offset.
          FIND FIRST OCCURRENCE OF cl_abap_char_utilities=>newline IN lv_caps
            MATCH OFFSET lv_nl_pos.
          IF sy-subrc = 0 AND lv_nl_pos > 0.
            lv_caps = lv_caps(lv_nl_pos).
            lv_has_filter = xsdbool( lv_caps CS 'filter' ).
          ENDIF.
        CATCH cx_sy_range_out_of_bounds.
          " Malformed/unexpected capability advertisement - treat as "no
          " filter capability", which the existing check below already
          " handles safely (returns early), never a hard failure.
          lv_has_filter = abap_false.
      ENDTRY.
    ENDIF.

    IF lv_has_filter = abap_false.
      " Server does not advertise 'filter': without it the server would send
      " commits + ALL trees + ALL blobs, potentially gigabytes for large repos.
      " Returning empty is safer than risking SYSTEM_NO_ROLL.
      RETURN.
    ENDIF.

    lo_client->set_headers( iv_url     = iv_url
                            iv_service = 'upload' ).

    " Build v1 want + deepen 1 + filter tree:0 request (commits only)
    lv_capa = 'side-band-64k no-progress multi_ack filter'.
    LOOP AT lt_hashes ASSIGNING <lv_sha1>.
      IF sy-tabix = 1.
        lv_line = |want { <lv_sha1> } { lv_capa }{ cl_abap_char_utilities=>newline }|.
      ELSE.
        lv_line = |want { <lv_sha1> }{ cl_abap_char_utilities=>newline }|.
      ENDIF.
      lv_buffer = lv_buffer && zcl_abapgit_git_utils=>pkt_string( lv_line ).
    ENDLOOP.

    lv_buffer = lv_buffer
      && zcl_abapgit_git_utils=>pkt_string( |deepen 1{ cl_abap_char_utilities=>newline }| )
      && zcl_abapgit_git_utils=>pkt_string( |filter tree:0{ cl_abap_char_utilities=>newline }| )
      && '0000'
      && '0009done' && cl_abap_char_utilities=>newline.

    lv_xstring = lo_client->send_receive_close(
      zcl_abapgit_convert=>string_to_xstring_utf8( lv_buffer ) ).

    parse( IMPORTING ev_pack = lv_pack
           CHANGING  cv_data = lv_xstring ).

    IF lv_pack IS INITIAL.
      RETURN.
    ENDIF.

    et_objects = zcl_abapgit_ortec_pack_dec=>decode_commits_only( lv_pack ).

  ENDMETHOD.

  METHOD try_filtered_commit_fetch.

    DATA lo_client     TYPE REF TO zcl_abapgit_http_client.
    DATA lv_buffer     TYPE string.
    DATA lv_line       TYPE string.
    DATA lv_capa       TYPE string.
    DATA lv_xstring    TYPE xstring.
    DATA lv_pack       TYPE xstring.
    DATA lv_ref_data   TYPE string.
    DATA lv_null       TYPE c LENGTH 1.
    DATA lv_has_filter TYPE abap_bool.
    DATA lv_null_pos   TYPE i.
    DATA lv_nl_pos     TYPE i.
    DATA lv_caps       TYPE string.
    DATA lv_offset     TYPE i.
    DATA lt_objects    TYPE zif_abapgit_definitions=>ty_objects_tt.

    IF iv_url IS INITIAL OR iv_commit IS INITIAL OR iv_repo_key IS INITIAL.
      RETURN.
    ENDIF.

    IF zcl_abapgit_ortec_obj_store=>exists( iv_repo_key = iv_repo_key iv_sha1 = iv_commit ) = abap_true.
      " Commit already present locally (e.g. reachable from an already
      " buffered branch) - nothing to fetch.
      rv_applicable = abap_true.
      RETURN.
    ENDIF.

    TRY.
        " Open a standard v1 upload-pack connection - same pattern as
        " fetch_tip_commits, no ORTEC fastpath routing needed for this
        " one-shot structural fetch.
        zcl_abapgit_git_transport=>find_branch_ortec(
          EXPORTING
            iv_url         = iv_url
            iv_service     = 'upload'
            iv_branch_name = iv_branch_name
          IMPORTING
            eo_client = lo_client ).

        " Check server capabilities for 'filter' (BEFORE set_headers to
        " preserve the response) - identical pattern to fetch_tip_commits.
        lv_ref_data = lo_client->get_cdata( ).
        lv_null = zcl_abapgit_git_utils=>get_null( ).
        FIND FIRST OCCURRENCE OF lv_null IN lv_ref_data MATCH OFFSET lv_null_pos.
        IF sy-subrc = 0.
          lv_offset = lv_null_pos + 1.
          lv_caps = lv_ref_data+lv_offset.
          FIND FIRST OCCURRENCE OF cl_abap_char_utilities=>newline IN lv_caps
            MATCH OFFSET lv_nl_pos.
          IF sy-subrc = 0 AND lv_nl_pos > 0.
            lv_caps = lv_caps(lv_nl_pos).
            lv_has_filter = xsdbool( lv_caps CS 'filter' ).
          ENDIF.
        ENDIF.

        IF lv_has_filter = abap_false.
          RETURN.
        ENDIF.

        lo_client->set_headers( iv_url     = iv_url
                                iv_service = 'upload' ).

        " want <commit> + deepen 1 + filter blob:none: commit + every
        " reachable tree, but never blob content.
        lv_capa = 'side-band-64k no-progress multi_ack filter'.
        lv_line = |want { iv_commit } { lv_capa }{ cl_abap_char_utilities=>newline }|.
        lv_buffer = zcl_abapgit_git_utils=>pkt_string( lv_line )
          && zcl_abapgit_git_utils=>pkt_string( |deepen 1{ cl_abap_char_utilities=>newline }| )
          && zcl_abapgit_git_utils=>pkt_string( |filter blob:none{ cl_abap_char_utilities=>newline }| )
          && '0000'
          && '0009done' && cl_abap_char_utilities=>newline.

        lv_xstring = lo_client->send_receive_close(
          zcl_abapgit_convert=>string_to_xstring_utf8( lv_buffer ) ).

        parse( IMPORTING ev_pack = lv_pack
               CHANGING  cv_data = lv_xstring ).

        IF lv_pack IS INITIAL.
          RETURN.
        ENDIF.

        lt_objects = zcl_abapgit_ortec_pack_dec=>decode_and_persist(
          iv_data     = lv_pack
          iv_repo_key = iv_repo_key ).

        IF lt_objects IS NOT INITIAL.
          rv_applicable = abap_true.
        ENDIF.
      CATCH zcx_abapgit_exception zcx_abapgit_ortec_git.
        " No fast-path benefit available for this commit - caller falls
        " back to its existing full-fetch path. Never a correctness risk:
        " decode_and_persist's own failure handling already leaves no
        " partial state behind.
        CLEAR rv_applicable.
    ENDTRY.

  ENDMETHOD.

METHOD pull_by_branch.

    DATA lt_resumed     TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_expanded    TYPE zif_abapgit_git_definitions=>ty_expanded_tt.
    DATA lv_repo_key    TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.
    DATA li_branches    TYPE REF TO zif_abapgit_git_branch_list.
    DATA lv_remote_sha  TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_req_deepen  TYPE i.
    DATA ls_active_sess TYPE zcl_abapgit_ortec_pack_raw=>ty_session_info.
    " TODO: variable is assigned but never used (ABAP cleaner)
    DATA ls_commit_obj  LIKE LINE OF lt_resumed.
    DATA ls_file        TYPE zif_abapgit_git_definitions=>ty_file.
    DATA ls_state       TYPE zcl_abapgit_ortec_repo_state=>ty_state.
    DATA lo_fp_timer    TYPE REF TO zcl_abapgit_timer.
    DATA lv_fp_duration TYPE string.
    DATA li_progress    TYPE REF TO zif_abapgit_progress.

    FIELD-SYMBOLS <ls_exp>  LIKE LINE OF lt_expanded.
    FIELD-SYMBOLS <ls_blob> LIKE LINE OF rs_result-objects.

    " Check master switch
    IF zcl_abapgit_ortec_git_switch=>is_active_for_repo( iv_url ) = abap_false.
      RETURN.
    ENDIF.

    " Resolve repo key — read-only lookup here (don't create yet)
    lv_repo_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( iv_url ).
    IF lv_repo_key IS INITIAL.
      RETURN. " First pull for this URL — fall through to standard path
    ENDIF.

    " Discover remote branch tip early (needed for resume validation)
    TRY.
        li_branches = zcl_abapgit_git_transport=>branches( iv_url ).
        lv_remote_sha = li_branches->find_by_name( iv_branch_name )-sha1.
      CATCH zcx_abapgit_exception.
        RETURN.
    ENDTRY.

    " Phase 1: Resume only if active decode session matches branch+deepen request
    lv_req_deepen = iv_deepen_level.
    IF lv_req_deepen IS INITIAL.
      lv_req_deepen = 1.
    ENDIF.

    ls_active_sess = zcl_abapgit_ortec_pack_raw=>find_active_session( lv_repo_key ).

    IF ls_active_sess-session_id IS NOT INITIAL.
      IF    ls_active_sess-branch_name  = iv_branch_name
        AND ls_active_sess-deepen_level = lv_req_deepen.
        TRY.
            lt_resumed = zcl_abapgit_ortec_pack_dec=>resume_decode( lv_repo_key ).
          CATCH zcx_abapgit_exception.
            " Resume failed - continue normally
        ENDTRY.
      ELSE.
        " Partial decode belongs to a different request context: cleanup.
        zcl_abapgit_ortec_pack_raw=>cleanup_partial_session(
          is_session = ls_active_sess
          iv_reason  = |Cleanup: resume skipped (context mismatch)| ).
      ENDIF.
    ENDIF.

    " Phase 1b: If decode was resumed and completed, check if it matches remote tip
    IF lt_resumed IS NOT INITIAL.
      READ TABLE lt_resumed INTO ls_commit_obj
           WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-commit
                                    sha1 = lv_remote_sha.
      IF sy-subrc = 0.
        " Resumed objects contain the target commit! Use them immediately.
        rs_result-objects = lt_resumed.
        rs_result-commit  = lv_remote_sha.

        lo_fp_timer = zcl_abapgit_timer=>create( )->start( ).
        TRY.
            lt_expanded = zcl_abapgit_git_porcelain=>full_tree(
                              it_objects = rs_result-objects
                              iv_parent  = rs_result-commit ).

            LOOP AT lt_expanded ASSIGNING <ls_exp>
                 WHERE chmod = zif_abapgit_git_definitions=>c_chmod-file.
              READ TABLE rs_result-objects ASSIGNING <ls_blob>
                   WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-blob
                                            sha1 = <ls_exp>-sha1.
              IF sy-subrc = 0.
                CLEAR ls_file.
                ls_file-path     = <ls_exp>-path.
                ls_file-filename = <ls_exp>-name.
                ls_file-data     = <ls_blob>-data.
                ls_file-sha1     = <ls_exp>-sha1.
                APPEND ls_file TO rs_result-files.
              ENDIF.
            ENDLOOP.

            " Update repo state to mark successful resume
            TRY.
                zcl_abapgit_ortec_fastpath=>persist_pull_result(
                    iv_url         = iv_url
                    iv_branch_name = iv_branch_name
                    iv_commit      = rs_result-commit
                    it_objects     = rs_result-objects
                    iv_repo_key    = lv_repo_key ).
              CATCH zcx_abapgit_ortec_git.
                " State update non-critical
            ENDTRY.

            lv_fp_duration = lo_fp_timer->end( ).
            li_progress = zcl_abapgit_progress=>get_instance( 1 ).
            li_progress->show(
              iv_current = 1
              iv_text    = |Fastpath: { lines( rs_result-objects ) } git objects (resumed), { lv_fp_duration }| ).
            RETURN. " Success! Avoid redundant GET from remote.
          CATCH zcx_abapgit_exception.
            CLEAR rs_result.
            RETURN.
        ENDTRY.
      ENDIF.
    ENDIF.

    " Phase 2: Check if remote tip matches stored state
    ls_state = zcl_abapgit_ortec_repo_state=>get_state(
                   iv_repo_key    = lv_repo_key
                   iv_branch_name = iv_branch_name ).
    IF ls_state-fetch_commit IS INITIAL.
      RETURN. " No previous fetch -> standard path
    ENDIF.

    IF lv_remote_sha = ls_state-fetch_commit.
      " Remote unchanged -> reconstitute from stored objects (Phase 3)
      lo_fp_timer = zcl_abapgit_timer=>create( )->start( ).
      rs_result-commit  = ls_state-fetch_commit.

      " Phase 4: Walk tree to produce files
      TRY.
          rs_result-objects = zcl_abapgit_ortec_obj_store=>get_reachable_objects(
            iv_repo_key = lv_repo_key
            iv_commit   = rs_result-commit ).

          IF rs_result-objects IS INITIAL.
            CLEAR rs_result.
            RETURN.
          ENDIF.

          lt_expanded = zcl_abapgit_git_porcelain=>full_tree(
                            it_objects = rs_result-objects
                            iv_parent  = rs_result-commit ).

          LOOP AT lt_expanded ASSIGNING <ls_exp>
               WHERE chmod = zif_abapgit_git_definitions=>c_chmod-file.
            READ TABLE rs_result-objects ASSIGNING <ls_blob>
                 WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-blob
                                          sha1 = <ls_exp>-sha1.
            IF sy-subrc = 0.
              CLEAR ls_file.
              ls_file-path     = <ls_exp>-path.
              ls_file-filename = <ls_exp>-name.
              ls_file-data     = <ls_blob>-data.
              ls_file-sha1     = <ls_exp>-sha1.
              APPEND ls_file TO rs_result-files.
            ENDIF.
          ENDLOOP.

        CATCH zcx_abapgit_ortec_git zcx_abapgit_exception.
          " Object store is tree-incomplete for this commit.
          " De-register only this tip as fully materialised so it is not
          " advertised as an empty-pack-safe have. Other complete commits
          " and all stored objects remain available as delta bases for repair.
          zcl_abapgit_ortec_repo_state=>invalidate_tip_commit(
            iv_repo_key    = lv_repo_key
            iv_branch_name = iv_branch_name
            iv_commit      = ls_state-fetch_commit ).
          CLEAR rs_result.
          RETURN.
      ENDTRY.

      lv_fp_duration = lo_fp_timer->end( ).
      li_progress = zcl_abapgit_progress=>get_instance( 1 ).
      li_progress->show(
        iv_current = 1
        iv_text    = |Fastpath: { lines( rs_result-objects ) } git objects, { lv_fp_duration }| ).
      RETURN.
    ENDIF.

    " Phase 2b: Remote changed (force-push/rebase or normal new commit).
    " Return INITIAL so caller does an HTTP fetch — but do NOT update state here.
    " The state (fetch_commit) gets updated post-fetch via persist_pull_result()
    " once the new objects are actually stored. Updating it eagerly here would
    " create a DB-state vs. obj_store inconsistency that breaks the second
    " pull_by_branch call (re-entered through upload_pack_by_branch) which
    " would attempt Phase 3 reconstitution against a commit not in cache.
    " Have-negotiation in upload_pack reads commits from zaog_obj_store
    " (not from zaog_repo_state), so it works correctly without the eager update.

  ENDMETHOD.


  METHOD upload_pack_by_branch.

    DATA ls_pull TYPE zcl_abapgit_git_porcelain=>ty_pull_result.
    DATA lt_hashes TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lo_client TYPE REF TO zcl_abapgit_http_client.
    FIELD-SYMBOLS <ls_branch> LIKE LINE OF it_branches.


    CLEAR: et_objects,
           ev_branch.

    data(li_progress) = zcl_abapgit_progress=>get_instance( 1 ).

    li_progress->show( iv_current = 2
                       iv_text    = 'Fetch remote files (Fastpath)' ).

    ls_pull = pull_by_branch(
      iv_url          = iv_url
      iv_branch_name  = iv_branch_name
      iv_deepen_level = iv_deepen_level ).
    IF ls_pull IS NOT INITIAL.
      et_objects = ls_pull-objects.
      ev_branch  = ls_pull-commit.
      RETURN.
    ENDIF.

    IF it_branches IS INITIAL.
      APPEND iv_branch_name TO lt_hashes.
    ELSE.
      LOOP AT it_branches ASSIGNING <ls_branch>.
        APPEND <ls_branch>-sha1 TO lt_hashes.
      ENDLOOP.
    ENDIF.

    zcl_abapgit_git_transport=>find_branch_ortec(
      EXPORTING
        iv_url         = iv_url
        iv_service     = 'upload'
        iv_branch_name = iv_branch_name
      IMPORTING
        eo_client      = lo_client
        ev_branch      = ev_branch ).

    IF it_branches IS INITIAL.
      CLEAR lt_hashes.
      APPEND ev_branch TO lt_hashes.
    ENDIF.

    TRY.
        et_objects = upload_pack(
          io_client       = lo_client
          iv_url          = iv_url
          iv_deepen_level = iv_deepen_level
          it_hashes       = lt_hashes
          iv_allow_thin   = abap_true ).
      CATCH zcx_abapgit_ortec_git zcx_abapgit_exception INTO DATA(lx_thin_branch).
        " Thin/ofs attempt failed - retry once, non-thin, with a fresh
        " client (the failed attempt's request/response is already
        " consumed). See target_design_phase5.md §6 fail-safe cascade.
        " Both exception types are caught: a decode/resolve failure inside
        " the Ortec pack decoder raises zcx_abapgit_ortec_git, but a
        " fallback to the standard decoder (which cannot understand an
        " ofs-delta/thin pack) raises zcx_abapgit_exception instead.
        zcl_abapgit_git_transport=>find_branch_ortec(
          EXPORTING
            iv_url         = iv_url
            iv_service     = 'upload'
            iv_branch_name = iv_branch_name
          IMPORTING
            eo_client      = lo_client
            ev_branch      = ev_branch ).
        IF it_branches IS INITIAL.
          CLEAR lt_hashes.
          APPEND ev_branch TO lt_hashes.
        ENDIF.
        TRY.
            et_objects = upload_pack(
              io_client       = lo_client
              iv_url          = iv_url
              iv_deepen_level = iv_deepen_level
              it_hashes       = lt_hashes
              iv_allow_thin   = abap_false ).
          CATCH zcx_abapgit_ortec_git zcx_abapgit_exception INTO DATA(lx_nonthin_branch).
            " Non-thin Ortec retry also failed - convert to
            " zcx_abapgit_ortec_git (this method's own declared type) so
            " the existing standard CATCH zcx_abapgit_ortec_git in
            " zcl_abapgit_git_transport falls through to the fully
            " standard fetch path.
            zcx_abapgit_ortec_git=>raise(
              |Ortec fastpath failed after thin+non-thin retry - thin: { lx_thin_branch->get_text( ) }, | &&
              |non-thin: { lx_nonthin_branch->get_text( ) }| ).
        ENDTRY.
    ENDTRY.

  ENDMETHOD.


  METHOD upload_pack_by_commit.

    DATA lt_hashes TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_headers TYPE zcl_abapgit_http=>ty_headers.
    DATA ls_header  LIKE LINE OF lt_headers.
    DATA lo_client TYPE REF TO zcl_abapgit_http_client.


    CLEAR: et_objects,
           ev_commit.

    APPEND iv_hash TO lt_hashes.
    ev_commit = iv_hash.

    ls_header-key   = '~request_uri'.
    ls_header-value = zcl_abapgit_url=>path_name( iv_url ) && |/info/refs?service=git-upload-pack|.
    APPEND ls_header TO lt_headers.

    lo_client = zcl_abapgit_http=>create_by_url(
      iv_url     = iv_url
      it_headers = lt_headers ).

    TRY.
        et_objects = upload_pack(
          io_client       = lo_client
          iv_url          = iv_url
          iv_deepen_level = iv_deepen_level
          it_hashes       = lt_hashes
          iv_allow_thin   = abap_true ).
      CATCH zcx_abapgit_ortec_git zcx_abapgit_exception INTO DATA(lx_thin_commit).
        " Thin/ofs attempt failed - retry once, non-thin, with a fresh
        " client (the failed attempt's request/response is already
        " consumed). See target_design_phase5.md §6 fail-safe cascade.
        " Both exception types are caught - see the matching comment in
        " upload_pack_by_branch for why zcx_abapgit_exception can also occur.
        lo_client = zcl_abapgit_http=>create_by_url(
          iv_url     = iv_url
          it_headers = lt_headers ).
        TRY.
            et_objects = upload_pack(
              io_client       = lo_client
              iv_url          = iv_url
              iv_deepen_level = iv_deepen_level
              it_hashes       = lt_hashes
              iv_allow_thin   = abap_false ).
          CATCH zcx_abapgit_ortec_git zcx_abapgit_exception INTO DATA(lx_nonthin_commit).
            zcx_abapgit_ortec_git=>raise(
              |Ortec fastpath failed after thin+non-thin retry - thin: { lx_thin_commit->get_text( ) }, | &&
              |non-thin: { lx_nonthin_commit->get_text( ) }| ).
        ENDTRY.
    ENDTRY.

  ENDMETHOD.


METHOD upload_pack.

    DATA lv_capa    TYPE string.
    DATA lv_line    TYPE string.
    DATA lv_buffer  TYPE string.
    DATA lv_xstring TYPE xstring.
    DATA lv_pack    TYPE xstring.
    DATA lt_ortec_haves TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv_advertise_thin TYPE abap_bool.

    DATA lo_fetch_timer   TYPE REF TO zcl_abapgit_timer.
    DATA lv_fetch_duration TYPE string.
    DATA li_progress      TYPE REF TO zif_abapgit_progress.

    FIELD-SYMBOLS <lv_hash> LIKE LINE OF it_hashes.
    FIELD-SYMBOLS <lv_ortec_have> LIKE LINE OF lt_ortec_haves.


    io_client->set_headers(
      iv_url     = iv_url
      iv_service = 'upload' ).

    " Resolve have commits BEFORE assembling the want/capability line, so the
    " capability string can correctly reflect whether thin-pack/ofs-delta are
    " safe to advertise. When thin is requested, only VERIFIED-COMPLETE haves
    " (index-ready, every reachable object present, no dangling delta base -
    " see zcl_abapgit_ortec_fetch_neg=>get_verified_have_commits) are used;
    " advertising thin-pack against an unverified have would let the server
    " send back an unresolvable delta. When thin is not requested, the
    " existing unfiltered have-set is used exactly as before.
    TRY.
        IF iv_allow_thin = abap_true.
          lt_ortec_haves = zcl_abapgit_ortec_fetch_neg=>get_verified_have_commits(
            iv_url         = iv_url
            it_want_hashes = it_hashes ).
        ELSE.
          lt_ortec_haves = zcl_abapgit_ortec_fetch_neg=>get_have_commits(
            iv_url         = iv_url
            it_want_hashes = it_hashes ).
        ENDIF.
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.

    " Only advertise thin-pack/ofs-delta when the caller allowed it AND at
    " least one verified-complete have exists to delta against - otherwise
    " thin capability would be pointless (nothing to delta against) or, if
    " haves existed but were unverified, unsafe.
    lv_advertise_thin = xsdbool( iv_allow_thin = abap_true AND lt_ortec_haves IS NOT INITIAL ).

    LOOP AT it_hashes FROM 1 ASSIGNING <lv_hash>.
      IF sy-tabix = 1.
        IF lv_advertise_thin = abap_true.
          lv_capa = 'side-band-64k no-progress multi_ack thin-pack ofs-delta'.
        ELSE.
          lv_capa = 'side-band-64k no-progress multi_ack'.
        ENDIF.
        lv_line = 'want' && ` ` && <lv_hash>
          && ` ` && lv_capa && cl_abap_char_utilities=>newline.
      ELSE.
        lv_line = 'want' && ` ` && <lv_hash>
          && cl_abap_char_utilities=>newline.
      ENDIF.
      lv_buffer = lv_buffer && zcl_abapgit_git_utils=>pkt_string( lv_line ).
    ENDLOOP.

    " Only send deepen when we have NO cached objects (first fetch).
    " With deepen, the server ignores have lines and sends a full shallow pack.
    " Without deepen (but with haves), the server sends only the delta.
    IF lt_ortec_haves IS INITIAL AND iv_deepen_level > 0.
      lv_buffer = lv_buffer && zcl_abapgit_git_utils=>pkt_string( |deepen { iv_deepen_level }| &&
        cl_abap_char_utilities=>newline ).
    ENDIF.

    lv_buffer = lv_buffer && '0000'.

    LOOP AT lt_ortec_haves ASSIGNING <lv_ortec_have>.
      lv_buffer = lv_buffer && zcl_abapgit_git_utils=>pkt_string(
        |have { <lv_ortec_have> }{ cl_abap_char_utilities=>newline }| ).
    ENDLOOP.

    lv_buffer = lv_buffer && '0009done' && cl_abap_char_utilities=>newline.

    lo_fetch_timer = zcl_abapgit_timer=>create( )->start( ).
    lv_xstring = io_client->send_receive_close( zcl_abapgit_convert=>string_to_xstring_utf8( lv_buffer ) ).

    parse( IMPORTING ev_pack = lv_pack
           CHANGING  cv_data = lv_xstring ).

    " A completely empty response (no pack section at all) OR a well-formed
    " pack that decodes to zero objects both carry the exact same meaning:
    " the server considers our have-set sufficient and has nothing new to
    " send. Peek the declared object count cheaply (12-byte header, no
    " decompression) before ever attempting a full decode - real DevOps
    " servers appear to prefer framing "nothing new" as a valid zero-object
    " pack rather than omitting the pack section entirely, which is why
    " this case only started firing regularly once today's have-negotiation
    " fix began offering the server real, verified haves to negotiate
    " against (previously have-negotiation rarely had anything to offer, so
    " the server rarely had reason to reply this way).
    IF lv_pack IS INITIAL OR zcl_abapgit_ortec_pack_dec=>peek_object_count( lv_pack ) = 0.
      rt_objects = serve_cached_when_nothing_new(
        iv_url    = iv_url
        it_hashes = it_hashes ).
      lv_fetch_duration = lo_fetch_timer->end( ).
      li_progress = zcl_abapgit_progress=>get_instance( 1 ).
      li_progress->show(
        iv_current = 1
        iv_text    = |Fastpath: { lines( rt_objects ) } git objects (cached, nothing new), { lv_fetch_duration }| ).
      RETURN.
    ENDIF.

    TRY.
        IF zcl_abapgit_ortec_git_switch=>is_active_for_repo( iv_url ) = abap_true.
          DATA lv_ortec_rk TYPE zcl_abapgit_ortec_pack_dec=>ty_repo_key.
          lv_ortec_rk = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( iv_url ).
          IF lv_ortec_rk IS NOT INITIAL.
            " decode_and_persist issues a targeted bulk SELECT for delta bases
            " (only SHA1s referenced as OBJ_REF_DELTA in this pack).
            " Full packs trigger no SELECT; the fallback full-store SELECT fires
            " only when a delta base is missing (edge case).
            rt_objects = zcl_abapgit_ortec_pack_dec=>decode_and_persist(
              iv_data     = lv_pack
              iv_repo_key = lv_ortec_rk ).
            IF rt_objects IS NOT INITIAL.
              " rt_objects = full merged set (base + new objects).
              " decode_and_persist already called invalidate_cache() internally.
              lv_fetch_duration = lo_fetch_timer->end( ).
              li_progress = zcl_abapgit_progress=>get_instance( 1 ).
              li_progress->show(
                iv_current = 1
                iv_text    = |Fetch: { lines( rt_objects ) } git objects, { lv_fetch_duration }| ).
              RETURN.
            ENDIF.
          ENDIF.
        ENDIF.
      CATCH zcx_abapgit_exception INTO DATA(lx_decode).
        " Do not fall back to standard decode on THESE SAME bytes here: a
        " thin/deepen fetch can legitimately contain OBJ_OFS_DELTA entries,
        " which the standard decoder cannot parse at all (it has no OFS
        " support), and even a genuinely self-contained pack that failed
        " here for some other reason should be re-fetched fresh rather than
        " re-parsed by a decoder never built/tested for Ortec-scale packs.
        " Feeding such bytes to zcl_abapgit_git_pack=>decode's
        " cl_abap_gzip=>decompress_binary call can desynchronize its
        " position tracking and crash the work process with SYSTEM_NO_ROLL
        " (kernel attempting an unbounded allocation on corrupt/misaligned
        " input) rather than failing cleanly. Re-raise instead and let the
        " caller's existing thin -> non-thin -> standard-via-transport-catch
        " cascade handle escalation, which always re-negotiates a fresh,
        " capability-appropriate pack rather than reusing these bytes.
        zcx_abapgit_ortec_git=>raise( |Ortec decode failed: { lx_decode->get_text( ) }| ).
    ENDTRY.

    " decode_and_persist is only skipped when Ortec is inactive for this repo
    " (is_active_for_repo = false) or no repo key could be resolved; both are
    " legitimate reasons this method may be reached with fastpath having
    " done nothing. Any actual decode failure above already re-raised.
    zcx_abapgit_ortec_git=>raise( 'Ortec decode not applicable for this repo' ).

  ENDMETHOD.


  METHOD serve_cached_when_nothing_new.

    DATA lv_repo_key TYPE zcl_abapgit_ortec_repo_state=>ty_repo_key.
    DATA lt_cached TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_reachable TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_cached_shas TYPE HASHED TABLE OF zif_abapgit_git_definitions=>ty_sha1
               WITH UNIQUE KEY table_line.

    FIELD-SYMBOLS <lv_hash> LIKE LINE OF it_hashes.
    FIELD-SYMBOLS <ls_reachable> LIKE LINE OF lt_reachable.

    " Return cached objects, but ONLY if the want-commit is actually present.
    " Otherwise the caller's tree-walk would fail on a missing commit object
    " and we'd silently return broken data.
    lv_repo_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( iv_url ).
    IF lv_repo_key IS NOT INITIAL.
      TRY.
          IF lines( it_hashes ) > 1.
            zcx_abapgit_ortec_git=>raise( 'Cached multi-want nothing-new response requires standard fetch' ).
          ENDIF.

          LOOP AT it_hashes ASSIGNING <lv_hash>.
            TRY.
                lt_reachable = zcl_abapgit_ortec_obj_store=>get_reachable_objects(
                  iv_repo_key = lv_repo_key
                  iv_commit   = <lv_hash> ).
                zcl_abapgit_git_porcelain=>full_tree(
                  it_objects = lt_reachable
                  iv_parent  = <lv_hash> ).
              CATCH zcx_abapgit_ortec_git zcx_abapgit_exception.
                " Cache is tree-incomplete. De-register this commit as
                " fully materialised so it is no longer advertised as an
                " empty-pack-safe have; keep other haves as delta bases.
                zcl_abapgit_ortec_repo_state=>invalidate_tip_commit(
                  iv_repo_key = lv_repo_key
                  iv_commit   = <lv_hash> ).
                zcx_abapgit_ortec_git=>raise(
                  'Cached objects have incomplete tree - falling back to standard fetch' ).
            ENDTRY.

            LOOP AT lt_reachable ASSIGNING <ls_reachable>.
              READ TABLE lt_cached_shas WITH TABLE KEY table_line = <ls_reachable>-sha1 TRANSPORTING NO FIELDS.
              IF sy-subrc <> 0.
                INSERT <ls_reachable>-sha1 INTO TABLE lt_cached_shas.
                APPEND <ls_reachable> TO lt_cached.
              ENDIF.
            ENDLOOP.
          ENDLOOP.

          IF lt_cached IS NOT INITIAL.
            rt_objects = lt_cached.
            RETURN.
          ENDIF.
        CATCH zcx_abapgit_ortec_git.
      ENDTRY.
    ENDIF.

    " Cached objects are unavailable or unsafe; caller must use the standard fetch path.
    zcx_abapgit_ortec_git=>raise(
      'Cached objects not available for nothing-new response - falling back to standard fetch' ).

  ENDMETHOD.


  METHOD parse.

    CONSTANTS lc_band1 TYPE x VALUE '01'.

    DATA lv_len      TYPE i.
    DATA lv_contents TYPE xstring.
    DATA lv_pack     TYPE xstring.


    TRY.
        WHILE xstrlen( cv_data ) >= 4.
          lv_len = zcl_abapgit_git_utils=>length_utf8_hex( cv_data ).

          IF lv_len > xstrlen( cv_data ).
            zcx_abapgit_ortec_git=>raise( 'parse, string length too large' ).
          ENDIF.

          IF lv_len = 0.
            cv_data = cv_data+4.
            CONTINUE.
          ENDIF.

          IF lv_len < 4.
            " A non-flush pkt-line always includes its own 4-byte length
            " header, so any length 1-3 is an invalid/malformed frame -
            " without this check, the +4 strip below could slice past the
            " end of a too-short lv_contents.
            zcx_abapgit_ortec_git=>raise( 'parse, invalid pkt-line length' ).
          ENDIF.

          lv_contents = cv_data(lv_len).
          cv_data = cv_data+lv_len.
          lv_contents = lv_contents+4.

          IF xstrlen( lv_contents ) > 1 AND lv_contents(1) = lc_band1.
            CONCATENATE lv_pack lv_contents+1 INTO lv_pack IN BYTE MODE.
          ENDIF.
        ENDWHILE.
      CATCH cx_sy_range_out_of_bounds INTO DATA(lx_range).
        zcx_abapgit_ortec_git=>raise( |parse, pkt-line framing error: { lx_range->get_text( ) }| ).
    ENDTRY.

    ev_pack = lv_pack.

  ENDMETHOD.

  METHOD persist_pull_result.
    DATA lv_repo_key       TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.
    DATA lv_ts             TYPE timestampl.
    DATA ls_row            TYPE zaog_obj_store.
    DATA lt_new            TYPE STANDARD TABLE OF zaog_obj_store.
    DATA lt_existing_shas  TYPE HASHED TABLE OF zif_abapgit_git_definitions=>ty_sha1
                            WITH UNIQUE KEY table_line.

    IF zcl_abapgit_ortec_git_switch=>is_active_for_repo( iv_url ) = abap_false.
      RETURN.
    ENDIF.
    IF iv_repo_key IS NOT INITIAL.
      lv_repo_key = iv_repo_key.
    ELSE.
      lv_repo_key = resolve_repo_key( iv_url ).
    ENDIF.
    IF lv_repo_key IS INITIAL.
      RETURN.
    ENDIF.

    GET TIME STAMP FIELD lv_ts.
    SELECT obj_sha1 FROM zaog_obj_store INTO TABLE lt_existing_shas
      WHERE repo_key = lv_repo_key
        AND status   = 'R'.

    FIELD-SYMBOLS <ls_obj> LIKE LINE OF it_objects.
    LOOP AT it_objects ASSIGNING <ls_obj>.
      READ TABLE lt_existing_shas WITH TABLE KEY table_line = <ls_obj>-sha1 TRANSPORTING NO FIELDS.
      IF sy-subrc <> 0.
        CLEAR ls_row.
        ls_row-repo_key   = lv_repo_key.
        ls_row-obj_sha1   = <ls_obj>-sha1.
        ls_row-obj_type   = <ls_obj>-type.
        ls_row-obj_data   = <ls_obj>-data.
        ls_row-obj_size   = xstrlen( <ls_obj>-data ).
        ls_row-created_at = lv_ts.
        ls_row-status     = 'R'.
        APPEND ls_row TO lt_new.
      ENDIF.
    ENDLOOP.
    IF lt_new IS NOT INITIAL.
      MODIFY zaog_obj_store FROM TABLE lt_new.
      " Invalidate in-memory session cache: MODIFY wrote directly to DB
      " (bypassing store_object/store_objects which call invalidate_cache).
      zcl_abapgit_ortec_obj_store=>invalidate_cache( ).
    ENDIF.
    zcl_abapgit_ortec_repo_state=>update_after_fetch(
        iv_repo_key    = lv_repo_key
        iv_branch_name = iv_branch_name
        iv_url         = iv_url
        iv_commit      = iv_commit ).
    " Record completed fetch in commit history (for multi-branch have negotiation)
    DATA ls_hist TYPE zaog_commit_hist.
    ls_hist-repo_key    = lv_repo_key.
    ls_hist-commit_sha1 = iv_commit.
    ls_hist-branch_name = iv_branch_name.
    ls_hist-fetched_at  = lv_ts.
    INSERT zaog_commit_hist FROM ls_hist. "#EC SUBRC_OK - duplicate key = already recorded
    COMMIT WORK.
  ENDMETHOD.

  METHOD resolve_repo_key.
    rv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( iv_url ).
  ENDMETHOD.
ENDCLASS.
