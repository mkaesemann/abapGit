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
        ev_deepen_used  TYPE i
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
        ev_deepen_used  TYPE i
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
    "! @parameter iv_deepen_used |
    "! The deepen level that actually succeeded for this fetch (see
    "! upload_pack_by_branch/upload_pack_by_commit's ev_deepen_used) -
    "! persisted as the next fetch's starting baseline via
    "! zcl_abapgit_ortec_repo_state=>update_after_fetch. Defaults to 1 for
    "! callers that don't track this (matches the pre-existing behavior).
    "! @raising zcx_abapgit_ortec_git |
    "! On error
    CLASS-METHODS persist_pull_result
      IMPORTING iv_url         TYPE string
                iv_branch_name TYPE string
                iv_commit      TYPE zif_abapgit_git_definitions=>ty_sha1
                it_objects     TYPE zif_abapgit_definitions=>ty_objects_tt
                iv_repo_key    TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key OPTIONAL
                iv_deepen_used TYPE i DEFAULT 1
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS persist_missing_objects
      IMPORTING iv_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                it_objects  TYPE zif_abapgit_definitions=>ty_objects_tt
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

    "! Thin-pack completion: fetches exactly ONE object (commit, tree, OR
    "! blob - not necessarily a commit) by SHA1 via a MATERIALIZE_BLOBS
    "! minimal want-only request (no haves, no shallow, no deepen) and
    "! persists it (and whatever the server bundles to resolve its own
    "! delta chain) into zaog_obj_store via the normal streaming
    "! decode/resolve pipeline.
    "! DORMANT as of Slice 2C's correctness correction (finding F-2C-001):
    "! its only caller, zcl_abapgit_ortec_pack_stream=>complete_missing_base,
    "! is now a permanent no-op (one-request-per-missing-base HTTP repair is
    "! not Variant-B-compliant), so this method is currently unreachable
    "! from any live path. The serializer/decode logic below remains
    "! correct and is expected to be reused by Slice 7's bulk
    "! collect/deduplicate/MATERIALIZE_BLOBS external-base resolution -
    "! kept in place for that purpose, not deleted.
    "! Historical doc (still accurate for what this method itself does, just
    "! not currently invoked): called by
    "! zcl_abapgit_ortec_pack_stream=>complete_missing_base when a larger
    "! pack's own delta resolution finds a base referenced but not
    "! included - see that method's doc and the "Architecture hardening
    "! plan" Phase-1-followup incident (.memory/state.md, 2026-07-20): even
    "! a non-thin, deeply-widened fetch is not always guaranteed
    "! self-contained against a large, real repository's history (GitHub's
    "! shallow pack generation can reference a stable, rarely-touched
    "! historical object as a delta base without including it, regardless
    "! of the requested depth) - fetching exactly the missing piece is the
    "! standard git-client remedy ("thin pack completion"/"fix-thin"),
    "! rather than requesting ever more history.
    "! Relies on the server supporting "want <any-reachable-sha1>", not just
    "! ref tips (GitHub and most modern smart-HTTP servers advertise
    "! `allow-reachable-sha1-in-want`/`allow-tip-sha1-in-want` for exactly
    "! this use case).
    "! @raising zcx_abapgit_ortec_git |
    "! On any network/protocol/decode failure - callers must treat this as
    "! "completion not possible right now", not retry indefinitely
    "! themselves (zcl_abapgit_ortec_pack_stream=>complete_missing_base
    "! already bounds its own retry budget).
    CLASS-METHODS complete_missing_object
      IMPORTING iv_url      TYPE string
                iv_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                iv_sha1     TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING   zcx_abapgit_ortec_git.

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

    "! Build the pkt-line request buffer for a git upload-pack request
    "! (want/shallow/deepen/flush/have/done). Public so unit tests can
    "! verify the wire-line shape and ordering directly without a live
    "! HTTP client - see zcl_abapgit_ortec_git_tests.clas.testclasses.abap
    "! ltcl_fastpath_protocol.
    "! @raising zcx_abapgit_exception |
    "! Propagated unchanged from ZCL_ABAPGIT_GIT_UTILS=>PKT_STRING (a pure
    "! pkt-line length-encoding failure) - this method is a wire-format
    "! formatter, not a network/protocol boundary, so it intentionally does
    "! NOT translate into ZCX_ABAPGIT_ORTEC_GIT (matches the existing
    "! legacy convention already used by PKT_STRING/LENGTH_UTF8_HEX
    "! themselves). Legacy/unreachable-from-migrated-paths as of Slice 2C
    "! (LEGACY_BUT_UNREACHABLE_AFTER_2C) - only called by
    "! ltcl_fastpath_protocol today, which already declares
    "! FOR TESTING RAISING cx_static_check (zcx_abapgit_exception's
    "! superclass), so this addition requires no test call-site changes.
    CLASS-METHODS build_upload_pack_buffer
      IMPORTING
        iv_deepen_level TYPE i DEFAULT 0
        it_hashes       TYPE zif_abapgit_git_definitions=>ty_sha1_tt
        it_ortec_haves  TYPE zif_abapgit_git_definitions=>ty_sha1_tt OPTIONAL
        iv_allow_thin   TYPE abap_bool DEFAULT abap_false
        iv_force_full   TYPE abap_bool DEFAULT abap_false
      RETURNING
        VALUE(rv_buffer) TYPE string
      RAISING
        zcx_abapgit_exception.

    "! Parse a pkt-line response stream: extracts side-band channel 1
    "! (packfile) bytes into ev_pack, and best-effort collects any plain
    "! (non-side-band) "shallow"/"unshallow" response lines the server may
    "! send before the packfile when the request included shallow/deepen
    "! lines. Public so unit tests can verify pkt-line parsing directly
    "! without a live HTTP client - see
    "! zcl_abapgit_ortec_git_tests.clas.testclasses.abap ltcl_fastpath_protocol.
    CLASS-METHODS parse
      EXPORTING
        ev_pack       TYPE xstring
        et_shallow    TYPE zif_abapgit_git_definitions=>ty_sha1_tt
        et_unshallow  TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      CHANGING
        cv_data TYPE xstring
      RAISING
        zcx_abapgit_ortec_git
        zcx_abapgit_exception.

    "! Progressive-deepening recovery tier (replaces the old one-shot
    "! "force_full = unbounded history" attempt - see Phase 1 of the
    "! architecture hardening plan in .memory/state.md, 2026-07-20). A
    "! genuinely unbounded fetch is impractical for a repo with substantial
    "! real history (confirmed live: 4737 commits for abapGit's own repo) -
    "! instead this starts at a moderate depth and widens it on each
    "! failure, stopping as soon as one attempt succeeds, bounded by
    "! c_progressive_max_steps/c_progressive_max_deepen so a genuinely
    "! unrecoverable case still fails in bounded time/DB cost rather than
    "! spinning or requesting an ever-larger pack forever. Public so unit
    "! tests can verify the widening formula directly - see
    "! zcl_abapgit_ortec_git_tests.clas.testclasses.abap ltcl_fastpath_protocol.
    CONSTANTS c_progressive_start_min    TYPE i VALUE 50.
    CONSTANTS c_progressive_widen_factor TYPE i VALUE 4.
    CONSTANTS c_progressive_max_deepen   TYPE i VALUE 2000.
    CONSTANTS c_progressive_max_steps    TYPE i VALUE 5.

    "! First deepen level to try in the progressive recovery loop, given the
    "! depth that was in use before recovery was needed.
    CLASS-METHODS first_progressive_deepen
      IMPORTING iv_prior_deepen  TYPE i
      RETURNING VALUE(rv_deepen) TYPE i.

    "! Next deepen level to try after a progressive recovery attempt at
    "! iv_current failed. Widens by c_progressive_widen_factor, capped at
    "! c_progressive_max_deepen.
    CLASS-METHODS next_progressive_deepen
      IMPORTING iv_current       TYPE i
      RETURNING VALUE(rv_deepen) TYPE i.

  PRIVATE SECTION.
    "! Resolve repo key from URL. Creates new key if none found.
    "! @parameter iv_url |
    "! Remote URL
    "! @parameter rv_key |
    "! Repository key (always non-empty)
    CLASS-METHODS resolve_repo_key
      IMPORTING iv_url        TYPE string
      RETURNING VALUE(rv_key) TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.

    "! Package C C2: certification lifecycle extracted from PERSIST_PULL_RESULT
    "! so it is directly unit-testable without the URL-keyed
    "! IS_ACTIVE_FOR_REPO switch (backed by a shared, singleton, XML-serialized
    "! user settings persistence object with its own uncontrolled COMMIT WORK
    "! AND WAIT - unsafe/impractical to flip from a unit test; see
    "! zcl_abapgit_persistence_ortec=>set_repo_use_cache/update_repo_config).
    "! Never called with an unresolved/blank iv_repo_key - PERSIST_PULL_RESULT
    "! already guarantees that before calling this method.
    "! Idempotent-safe: begin_attempt/mark_graph_complete/mark_full_complete
    "! never downgrade an existing, higher certification level.
    "! @parameter iv_repo_key |
    "! Repository key (already resolved and non-initial)
    "! @parameter iv_commit |
    "! Fetched commit SHA1 to certify
    "! @parameter iv_branch_name |
    "! Branch whose materialized pointer is published on full completeness
    "! @raising zcx_abapgit_ortec_git |
    "! On a genuine technical/persistence failure (not on expected local
    "! incompleteness, which is caught internally and simply skips
    "! publication for this round)
    CLASS-METHODS certify_fetched_commit
      IMPORTING iv_repo_key    TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                iv_commit      TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_branch_name TYPE string
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS upload_pack
      IMPORTING
        io_client       TYPE REF TO zcl_abapgit_http_client
        iv_url          TYPE string
        iv_deepen_level TYPE i DEFAULT 0
        it_hashes       TYPE zif_abapgit_git_definitions=>ty_sha1_tt
        iv_mode         TYPE zcl_abapgit_ortec_fetch_req=>ty_fetch_mode
        iv_server_caps  TYPE string OPTIONAL
      RETURNING
        VALUE(rt_objects) TYPE zif_abapgit_definitions=>ty_objects_tt
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

    "! Returns ABAP_TRUE if ix_exception is a zcx_abapgit_ortec_git with
    "! mv_retry_without_haves set - a small helper to avoid duplicating the
    "! INSTANCE OF check at every cascade catch site.
    CLASS-METHODS is_retry_without_haves
      IMPORTING
        ix_exception  TYPE REF TO cx_root
      RETURNING
        VALUE(rv_yes) TYPE abap_bool.

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

  METHOD complete_missing_object.
    DATA lt_headers TYPE zcl_abapgit_http=>ty_headers.
    DATA ls_header  LIKE LINE OF lt_headers.
    DATA lo_client  TYPE REF TO zcl_abapgit_http_client.
    DATA lt_hashes  TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv_ref_data TYPE string.
    DATA lv_server_caps TYPE string.

    APPEND iv_sha1 TO lt_hashes.

    TRY.

        ls_header-key   = '~request_uri'.
        ls_header-value = zcl_abapgit_url=>path_name( iv_url ) && |/info/refs?service=git-upload-pack|.
        APPEND ls_header TO lt_headers.

        " MATERIALIZE_BLOBS with a single-entry want list: no haves, no shallow
        " line - this is a targeted, self-contained fetch for exactly iv_sha1
        " (plus whatever the server needs to bundle to resolve its own delta
        " chain), not a haves-based negotiation, and never a deepen (we are not
        " trying to fetch history, just one object). MATERIALIZE_BLOBS hard-
        " requires an arbitrary-object-want capability
        " (allow-reachable-sha1-in-want/allow-tip-sha1-in-want); if the server
        " does not advertise it, build_request raises zcx_abapgit_ortec_git with
        " mv_unsupported_capability = abap_true, which this method already
        " propagates unchanged (only zcx_abapgit_exception is caught below).
        " upload_pack persists every decoded/resolved object as a side effect
        " (via decode_streaming's underlying decode_and_persist_streaming +
        " resolve_streaming) regardless of iv_sha1's own type or of what
        " decode_streaming's own commit-only return filter discards - the
        " RETURNING value here is deliberately discarded, only the persistence
        " side effect matters to the caller.
        " This method's own declared contract is "RAISING zcx_abapgit_ortec_git"
        " only, so CREATE_BY_URL (which raises zcx_abapgit_exception, e.g. on a
        " network/connection failure) must be inside this same TRY, not just
        " the final upload_pack( ) call - an earlier version left it outside,
        " which let a plain zcx_abapgit_exception escape this method's boundary
        " undeclared (a real ATC finding, not just a style issue: a checked
        " exception can validly propagate without a RAISING declaration in
        " ABAP, but every caller of this ORTEC-internal API expects only
        " ZCX_ABAPGIT_ORTEC_GIT to ever cross it).
        lo_client = zcl_abapgit_http=>create_by_url(
          iv_url     = iv_url
          it_headers = lt_headers ).

        lv_ref_data = lo_client->get_cdata( ).
        lv_server_caps = zcl_abapgit_ortec_fetch_req=>parse_capabilities( lv_ref_data ).

        upload_pack(
          io_client       = lo_client
          iv_url          = iv_url
          it_hashes       = lt_hashes
          iv_mode         = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-materialize_blobs
          iv_server_caps  = lv_server_caps ).
      CATCH zcx_abapgit_exception INTO DATA(lx_std).
        RAISE EXCEPTION TYPE zcx_abapgit_ortec_git
          EXPORTING
            iv_text  = |Thin-pack completion failed: { lx_std->get_text( ) }|
            previous = lx_std.
    ENDTRY.
  ENDMETHOD.


  METHOD try_filtered_commit_fetch.

    DATA lo_client     TYPE REF TO zcl_abapgit_http_client.
    DATA lv_xstring    TYPE xstring.
    DATA lv_pack       TYPE xstring.
    DATA lv_ref_data   TYPE string.
    DATA lv_server_caps TYPE string.
    DATA lt_objects    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_want       TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

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

        " Server capabilities (BEFORE set_headers to preserve the response) -
        " identical pattern to fetch_tip_commits, now via the shared
        " serializer's parser instead of an inline duplicate.
        lv_ref_data = lo_client->get_cdata( ).
        lv_server_caps = zcl_abapgit_ortec_fetch_req=>parse_capabilities( lv_ref_data ).

        lo_client->set_headers( iv_url     = iv_url
                                iv_service = 'upload' ).

        " INITIAL_BRANCH_BLOBLESS: want <commit> + filter blob:none - commit
        " + every reachable tree, but never blob content, never deepen. If
        " the server does not advertise `filter`, build_request raises
        " zcx_abapgit_ortec_git with mv_unsupported_capability = abap_true,
        " which the outer CATCH below still swallows into the same
        " rv_applicable = abap_false result as today's early RETURN on a
        " missing filter capability - end-to-end surfacing of that flag to
        " this method's own caller is deferred to Slice 3 (DR-001
        " resolution, .memory/logs/variant_b_slice2_design.md §3).
        APPEND iv_commit TO lt_want.
        DATA(ls_request) = zcl_abapgit_ortec_fetch_req=>build_request(
          iv_mode        = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-initial_branch_blobless
          it_want_hashes = lt_want
          iv_server_caps = lv_server_caps ).

        lv_xstring = lo_client->send_receive_close(
          zcl_abapgit_convert=>string_to_xstring_utf8( ls_request-buffer ) ).

        parse( IMPORTING ev_pack = lv_pack
               CHANGING  cv_data = lv_xstring ).

        IF lv_pack IS INITIAL.
          RETURN.
        ENDIF.

        " Phase 4 routing: try the streaming decoder first (default for any
        " Ortec-active repo per the design's decision 6); fall back to the
        " proven non-streaming decoder on the SAME pack bytes on failure
        " (DR-001 fallback cascade, tier 2). This caller only checks
        " "did anything decode" (lt_objects IS NOT INITIAL) - it never reads
        " object content - so decode_streaming's sparse (commit-only)
        " result is already exactly what is needed here.
        TRY.
            lt_objects = zcl_abapgit_ortec_pack_stream=>decode_streaming(
              iv_data     = lv_pack
              iv_repo_key = iv_repo_key
              iv_url      = iv_url ).
          CATCH zcx_abapgit_ortec_git.
            lt_objects = zcl_abapgit_ortec_pack_dec=>decode_and_persist(
              iv_data     = lv_pack
              iv_repo_key = iv_repo_key ).
        ENDTRY.

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
                    iv_repo_key    = lv_repo_key
                    iv_deepen_used = lv_req_deepen ).
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
    DATA lv_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.
    DATA ls_repo_state TYPE zcl_abapgit_ortec_repo_state=>ty_state.
    DATA lv_deepen_level TYPE i.
    DATA lv_ref_data TYPE string.
    DATA lv_server_caps TYPE string.
    FIELD-SYMBOLS <ls_branch> LIKE LINE OF it_branches.


    CLEAR: et_objects,
           ev_branch,
           ev_deepen_used.

    " Reset the thin-pack completion budget once for this WHOLE fetch
    " attempt (shared across the thin/self-contained/recovery tiers below) -
    " see zcl_abapgit_ortec_pack_stream=>reset_completion_budget's doc.
    zcl_abapgit_ortec_pack_stream=>reset_completion_budget( ).

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

    " Use the LARGER of the caller-supplied depth and this repo/branch's own
    " last-successful depth (persisted by persist_pull_result via
    " zcl_abapgit_ortec_repo_state=>update_after_fetch) as the starting point
    " for the thin/self-contained tiers - avoids repeatedly starting from a
    " too-shallow depth on a repo that has already proven it needs more
    " (Phase 1, .memory/state.md "Architecture hardening plan", 2026-07-20).
    lv_repo_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( iv_url ).
    lv_deepen_level = iv_deepen_level.
    IF lv_repo_key IS NOT INITIAL.
      ls_repo_state = zcl_abapgit_ortec_repo_state=>get_state(
        iv_repo_key    = lv_repo_key
        iv_branch_name = iv_branch_name ).
      IF ls_repo_state-deepen_lvl > lv_deepen_level.
        lv_deepen_level = ls_repo_state-deepen_lvl.
      ENDIF.
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

    " Server-advertised capabilities are read once per connection (same
    " pattern already used by try_filtered_commit_fetch/fetch_tip_commits) -
    " zcl_abapgit_ortec_fetch_req=>build_request intersects requested
    " capabilities (thin-pack/ofs-delta for INCREMENTAL_THIN, filter for
    " INITIAL_BRANCH_BLOBLESS, etc.) against this advertisement itself; this
    " caller only needs to supply what the server actually said.
    lv_ref_data = lo_client->get_cdata( ).
    lv_server_caps = zcl_abapgit_ortec_fetch_req=>parse_capabilities( lv_ref_data ).

    TRY.
        et_objects = upload_pack(
          io_client       = lo_client
          iv_url          = iv_url
          iv_deepen_level = lv_deepen_level
          it_hashes       = lt_hashes
          iv_mode         = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-incremental_thin
          iv_server_caps  = lv_server_caps ).
        " F-2C-002: the migrated wire modes never emit deepen (iv_deepen_level
        " is not read by upload_pack's body); reporting the caller/persisted
        " depth here would be a false signal implying a real shallow-history
        " depth was negotiated. Always 0 for every migrated mode.
        ev_deepen_used = 0.
      CATCH zcx_abapgit_ortec_git zcx_abapgit_exception INTO DATA(lx_thin_branch).
        " Thin/ofs attempt failed - retry once, self-contained, with a fresh
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
        lv_ref_data = lo_client->get_cdata( ).
        lv_server_caps = zcl_abapgit_ortec_fetch_req=>parse_capabilities( lv_ref_data ).
        TRY.
            et_objects = upload_pack(
              io_client       = lo_client
              iv_url          = iv_url
              iv_deepen_level = lv_deepen_level
              it_hashes       = lt_hashes
              iv_mode         = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-incremental_self_contained
              iv_server_caps  = lv_server_caps ).
            " F-2C-002: see the matching comment in the thin tier above.
            ev_deepen_used = 0.
          CATCH zcx_abapgit_ortec_git zcx_abapgit_exception INTO DATA(lx_nonthin_branch).
            " Self-contained Ortec retry also failed. If either failure
            " indicates a fresh haves-free fetch might succeed (the server
            " claimed "nothing new" but our own cache verification
            " disagreed), make exactly ONE RECOVERY_BRANCH_FULL attempt -
            " no progressive widening, no deepen (see the Variant B owner
            " spec and .memory/logs/variant_b_slice2_design.md §3: a
            " progressive, ever-widening haves-free loop is exactly the
            " "deepen N is never completeness" anti-pattern this migration
            " removes). Otherwise convert to zcx_abapgit_ortec_git (this
            " method's own declared type) so the existing standard
            " CATCH zcx_abapgit_ortec_git in zcl_abapgit_git_transport falls
            " through to the fully standard fetch path.
            IF is_retry_without_haves( lx_thin_branch ) = abap_true
              OR is_retry_without_haves( lx_nonthin_branch ) = abap_true.
              " Reset the decode-local completion budget again immediately
              " before this single recovery attempt (DR-004 resolution,
              " .memory/logs/variant_b_slice2_design.md §8) - in addition
              " to, not instead of, the once-per-call reset above.
              zcl_abapgit_ortec_pack_stream=>reset_completion_budget( ).
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
              lv_ref_data = lo_client->get_cdata( ).
              lv_server_caps = zcl_abapgit_ortec_fetch_req=>parse_capabilities( lv_ref_data ).
              TRY.
                  et_objects = upload_pack(
                    io_client       = lo_client
                    iv_url          = iv_url
                    it_hashes       = lt_hashes
                    iv_mode         = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-recovery_branch_full
                    iv_server_caps  = lv_server_caps ).
                  ev_deepen_used = 0.
                  RETURN.
                CATCH zcx_abapgit_ortec_git zcx_abapgit_exception INTO DATA(lx_recovery_branch).
                  zcx_abapgit_ortec_git=>raise(
                    |Ortec fastpath failed after thin+self-contained+recovery retry - thin: | &&
                    |{ lx_thin_branch->get_text( ) }, self-contained: { lx_nonthin_branch->get_text( ) }, | &&
                    |recovery: { lx_recovery_branch->get_text( ) }| ).
              ENDTRY.
            ENDIF.
            zcx_abapgit_ortec_git=>raise(
              |Ortec fastpath failed after thin+self-contained retry - thin: { lx_thin_branch->get_text( ) }, | &&
              |self-contained: { lx_nonthin_branch->get_text( ) }| ).
        ENDTRY.
    ENDTRY.

  ENDMETHOD.


  METHOD upload_pack_by_commit.

    DATA lt_hashes TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_headers TYPE zcl_abapgit_http=>ty_headers.
    DATA ls_header  LIKE LINE OF lt_headers.
    DATA lo_client TYPE REF TO zcl_abapgit_http_client.
    DATA lv_ref_data TYPE string.
    DATA lv_server_caps TYPE string.


    CLEAR: et_objects,
           ev_commit,
           ev_deepen_used.

    " See the matching comment in upload_pack_by_branch.
    zcl_abapgit_ortec_pack_stream=>reset_completion_budget( ).

    APPEND iv_hash TO lt_hashes.
    ev_commit = iv_hash.

    ls_header-key   = '~request_uri'.
    ls_header-value = zcl_abapgit_url=>path_name( iv_url ) && |/info/refs?service=git-upload-pack|.
    APPEND ls_header TO lt_headers.

    lo_client = zcl_abapgit_http=>create_by_url(
      iv_url     = iv_url
      it_headers = lt_headers ).

    " See the matching comment in upload_pack_by_branch.
    lv_ref_data = lo_client->get_cdata( ).
    lv_server_caps = zcl_abapgit_ortec_fetch_req=>parse_capabilities( lv_ref_data ).

    TRY.
        et_objects = upload_pack(
          io_client       = lo_client
          iv_url          = iv_url
          iv_deepen_level = iv_deepen_level
          it_hashes       = lt_hashes
          iv_mode         = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-incremental_thin
          iv_server_caps  = lv_server_caps ).
        " F-2C-002: see the matching comment in upload_pack_by_branch.
        ev_deepen_used = 0.
      CATCH zcx_abapgit_ortec_git zcx_abapgit_exception INTO DATA(lx_thin_commit).
        " Thin/ofs attempt failed - retry once, self-contained, with a fresh
        " client (the failed attempt's request/response is already
        " consumed). See target_design_phase5.md §6 fail-safe cascade.
        " Both exception types are caught - see the matching comment in
        " upload_pack_by_branch for why zcx_abapgit_exception can also occur.
        lo_client = zcl_abapgit_http=>create_by_url(
          iv_url     = iv_url
          it_headers = lt_headers ).
        lv_ref_data = lo_client->get_cdata( ).
        lv_server_caps = zcl_abapgit_ortec_fetch_req=>parse_capabilities( lv_ref_data ).
        TRY.
            et_objects = upload_pack(
              io_client       = lo_client
              iv_url          = iv_url
              iv_deepen_level = iv_deepen_level
              it_hashes       = lt_hashes
              iv_mode         = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-incremental_self_contained
              iv_server_caps  = lv_server_caps ).
            " F-2C-002: see the matching comment in upload_pack_by_branch.
            ev_deepen_used = 0.
          CATCH zcx_abapgit_ortec_git zcx_abapgit_exception INTO DATA(lx_nonthin_commit).
            " Self-contained Ortec retry also failed. See the matching
            " comment in upload_pack_by_branch: if either failure indicates
            " a fresh haves-free fetch might succeed, make exactly ONE
            " RECOVERY_BRANCH_FULL attempt - no progressive widening, no
            " deepen.
            IF is_retry_without_haves( lx_thin_commit ) = abap_true
              OR is_retry_without_haves( lx_nonthin_commit ) = abap_true.
              " Reset the decode-local completion budget again immediately
              " before this single recovery attempt (DR-004 resolution) -
              " in addition to, not instead of, the once-per-call reset
              " above.
              zcl_abapgit_ortec_pack_stream=>reset_completion_budget( ).
              lo_client = zcl_abapgit_http=>create_by_url(
                iv_url     = iv_url
                it_headers = lt_headers ).
              lv_ref_data = lo_client->get_cdata( ).
              lv_server_caps = zcl_abapgit_ortec_fetch_req=>parse_capabilities( lv_ref_data ).
              TRY.
                  et_objects = upload_pack(
                    io_client       = lo_client
                    iv_url          = iv_url
                    it_hashes       = lt_hashes
                    iv_mode         = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-recovery_branch_full
                    iv_server_caps  = lv_server_caps ).
                  ev_deepen_used = 0.
                  RETURN.
                CATCH zcx_abapgit_ortec_git zcx_abapgit_exception INTO DATA(lx_recovery_commit).
                  zcx_abapgit_ortec_git=>raise(
                    |Ortec fastpath failed after thin+self-contained+recovery retry - thin: | &&
                    |{ lx_thin_commit->get_text( ) }, self-contained: { lx_nonthin_commit->get_text( ) }, | &&
                    |recovery: { lx_recovery_commit->get_text( ) }| ).
              ENDTRY.
            ENDIF.
            zcx_abapgit_ortec_git=>raise(
              |Ortec fastpath failed after thin+self-contained retry - thin: { lx_thin_commit->get_text( ) }, | &&
              |self-contained: { lx_nonthin_commit->get_text( ) }| ).
        ENDTRY.
    ENDTRY.

  ENDMETHOD.


METHOD upload_pack.

    DATA lv_xstring TYPE xstring.
    DATA lv_pack    TYPE xstring.
    DATA lt_ortec_haves TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv_have_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.

    DATA lo_fetch_timer   TYPE REF TO zcl_abapgit_timer.
    DATA lv_fetch_duration TYPE string.
    DATA li_progress      TYPE REF TO zif_abapgit_progress.


    io_client->set_headers(
      iv_url     = iv_url
      iv_service = 'upload' ).

    " Resolve have commits BEFORE assembling the want/capability line, so the
    " capability string can correctly reflect whether thin-pack/ofs-delta are
    " safe to advertise. Both INCREMENTAL_THIN and INCREMENTAL_SELF_CONTAINED
    " use only certified (HIST_LEVEL = FULL_COMPLETE) haves, sourced from
    " ZCL_ABAPGIT_ORTEC_HAVE_POLICY=>GET_CERTIFIED_HAVES (Variant B Package C
    " C1 - .memory/logs/variant_b_package_c_design.md §5): advertising ANY
    " have that turns out to be incomplete lets the server omit/delta-encode
    " objects against content we don't actually have, which is exactly the
    " failure class Phase 1 of the architecture hardening plan (.memory/
    " state.md, 2026-07-20) exists to close - the unverified
    " zcl_abapgit_ortec_fetch_neg=>get_have_commits must not be used for live
    " negotiation for either mode. GET_CERTIFIED_HAVES is a single bulk SQL
    " read against ZAOG_COMMIT_HIST scoped by repo_key (no per-candidate SQL,
    " no object-store payload read, no graph walk - see its own docstring);
    " ZCL_ABAPGIT_ORTEC_FETCH_NEG=>GET_VERIFIED_HAVE_COMMITS/GET_HAVE_COMMITS/
    " COLLECT_ANCESTOR_HAVES are no longer called from this method.
    " RECOVERY_BRANCH_FULL and MATERIALIZE_BLOBS never negotiate haves
    " (lt_ortec_haves stays empty, per the Variant B mode table): used by the
    " thin+non-thin cascade's last-resort retry when a prior attempt
    " discovered the server's "nothing new" response could not actually be
    " trusted against the local cache - offering the same haves again would
    " likely reproduce the identical false "nothing new" outcome.
    " GET_CERTIFIED_HAVES is not wrapped in a blanket empty CATCH here: a
    " valid "no candidates yet" outcome already returns an empty table (not
    " an exception, see its own no-repo-key/no-row RETURN guards) - it also
    " performs no per-candidate SQL, so there is no per-candidate technical
    " failure to normalize either.
    IF iv_mode = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-incremental_thin
        OR iv_mode = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-incremental_self_contained.
      lv_have_repo_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( iv_url ).
      IF lv_have_repo_key IS NOT INITIAL.
        lt_ortec_haves = zcl_abapgit_ortec_have_policy=>get_certified_haves(
          iv_repo_key    = lv_have_repo_key
          it_want_hashes = it_hashes ).
      ENDIF.
    ENDIF.

    DATA(ls_request) = zcl_abapgit_ortec_fetch_req=>build_request(
      iv_mode            = iv_mode
      it_want_hashes     = it_hashes
      it_certified_haves = lt_ortec_haves
      iv_server_caps     = iv_server_caps ).


    lo_fetch_timer = zcl_abapgit_timer=>create( )->start( ).
    lv_xstring = io_client->send_receive_close( zcl_abapgit_convert=>string_to_xstring_utf8( ls_request-buffer ) ).

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
          DATA lv_via_streaming TYPE abap_bool.
          lv_ortec_rk = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( iv_url ).
          IF lv_ortec_rk IS NOT INITIAL.
            " Phase 4 routing: the streaming decoder is the DEFAULT engine for
            " any Ortec-active repo (design decision 6) - it never risks the
            " rt_objects-class SYSTEM_NO_ROLL ceiling since decoded objects
            " are persisted and freed one at a time, and it returns only the
            " sparse commit-object set standard pull()/H4 actually need. On
            " any streaming failure, fall back to the proven non-streaming
            " decoder on the SAME pack bytes (DR-001 fallback cascade, tier
            " 2) - both understand the full OFS/thin pack format, so this
            " re-decode is safe (unlike the standard abapGit decoder, which
            " the comment below this TRY still correctly refuses to use).
            TRY.
                rt_objects = zcl_abapgit_ortec_pack_stream=>decode_streaming(
                  iv_data     = lv_pack
                  iv_repo_key = lv_ortec_rk
                  iv_url      = iv_url ).
                lv_via_streaming = abap_true.
              CATCH zcx_abapgit_ortec_git INTO DATA(lx_streaming).
                " TEMPORARY DIAGNOSTIC (2026-07-17): a live repo is hitting
                " SYSTEM_NO_ROLL via the fallback tier below, meaning
                " decode_streaming itself is failing for this repo/pack and
                " the OLD decoder then predictably crashes on the same huge
                " pack - exactly the crash this whole effort exists to
                " remove. Re-raising here INSTEAD OF falling back, so the
                " real, specific streaming failure reason surfaces as a
                " clean, catchable error (visible in the abapGit UI) rather
                " than being silently masked by a fallback that only trades
                " one crash for the exact same crash. TODO: once the real
                " root cause is understood and fixed (or confirmed to be a
                " genuine, unfixable-in-v1 edge case), restore the
                " decode_and_persist fallback call that used to be here.
                RAISE EXCEPTION lx_streaming.
            ENDTRY.
            IF rt_objects IS NOT INITIAL.
              " decode_and_persist/decode_streaming already called
              " invalidate_cache() internally. rt_objects is either the full
              " merged set (fallback tier) or the sparse commit-only set
              " (streaming, the default) - the object count in the progress
              " message below reflects whichever actually ran, made visible
              " so a streaming->fallback event is never silently normalized.
              lv_fetch_duration = lo_fetch_timer->end( ).
              li_progress = zcl_abapgit_progress=>get_instance( 1 ).
              IF lv_via_streaming = abap_true.
                li_progress->show(
                  iv_current = 1
                  iv_text    = |Fetch (streaming): { lines( rt_objects ) } commit(s), { lv_fetch_duration }| ).
              ELSE.
                li_progress->show(
                  iv_current = 1
                  iv_text    = |Fetch (fallback decoder): { lines( rt_objects ) } git objects, { lv_fetch_duration }| ).
              ENDIF.
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
            zcx_abapgit_ortec_git=>raise(
              iv_text                = 'Cached multi-want nothing-new response requires standard fetch'
              iv_retry_without_haves = abap_true ).
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
                  iv_text                = 'Cached objects have incomplete tree - falling back to standard fetch'
                  iv_retry_without_haves = abap_true ).
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
        CATCH zcx_abapgit_ortec_git INTO DATA(lx_cache_reason).
          " Preserve the specific reason (multi-want / incomplete tree) for
          " the final raise below instead of silently discarding it - a
          " generic "not available" message with no detail was hiding the
          " actual cause of every prior incident in this method.
      ENDTRY.
    ENDIF.

    " Cached objects are unavailable or unsafe; caller must use the standard fetch path.
    " Every failure above means the same thing: the local cache cannot
    " safely stand in for the server's "nothing new" claim - a fresh
    " haves-free retry (see upload_pack's RECOVERY_BRANCH_FULL mode) is
    " always worth attempting regardless of which specific reason fired.
    IF lx_cache_reason IS BOUND.
      zcx_abapgit_ortec_git=>raise(
        iv_text                = |Cached objects not available for nothing-new response - | &&
                                  |falling back to standard fetch: { lx_cache_reason->get_text( ) }|
        iv_retry_without_haves = abap_true ).
    ELSE.
      zcx_abapgit_ortec_git=>raise(
        iv_text                = 'Cached objects not available for nothing-new response - falling back to standard fetch'
        iv_retry_without_haves = abap_true ).
    ENDIF.

  ENDMETHOD.


  METHOD is_retry_without_haves.
    DATA lx_ortec TYPE REF TO zcx_abapgit_ortec_git.
    IF ix_exception IS INSTANCE OF zcx_abapgit_ortec_git.
      lx_ortec ?= ix_exception.
      rv_yes = lx_ortec->mv_retry_without_haves.
    ENDIF.
  ENDMETHOD.

  METHOD first_progressive_deepen.
    rv_deepen = nmax( val1 = c_progressive_start_min
                       val2 = iv_prior_deepen * c_progressive_widen_factor ).
    IF rv_deepen > c_progressive_max_deepen.
      rv_deepen = c_progressive_max_deepen.
    ENDIF.
  ENDMETHOD.

  METHOD next_progressive_deepen.
    rv_deepen = nmin( val1 = c_progressive_max_deepen
                       val2 = iv_current * c_progressive_widen_factor ).
  ENDMETHOD.


  METHOD build_upload_pack_buffer.

    DATA lv_capa    TYPE string.
    DATA lv_line    TYPE string.
    DATA lv_buffer  TYPE string.
    DATA lv_advertise_thin TYPE abap_bool.
    DATA lv_effective_deepen TYPE i.

    FIELD-SYMBOLS <lv_hash> LIKE LINE OF it_hashes.
    FIELD-SYMBOLS <lv_ortec_have> LIKE LINE OF it_ortec_haves.

    " Only advertise thin-pack/ofs-delta when the caller allowed it AND at
    " least one verified-complete have exists to delta against - otherwise
    " thin capability would be pointless (nothing to delta against) or, if
    " haves existed but were unverified, unsafe.
    lv_advertise_thin = xsdbool( iv_allow_thin = abap_true AND it_ortec_haves IS NOT INITIAL ).

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

    IF iv_force_full = abap_false AND it_ortec_haves IS NOT INITIAL.
      LOOP AT it_ortec_haves ASSIGNING <lv_ortec_have>.
        lv_buffer = lv_buffer && zcl_abapgit_git_utils=>pkt_string(
          |shallow { <lv_ortec_have> }{ cl_abap_char_utilities=>newline }| ).
      ENDLOOP.
    ENDIF.

    " Only send deepen when we have NO cached objects (first fetch).
    " With deepen, the server ignores have lines and sends a full shallow pack.
    " Without deepen (but with haves), the server sends only the delta.
    " Whenever there are NO haves at all, ALWAYS send at least deepen 1 -
    " confirmed live that an empty have-set combined with iv_deepen_level=0
    " sends neither a deepen line nor any have lines, which is standard git
    " wire-protocol shorthand for "send the complete history from the
    " beginning of the repo" (the same class of bug fixed for
    " zcl_abapgit_ortec_missing_obj=>ensure_available's own call site
    " earlier - this closes the same gap for every caller of this method,
    " not just that one).
    " iv_force_full does NOT change this - it no longer omits deepen (that
    " approach, from an earlier commit, requested a repo's COMPLETE
    " unbounded history in one shot, which failed live for a repo with real,
    " substantial history - see the "Architecture hardening plan" /
    " Phase 1 in .memory/state.md, 2026-07-20). force_full callers are now
    " responsible for choosing a PROGRESSIVE, widening iv_deepen_level
    " themselves across repeated attempts (see upload_pack_by_branch/
    " upload_pack_by_commit's retry loop) - this method's own job is
    " unchanged: send deepen whenever there are no haves to negotiate with,
    " using whatever value the caller passed.
    IF it_ortec_haves IS INITIAL.
      lv_effective_deepen = iv_deepen_level.
      IF lv_effective_deepen <= 0.
        lv_effective_deepen = 1.
      ENDIF.
      lv_buffer = lv_buffer && zcl_abapgit_git_utils=>pkt_string( |deepen { lv_effective_deepen }| &&
        cl_abap_char_utilities=>newline ).
    ENDIF.

    lv_buffer = lv_buffer && '0000'.

    IF iv_force_full = abap_false AND it_ortec_haves IS NOT INITIAL.
      LOOP AT it_ortec_haves ASSIGNING <lv_ortec_have>.
        lv_buffer = lv_buffer && zcl_abapgit_git_utils=>pkt_string(
          |have { <lv_ortec_have> }{ cl_abap_char_utilities=>newline }| ).
      ENDLOOP.
    ENDIF.

    lv_buffer = lv_buffer && '0009done' && cl_abap_char_utilities=>newline.

    rv_buffer = lv_buffer.

  ENDMETHOD.


  METHOD parse.

    CONSTANTS lc_band1 TYPE x VALUE '01'.

    DATA lv_len      TYPE i.
    DATA lv_contents TYPE xstring.
    DATA lv_pack     TYPE xstring.
    DATA lv_text     TYPE string.
    DATA lv_sha1     TYPE zif_abapgit_git_definitions=>ty_sha1.

    CLEAR: et_shallow, et_unshallow.

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
            CONTINUE.
          ENDIF.

          TRY.
            lv_text = zcl_abapgit_convert=>xstring_to_string_utf8_raw( lv_contents ).
          CATCH zcx_abapgit_exception.
            CONTINUE.
          ENDTRY.

          REPLACE ALL OCCURRENCES OF cl_abap_char_utilities=>cr_lf IN lv_text WITH ''.
          REPLACE ALL OCCURRENCES OF cl_abap_char_utilities=>newline IN lv_text WITH ''.

          IF lv_text CP 'shallow *' AND strlen( lv_text ) > 8.
            lv_sha1 = lv_text+8.
            APPEND lv_sha1 TO et_shallow.
          ELSEIF lv_text CP 'unshallow *' AND strlen( lv_text ) > 10.
            lv_sha1 = lv_text+10.
            APPEND lv_sha1 TO et_unshallow.
          ENDIF.
        ENDWHILE.
      CATCH cx_sy_range_out_of_bounds INTO DATA(lx_range).
        zcx_abapgit_ortec_git=>raise( |parse, pkt-line framing error: { lx_range->get_text( ) }| ).
    ENDTRY.

    ev_pack = lv_pack.

  ENDMETHOD.

  METHOD persist_missing_objects.
    DATA lv_ts             TYPE timestampl.
    DATA ls_row            TYPE zaog_obj_store.
    DATA lt_new            TYPE STANDARD TABLE OF zaog_obj_store.
    DATA lt_unique_sha1s   TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_missing_sha1s  TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_missing_lookup TYPE HASHED TABLE OF zif_abapgit_git_definitions=>ty_sha1 WITH UNIQUE KEY table_line.
    DATA lt_appended_sha1s TYPE HASHED TABLE OF zif_abapgit_git_definitions=>ty_sha1 WITH UNIQUE KEY table_line.

    GET TIME STAMP FIELD lv_ts.

    FIELD-SYMBOLS <ls_obj> LIKE LINE OF it_objects.
    FIELD-SYMBOLS <lv_sha1> LIKE LINE OF lt_unique_sha1s.

    LOOP AT it_objects ASSIGNING <ls_obj>.
      IF <ls_obj>-sha1 IS INITIAL.
        CONTINUE.
      ENDIF.
      APPEND <ls_obj>-sha1 TO lt_unique_sha1s.
    ENDLOOP.
    SORT lt_unique_sha1s.
    DELETE ADJACENT DUPLICATES FROM lt_unique_sha1s.

    lt_missing_sha1s = zcl_abapgit_ortec_obj_store=>get_missing_sha1s(
                         iv_repo_key = iv_repo_key
                         it_sha1s    = lt_unique_sha1s ).
    LOOP AT lt_missing_sha1s assigning <lv_sha1>.
      INSERT <lv_sha1> INTO TABLE lt_missing_lookup.
    ENDLOOP.

    LOOP AT it_objects ASSIGNING <ls_obj>.
      IF <ls_obj>-sha1 IS INITIAL.
        CONTINUE.
      ENDIF.
      READ TABLE lt_missing_lookup WITH TABLE KEY table_line = <ls_obj>-sha1 TRANSPORTING NO FIELDS.
      IF sy-subrc <> 0.
        CONTINUE.
      ENDIF.
      READ TABLE lt_appended_sha1s WITH TABLE KEY table_line = <ls_obj>-sha1 TRANSPORTING NO FIELDS.
      IF sy-subrc = 0.
        CONTINUE.
      ENDIF.
      INSERT <ls_obj>-sha1 INTO TABLE lt_appended_sha1s.

      CLEAR ls_row.
      ls_row-repo_key   = iv_repo_key.
      ls_row-obj_sha1   = <ls_obj>-sha1.
      ls_row-obj_type   = <ls_obj>-type.
      ls_row-obj_data   = <ls_obj>-data.
      ls_row-obj_size   = xstrlen( <ls_obj>-data ).
      ls_row-created_at = lv_ts.
      ls_row-status     = 'R'.
      APPEND ls_row TO lt_new.
    ENDLOOP.
    IF lt_new IS NOT INITIAL.
      MODIFY zaog_obj_store FROM TABLE lt_new.
      " Invalidate in-memory session cache: MODIFY wrote directly to DB
      " (bypassing store_object/store_objects which call invalidate_cache).
      zcl_abapgit_ortec_obj_store=>invalidate_cache( ).
    ENDIF.
  ENDMETHOD.

  METHOD persist_pull_result.
    DATA lv_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.

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

    persist_missing_objects(
      iv_repo_key = lv_repo_key
      it_objects  = it_objects ).

    " Package C C2: certification lifecycle - replaces the pre-Package-C raw
    " ZAOG_COMMIT_HIST INSERT, which never set hist_level/snap_state (see
    " .memory/logs/variant_b_package_c_design.md §0). This method is reached
    " ONLY by the INCREMENTAL_UPDATE routing branch of
    " zcl_abapgit_ortec_porcelain=>pull_by_branch/pull_by_commit -
    " WARM_UNCHANGED and COLD_BRANCH are already fully (re-)certified by
    " their own respective paths (local reconstruction / Package B
    " zcl_abapgit_ortec_cold_init) before this method would ever run.
    " Extracted into CERTIFY_FETCHED_COMMIT for direct unit-testability
    " (see that method's doc comment for why it cannot be tested through
    " this public entry point).
    certify_fetched_commit(
      iv_repo_key    = lv_repo_key
      iv_commit      = iv_commit
      iv_branch_name = iv_branch_name ).

    " Bookkeeping only (deepen level / last-fetch pointer for the thin ->
    " self-contained -> recovery cascade's own depth heuristics) - never a
    " completeness/have-certification signal. Kept unconditional so a
    " round that only achieved GRAPH_COMPLETE (or nothing) still records
    " that a fetch attempt happened.
    zcl_abapgit_ortec_repo_state=>update_after_fetch(
        iv_repo_key    = lv_repo_key
        iv_branch_name = iv_branch_name
        iv_url         = iv_url
        iv_commit      = iv_commit
        iv_deepen      = iv_deepen_used ).

    COMMIT WORK.
  ENDMETHOD.

  METHOD certify_fetched_commit.
    DATA(lv_attempt_id) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = iv_repo_key
      iv_commit   = iv_commit ).

    DATA(lv_graph_complete) = abap_false.
    TRY.
        zcl_abapgit_ortec_obj_store=>verify_tree_closure(
          iv_repo_key = iv_repo_key
          iv_commit   = iv_commit ).
        lv_graph_complete = abap_true.
      CATCH zcx_abapgit_ortec_git.
        " Expected, non-exceptional local incompleteness (e.g. a thin fetch
        " whose delta bases/trees are not all locally resident yet) - no
        " certificate is published this round. Already-persisted objects
        " and the caller's bookkeeping call remain unaffected.
    ENDTRY.

    IF lv_graph_complete = abap_false.
      RETURN.
    ENDIF.

    zcl_abapgit_ortec_mat_state=>mark_graph_complete(
      iv_repo_key   = iv_repo_key
      iv_commit     = iv_commit
      iv_attempt_id = lv_attempt_id ).

    DATA(lt_tip_blob_sha1s) = zcl_abapgit_ortec_obj_store=>get_tip_blob_sha1s(
      iv_repo_key = iv_repo_key
      iv_commit   = iv_commit ).
    DATA(lt_missing_sha1s) = zcl_abapgit_ortec_obj_store=>get_missing_sha1s(
      iv_repo_key = iv_repo_key
      it_sha1s    = lt_tip_blob_sha1s ).

    IF lt_missing_sha1s IS NOT INITIAL.
      RETURN.
    ENDIF.

    zcl_abapgit_ortec_mat_state=>mark_full_complete(
      iv_repo_key   = iv_repo_key
      iv_commit     = iv_commit
      iv_attempt_id = lv_attempt_id ).
    zcl_abapgit_ortec_mat_state=>publish_snapshot_complete(
      iv_repo_key    = iv_repo_key
      iv_branch_name = iv_branch_name
      iv_commit      = iv_commit
      iv_attempt_id  = lv_attempt_id ).
  ENDMETHOD.

  METHOD resolve_repo_key.
    rv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( iv_url ).
  ENDMETHOD.
ENDCLASS.
