"! <p class="shorttext synchronized">ORTEC Git Cold Blobless Graph Acquisition</p>
"! Variant B / Package B, checkpoint B1: given an already-resolved
"! repository key and branch tip commit (resolution/validation of the
"! advertised remote tip and the decision that a branch is "cold" both
"! remain the caller's/Package C's responsibility - see
"! .memory/logs/variant_b_package_b_design.md §3), performs one
"! INITIAL_BRANCH_BLOBLESS (`filter blob:none`) fetch, persists the
"! decoded commit/tree/blob objects via the streaming decoder, verifies
"! the complete commit -> tree closure is present (blobs are NOT
"! required - they are legitimately promised, see
"! zcl_abapgit_ortec_obj_store=>verify_tree_closure), and only then
"! publishes GRAPH_COMPLETE via zcl_abapgit_ortec_mat_state. No productive
"! caller is wired to this class yet (Package C scope) - see the same
"! precedent already established by Slice 2's zcl_abapgit_ortec_fetch_req.
CLASS zcl_abapgit_ortec_cold_init DEFINITION
  PUBLIC FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    "! Memory-risk ceiling (INV-B-12,
    "! .memory/logs/variant_b_package_b_design.md §10) on the single
    "! materialized HTTP response XSTRING for one ACQUIRE_BLOBLESS_GRAPH
    "! call. INITIAL_BRANCH_BLOBLESS never sends deepen/shallow/haves, so
    "! its response size is bounded only by total repository history size -
    "! this is the one Package B request shape without an inherent
    "! want-count-based bound (unlike MATERIALIZE_BLOBS's
    "! c_materialize_batch_max), matching the mandatory memory-risk gate
    "! requirement in .github/skills/git-partial-clone/SKILL.md for any
    "! full/unbounded history response materialized as one XSTRING.
    "! Exceeding this raises zcx_abapgit_ortec_git - no fallback within
    "! Package B (fail-fast; propagate, caller/Package C decides).
    CONSTANTS c_max_graph_response_bytes TYPE i VALUE 209715200.

    "! B1 entry point. Establishes its own HTTP connection (own
    "! info/refs capability discovery, exactly like
    "! zcl_abapgit_ortec_fastpath=>upload_pack_by_commit) rather than
    "! accepting a caller-supplied client - this method IS a true
    "! top-level fetch attempt, not a step nested inside another one.
    "! @parameter iv_url |
    "! Repository remote URL
    "! @parameter iv_repo_key |
    "! Repository key (already resolved by the caller)
    "! @parameter iv_tip_commit |
    "! Branch tip commit SHA1 to want (already resolved/advertised - this
    "! method does not itself query or validate a branch ref)
    "! @raising zcx_abapgit_ortec_git |
    "! Capability missing, oversized response (memory-risk gate), decode
    "! failure, incomplete tree closure, or persistence failure. No
    "! GRAPH_COMPLETE certificate is published unless every step succeeds.
    CLASS-METHODS acquire_blobless_graph
      IMPORTING iv_url        TYPE string
                iv_repo_key   TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                iv_tip_commit TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING   zcx_abapgit_ortec_git.
ENDCLASS.


CLASS zcl_abapgit_ortec_cold_init IMPLEMENTATION.
  METHOD acquire_blobless_graph.

    DATA lt_headers     TYPE zcl_abapgit_http=>ty_headers.
    DATA lv_attempt_id  TYPE zcl_abapgit_ortec_mat_state=>ty_attempt_id.
    DATA ls_header      LIKE LINE OF lt_headers.
    DATA lo_client      TYPE REF TO zcl_abapgit_http_client.
    DATA lv_ref_data    TYPE string.
    DATA lv_server_caps TYPE string.
    DATA lt_want        TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv_response    TYPE xstring.
    DATA lv_pack        TYPE xstring.
    " TODO: variable is assigned but never used (ABAP cleaner)
    DATA lt_shallow     TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    " TODO: variable is assigned but never used (ABAP cleaner)
    DATA lt_unshallow   TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    IF iv_repo_key IS INITIAL.
      zcx_abapgit_ortec_git=>raise( 'Cold-init requires a repository key' ).
    ENDIF.
    IF iv_tip_commit IS INITIAL.
      zcx_abapgit_ortec_git=>raise( 'Cold-init requires a tip commit' ).
    ENDIF.

    " True top-level fetch attempt - reset the shared thin-completion
    " budget once, per decode_streaming's documented contract (mirrors
    " upload_pack_by_branch/upload_pack_by_commit precedent). This mode
    " never triggers thin completion (INITIAL_BRANCH_BLOBLESS is never
    " thin), but the reset is still owned here since this call IS the
    " top-level attempt boundary.
    zcl_abapgit_ortec_pack_stream=>reset_completion_budget( ).

    lv_attempt_id = zcl_abapgit_ortec_mat_state=>begin_attempt(
                        iv_repo_key = iv_repo_key
                        iv_commit   = iv_tip_commit ).

    TRY.

        ls_header-key   = '~request_uri'.
        ls_header-value = |{ zcl_abapgit_url=>path_name( iv_url ) }/info/refs?service=git-upload-pack|.
        APPEND ls_header TO lt_headers.

        lo_client = zcl_abapgit_http=>create_by_url(
                        iv_url     = iv_url
                        it_headers = lt_headers ).

        lv_ref_data    = lo_client->get_cdata( ).
        lv_server_caps = zcl_abapgit_ortec_fetch_req=>parse_capabilities( lv_ref_data ).

        APPEND iv_tip_commit TO lt_want.

        DATA(ls_request) = zcl_abapgit_ortec_fetch_req=>build_request(
                               iv_mode        = zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-initial_branch_blobless
                               it_want_hashes = lt_want
                               iv_server_caps = lv_server_caps ).

        lo_client->set_headers( iv_url = iv_url iv_service = 'upload' ).

        lv_response = lo_client->send_receive_close( zcl_abapgit_convert=>string_to_xstring_utf8( ls_request-buffer ) ).

        " INV-B-12: memory-risk gate, checked on the single materialized
        " HTTP response XSTRING BEFORE any further parsing/decoding.
        IF xstrlen( lv_response ) > c_max_graph_response_bytes.
          zcx_abapgit_ortec_git=>raise( |Cold-init blobless response for { iv_tip_commit } exceeds the memory-risk | &&
                                        |ceiling ({ xstrlen( lv_response ) } > { c_max_graph_response_bytes } bytes)| ).
        ENDIF.

        zcl_abapgit_ortec_fastpath=>parse(
          IMPORTING
            ev_pack      = lv_pack
            et_shallow   = lt_shallow
            et_unshallow = lt_unshallow
          CHANGING
            cv_data      = lv_response ).
      CATCH zcx_abapgit_exception INTO DATA(lx_error).

        RAISE EXCEPTION NEW zcx_abapgit_ortec_git(
                                iv_text  = |Cold-init blobless fetch for { iv_tip_commit } failed: | &&
                                           |{ lx_error->get_text( ) }|
                                previous = lx_error ).

    ENDTRY.

    IF lv_pack IS INITIAL OR zcl_abapgit_ortec_pack_dec=>peek_object_count( lv_pack ) = 0.
      zcx_abapgit_ortec_git=>raise( |Cold-init blobless fetch for { iv_tip_commit } returned no pack data| ).
    ENDIF.

    zcl_abapgit_ortec_pack_stream=>decode_streaming(
        iv_data     = lv_pack
        iv_repo_key = iv_repo_key
        iv_url      = iv_url ).

    zcl_abapgit_ortec_obj_store=>verify_tree_closure(
        iv_repo_key = iv_repo_key
        iv_commit   = iv_tip_commit ).

    zcl_abapgit_ortec_mat_state=>mark_graph_complete(
        iv_repo_key   = iv_repo_key
        iv_commit     = iv_tip_commit
        iv_attempt_id = lv_attempt_id ).

    COMMIT WORK.

  ENDMETHOD.
ENDCLASS.
