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
    "! Nested batch table used by CHUNK_MISSING_SHA1S - one entry per
    "! bounded MATERIALIZE_BLOBS want-list batch.
    TYPES ty_sha1_batch_tt TYPE STANDARD TABLE OF zif_abapgit_git_definitions=>ty_sha1_tt WITH EMPTY KEY.

    "! Deterministic decision returned by DECIDE_OVERSIZE_ACTION - see its
    "! own doc.
    TYPES ty_oversize_action TYPE c LENGTH 1.

    CONSTANTS: BEGIN OF cs_oversize_action,
                 "! Response is within budget - persist as-is.
                 none  TYPE ty_oversize_action VALUE ' ',
                 "! Response exceeds budget and the batch can still be
                 "! halved within its per-batch split budget.
                 split TYPE ty_oversize_action VALUE 'S',
                 "! Response exceeds budget and cannot be split further
                 "! (single-SHA1 batch, or split budget exhausted) - a
                 "! structured failure, never a silent unbounded accept.
                 raise TYPE ty_oversize_action VALUE 'R',
               END OF cs_oversize_action.

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

    "! Per-batch response byte ceiling for MATERIALIZE_TIP_SNAPSHOT
    "! (design §10). A batch whose response exceeds this AND still has
    "! more than one want is halved and retried as two independent
    "! MATERIALIZE_BLOBS requests, bounded to C_MAX_OVERSIZE_SPLITS splits
    "! - the split budget is local to EACH top-level batch (design-review
    "! Finding 3 fix, INV-B-07b), never shared across the other batches of
    "! the same MATERIALIZE_TIP_SNAPSHOT call.
    CONSTANTS c_max_batch_response_bytes TYPE i VALUE 26214400.

    "! Bound on halving splits per top-level batch - ceil(log2(1000)) = 10,
    "! since C_MATERIALIZE_BATCH_MAX = 1000 is the largest possible
    "! top-level batch.
    CONSTANTS c_max_oversize_splits TYPE i VALUE 10.

    "! Desired successful response size used by the adaptive controller.
    "! The controller adjusts the next row count from the response bytes of the
    "! preceding successful top-level batch.
    CONSTANTS c_target_response_bytes TYPE i VALUE 16777216.

    "! Adaptive row-count limits. The serializer's absolute hard maximum remains
    "! ZCL_ABAPGIT_ORTEC_FETCH_REQ=>C_MATERIALIZE_BATCH_MAX = 1000.
    CONSTANTS c_batch_rows_initial TYPE i VALUE 500.
    CONSTANTS c_batch_rows_min     TYPE i VALUE 50.
    CONSTANTS c_batch_rows_max     TYPE i VALUE 1000.

    "! A successful batch may at most double the row count of its predecessor.
    "! This prevents a very small response from causing an unbounded jump.
    CONSTANTS c_max_batch_growth TYPE i VALUE 2.

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

    "! Variant B / Package B checkpoints B2+B3 (combined per the design's
    "! checkpoint-shape decision, .memory/logs/variant_b_package_b_design.md
    "! §9 - GET_TIP_BLOB_SHA1S has no independent productive caller/
    "! observable behavior on its own): discovers the complete unique blob
    "! SHA1 set reachable from the tip's tree (B2,
    "! zcl_abapgit_ortec_obj_store=>get_tip_blob_sha1s, re-verifying closure
    "! independently every call - no cross-call in-memory trust, design §11
    "! step 1), bulk-subtracts already-READY presence, and - only for the
    "! genuinely missing subset - fetches them via bounded, capability-safe
    "! MATERIALIZE_BLOBS batches (B3), verifying each batch and the complete
    "! set before publishing SNAPSHOT_COMPLETE via zcl_abapgit_ortec_mat_state.
    "! No productive caller is wired to this method yet (Package C scope),
    "! matching ACQUIRE_BLOBLESS_GRAPH's own precedent.
    "! @parameter iv_url |
    "! Repository remote URL
    "! @parameter iv_repo_key |
    "! Repository key (already resolved by the caller)
    "! @parameter iv_branch_name |
    "! Branch whose materialized commit pointer is published on success -
    "! forwarded to zcl_abapgit_ortec_mat_state=>publish_snapshot_complete
    "! @parameter iv_tip_commit |
    "! Commit SHA1 whose tip blob set must be selected and materialized -
    "! must already be GRAPH_COMPLETE (see
    "! zcl_abapgit_ortec_mat_state=>mark_graph_complete/ACQUIRE_BLOBLESS_GRAPH)
    "! @raising zcx_abapgit_ortec_git |
    "! Closure verification failure, missing arbitrary-object-want
    "! capability, an oversized batch that cannot be split further, a
    "! batch that does not fully materialize its wants, or persistence
    "! failure. No SNAPSHOT_COMPLETE certificate is published unless every
    "! selected blob is verified present afterwards.
    CLASS-METHODS materialize_tip_snapshot
      IMPORTING iv_url         TYPE string
                iv_repo_key    TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                iv_branch_name TYPE string
                iv_tip_commit  TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING   zcx_abapgit_ortec_git.

    "! Pure, HTTP-free chunking of a (possibly duplicate-containing)
    "! candidate SHA1 list into batches of at most
    "! zcl_abapgit_ortec_fetch_req=>c_materialize_batch_max entries each,
    "! deduplicated defensively (callers are already expected to pass a
    "! deduplicated set - see GET_TIP_BLOB_SHA1S/GET_MISSING_SHA1S - but
    "! this is a second, harmless dedup layer, not the only guard).
    "! @parameter it_missing |
    "! Candidate SHA1s to batch (order preserved, duplicates ignored)
    "! @parameter rt_batches |
    "! Zero or more batches, each with 1..C_MATERIALIZE_BATCH_MAX entries
    CLASS-METHODS chunk_missing_sha1s
      IMPORTING it_missing        TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RETURNING VALUE(rt_batches) TYPE ty_sha1_batch_tt.

    "! Pure, HTTP-free oversized-response decision (design §10, INV-B-07/
    "! INV-B-07b) - deterministic given the same three inputs, with no
    "! side effects, so it is directly unit-testable without a live HTTP
    "! response.
    "! @parameter iv_response_bytes |
    "! Size of the already-materialized HTTP response for this batch
    "! @parameter iv_batch_size |
    "! Number of SHA1s wanted in this batch
    "! @parameter iv_splits_used |
    "! Number of times THIS top-level batch has already been halved
    "! @parameter rv_action |
    "! NONE (persist as-is), SPLIT (halve and retry both halves), or
    "! RAISE (cannot be split further - a structured failure)
    CLASS-METHODS decide_oversize_action
      IMPORTING iv_response_bytes TYPE i
                iv_batch_size     TYPE i
                iv_splits_used    TYPE i
      RETURNING VALUE(rv_action)  TYPE ty_oversize_action.

  PRIVATE SECTION.
    "! Splits it_batch into two roughly-equal, order-preserving halves.
    "! Pure, HTTP-free - used by MATERIALIZE_BATCH's oversized-response
    "! recovery (design §10).
    CLASS-METHODS split_batch_in_half
      IMPORTING it_batch       TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      EXPORTING et_first_half  TYPE zif_abapgit_git_definitions=>ty_sha1_tt
                et_second_half TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    "! Fetches and decodes/persists exactly one bounded MATERIALIZE_BLOBS
    "! batch. Reuses the operation-local authenticated HTTP client and the
    "! capability set created once by INIT_MATERIALIZE_CLIENT.
    "!
    "! If the response exceeds the hard byte ceiling, the batch is split into
    "! two order-preserving halves. Both recursive children use the same client
    "! and capability set.
    "!
    "! MATERIALIZE_BATCH never closes the client. Client ownership and cleanup
    "! belong exclusively to MATERIALIZE_TIP_SNAPSHOT.
    CLASS-METHODS materialize_batch
      IMPORTING
        iv_url            TYPE string
        iv_repo_key       TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
        it_batch          TYPE zif_abapgit_git_definitions=>ty_sha1_tt
        iv_server_caps    TYPE string
        io_client         TYPE REF TO zcl_abapgit_http_client
        iv_splits_used    TYPE i DEFAULT 0
      EXPORTING
        ev_response_bytes TYPE i
        ev_split_used     TYPE abap_bool
      RAISING
        zcx_abapgit_ortec_git.

    "! Legacy per-batch payload verification retained temporarily for existing
    "! Package B unit tests. MATERIALIZE_TIP_SNAPSHOT no longer calls this
    "! method; Package E uses one final metadata-only VERIFY_READY_BLOBS call.
    "! Physical removal belongs to Package F after reference and regression
    "! validation.
    CLASS-METHODS verify_batch_objects
      IMPORTING iv_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                it_batch    TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RAISING   zcx_abapgit_ortec_git.

    "! Pure publication gate: SNAPSHOT_COMPLETE may only be published if
    "! the final, complete-set re-check (design §11 step 5) found nothing
    "! still missing.
    "! @parameter it_still_missing |
    "! Result of the final GET_MISSING_SHA1S re-check across the complete
    "! original tip blob set
    "! @parameter rv_yes |
    "! ABAP_TRUE only if it_still_missing is empty
    CLASS-METHODS may_publish_snapshot
      IMPORTING it_still_missing TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RETURNING VALUE(rv_yes)    TYPE abap_bool.

    CLASS-METHODS finalize_snapshot
      IMPORTING iv_url         TYPE string
                iv_repo_key    TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                iv_branch_name TYPE string
                iv_tip_commit  TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_attempt_id  TYPE zcl_abapgit_ortec_mat_state=>ty_attempt_id
      RAISING   zcx_abapgit_ortec_git.

    "! Create and authenticate one operation-local HTTP client through the
    "! upload-pack info/refs endpoint. Return the advertised capabilities and
    "! keep the client open for all materialization batches of this operation.
    "! The caller owns the client and must close it on success and failure.
    CLASS-METHODS init_materialize_client
      IMPORTING
        iv_url         TYPE string
      EXPORTING
        eo_client      TYPE REF TO zcl_abapgit_http_client
        ev_server_caps TYPE string
      RAISING
        zcx_abapgit_ortec_git.

    "! Deduplicate SHA1 values while preserving the order of their first
    "! occurrence. This is executed once before adaptive batching begins.
    CLASS-METHODS deduplicate_sha1s
      IMPORTING
        it_sha1s        TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RETURNING
        VALUE(rt_sha1s) TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    "! Return the next bounded slice of an already deduplicated SHA1 list.
    "! EV_NEXT_INDEX identifies the first row not returned by this invocation.
    CLASS-METHODS take_next_batch
      IMPORTING
        it_sha1s       TYPE zif_abapgit_git_definitions=>ty_sha1_tt
        iv_start_index TYPE i
        iv_max_rows    TYPE i
      EXPORTING
        et_batch       TYPE zif_abapgit_git_definitions=>ty_sha1_tt
        ev_next_index  TYPE i.

    "! Calculate the next desired top-level batch size.
    "! A split halves the next top-level size. A successful unsplit batch scales
    "! proportionally toward C_TARGET_RESPONSE_BYTES, with bounded growth.
    CLASS-METHODS calculate_next_batch_size
      IMPORTING
        iv_current_rows     TYPE i
        iv_response_bytes   TYPE i
        iv_split_used       TYPE abap_bool
      RETURNING
        VALUE(rv_next_rows) TYPE i.

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

        RAISE EXCEPTION NEW zcx_abapgit_ortec_git( iv_text  = |Cold-init blobless fetch for { iv_tip_commit } failed: | &&
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


  METHOD materialize_tip_snapshot.

    DATA lt_all_blob_sha1s TYPE
      zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_missing TYPE
      zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_ordered_missing TYPE
      zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_batch TYPE
      zif_abapgit_git_definitions=>ty_sha1_tt.

    DATA lv_attempt_id TYPE
      zcl_abapgit_ortec_mat_state=>ty_attempt_id.
    DATA lo_client TYPE REF TO zcl_abapgit_http_client.
    DATA lv_server_caps TYPE string.
    DATA lv_batch_rows TYPE i.
    DATA lv_next_index TYPE i.
    DATA lv_response_bytes TYPE i.
    DATA lv_split_used TYPE abap_bool.

    IF iv_repo_key IS INITIAL.
      zcx_abapgit_ortec_git=>raise(
        'Materialize requires a repository key' ).
    ENDIF.

    IF iv_branch_name IS INITIAL.
      zcx_abapgit_ortec_git=>raise(
        'Materialize requires a branch name' ).
    ENDIF.

    IF iv_tip_commit IS INITIAL.
      zcx_abapgit_ortec_git=>raise(
        'Materialize requires a tip commit' ).
    ENDIF.

    " Independently verify the graph/tree closure and derive the complete,
    " unique set of blobs referenced by the selected tip.
    lt_all_blob_sha1s =
      zcl_abapgit_ortec_obj_store=>get_tip_blob_sha1s(
        iv_repo_key = iv_repo_key
        iv_commit   = iv_tip_commit ).

    " Presence-only lookup. GET_MISSING_SHA1S does not load OBJ_DATA.
    lt_missing =
      zcl_abapgit_ortec_obj_store=>get_missing_sha1s(
        iv_repo_key = iv_repo_key
        it_sha1s    = lt_all_blob_sha1s ).

    " One attempt spans every adaptive network batch and the final
    " snapshot publication.
    lv_attempt_id =
      zcl_abapgit_ortec_mat_state=>begin_attempt(
        iv_repo_key = iv_repo_key
        iv_commit   = iv_tip_commit ).

    IF lt_missing IS NOT INITIAL.

      TRY.

          " Create and authenticate exactly one operation-local HTTP client.
          " The INFO/REFS response also supplies the advertised capabilities.
          init_materialize_client(
            EXPORTING
              iv_url         = iv_url
            IMPORTING
              eo_client      = lo_client
              ev_server_caps = lv_server_caps ).

          " One reset for the complete top-level materialization attempt,
          " not once per adaptive batch.
          zcl_abapgit_ortec_pack_stream=>reset_completion_budget( ).

          lt_ordered_missing = deduplicate_sha1s( lt_missing ).

          lv_batch_rows = c_batch_rows_initial.
          lv_next_index = 1.

          WHILE lv_next_index <= lines( lt_ordered_missing ).

            CLEAR:
              lt_batch,
              lv_response_bytes,
              lv_split_used.

            take_next_batch(
              EXPORTING
                it_sha1s       = lt_ordered_missing
                iv_start_index = lv_next_index
                iv_max_rows    = lv_batch_rows
              IMPORTING
                et_batch       = lt_batch
                ev_next_index  = lv_next_index ).

            IF lt_batch IS INITIAL.
              EXIT.
            ENDIF.

            materialize_batch(
              EXPORTING
                iv_url           = iv_url
                iv_repo_key      = iv_repo_key
                it_batch         = lt_batch
                iv_server_caps   = lv_server_caps
                io_client        = lo_client
                iv_splits_used   = 0
              IMPORTING
                ev_response_bytes = lv_response_bytes
                ev_split_used     = lv_split_used ).

            lv_batch_rows = calculate_next_batch_size(
              iv_current_rows   = lines( lt_batch )
              iv_response_bytes = lv_response_bytes
              iv_split_used     = lv_split_used ).

          ENDWHILE.

        CATCH zcx_abapgit_ortec_git INTO DATA(lx_materialize).

          " The operation owns the client and must close it on every failure.
          IF lo_client IS BOUND.
            lo_client->close( ).
            CLEAR lo_client.
          ENDIF.

          RAISE EXCEPTION lx_materialize.

      ENDTRY.

      " Normal successful completion of all adaptive and split batches.
      IF lo_client IS BOUND.
        lo_client->close( ).
        CLEAR lo_client.
      ENDIF.

    ENDIF.

    " One metadata-only verification over the complete selected-tip blob set.
    " VERIFY_READY_BLOBS must not load OBJ_DATA.
    zcl_abapgit_ortec_obj_store=>verify_ready_blobs(
      iv_repo_key = iv_repo_key
      it_sha1s    = lt_all_blob_sha1s ).

    " VERIFY_READY_BLOBS either proves the complete set or raises.
    CLEAR lt_missing.

    IF may_publish_snapshot( lt_missing ) = abap_false.
      zcx_abapgit_ortec_git=>raise(
        'Materialize: selected snapshot is not complete' ).
    ENDIF.

    " FINALIZE_SNAPSHOT already performs:
    " - MARK_FULL_COMPLETE
    " - PREPARE_FULL_SNAPSHOT
    " - PUBLISH_SNAPSHOT_COMPLETE
    "
    " Do not call the first two operations separately here.
    finalize_snapshot(
      iv_url         = iv_url
      iv_repo_key    = iv_repo_key
      iv_branch_name = iv_branch_name
      iv_tip_commit  = iv_tip_commit
      iv_attempt_id  = lv_attempt_id ).

    COMMIT WORK.

  ENDMETHOD.

  METHOD chunk_missing_sha1s.

    DATA lt_unique  TYPE HASHED TABLE OF zif_abapgit_git_definitions=>ty_sha1 WITH UNIQUE KEY table_line.
    DATA lt_ordered TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_current TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    FIELD-SYMBOLS <lv_sha1> LIKE LINE OF it_missing.

    LOOP AT it_missing ASSIGNING <lv_sha1> WHERE table_line IS NOT INITIAL.
      READ TABLE lt_unique WITH TABLE KEY table_line = <lv_sha1> TRANSPORTING NO FIELDS.
      IF sy-subrc <> 0.
        INSERT <lv_sha1> INTO TABLE lt_unique.
        APPEND <lv_sha1> TO lt_ordered.
      ENDIF.
    ENDLOOP.

    LOOP AT lt_ordered INTO DATA(lv_sha1).
      APPEND lv_sha1 TO lt_current.
      IF lines( lt_current ) >= zcl_abapgit_ortec_fetch_req=>c_materialize_batch_max.
        APPEND lt_current TO rt_batches.
        CLEAR lt_current.
      ENDIF.
    ENDLOOP.

    IF lt_current IS NOT INITIAL.
      APPEND lt_current TO rt_batches.
    ENDIF.

  ENDMETHOD.


  METHOD decide_oversize_action.

    IF iv_response_bytes <= c_max_batch_response_bytes.
      rv_action = cs_oversize_action-none.
      RETURN.
    ENDIF.

    IF iv_batch_size <= 1 OR iv_splits_used >= c_max_oversize_splits.
      rv_action = cs_oversize_action-raise.
      RETURN.
    ENDIF.

    rv_action = cs_oversize_action-split.

  ENDMETHOD.


  METHOD split_batch_in_half.

    DATA lv_mid TYPE i.

    CLEAR: et_first_half, et_second_half.

    lv_mid = lines( it_batch ) / 2.
    IF lv_mid < 1.
      lv_mid = 1.
    ENDIF.

    et_first_half = it_batch.
    DELETE et_first_half FROM lv_mid + 1.

    et_second_half = it_batch.
    DELETE et_second_half TO lv_mid.

  ENDMETHOD.


  METHOD materialize_batch.

    DATA lv_response TYPE xstring.
    DATA lv_pack TYPE xstring.
    DATA lv_action TYPE ty_oversize_action.

    DATA lt_first_half TYPE
      zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_second_half TYPE
      zif_abapgit_git_definitions=>ty_sha1_tt.

    DATA lt_shallow TYPE
      zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_unshallow TYPE
      zif_abapgit_git_definitions=>ty_sha1_tt.

    DATA lv_child_bytes TYPE i.
    DATA lv_child_split TYPE abap_bool.

    CLEAR:
      ev_response_bytes,
      ev_split_used.

    IF it_batch IS INITIAL.
      zcx_abapgit_ortec_git=>raise(
        'Materialize batch must contain at least one SHA1' ).
    ENDIF.

    IF lines( it_batch ) >
         zcl_abapgit_ortec_fetch_req=>c_materialize_batch_max.
      zcx_abapgit_ortec_git=>raise(
        |Materialize batch contains { lines( it_batch ) } wants; maximum is | &&
        |{ zcl_abapgit_ortec_fetch_req=>c_materialize_batch_max }| ).
    ENDIF.

    IF io_client IS NOT BOUND.
      zcx_abapgit_ortec_git=>raise(
        'Materialize batch requires an initialized HTTP client' ).
    ENDIF.

    TRY.

        DATA(ls_request) =
          zcl_abapgit_ortec_fetch_req=>build_request(
            iv_mode =
              zcl_abapgit_ortec_fetch_req=>cs_fetch_mode-materialize_blobs
            it_want_hashes = it_batch
            iv_server_caps = iv_server_caps ).

        " Restore the Git upload-pack request configuration before every POST.
        " For Digest authentication SET_HEADERS also recalculates the
        " request-specific Authorization header.
        io_client->set_headers(
          iv_url     = iv_url
          iv_service = 'upload' ).

        " SEND_RECEIVE_DATA sends the request without closing the authenticated
        " client. MATERIALIZE_TIP_SNAPSHOT owns and closes the client.
        lv_response = io_client->send_receive_data(
          zcl_abapgit_convert=>string_to_xstring_utf8(
            ls_request-buffer ) ).

        ev_response_bytes = xstrlen( lv_response ).

        lv_action = decide_oversize_action(
          iv_response_bytes = ev_response_bytes
          iv_batch_size     = lines( it_batch )
          iv_splits_used    = iv_splits_used ).

        IF lv_action = cs_oversize_action-raise.
          zcx_abapgit_ortec_git=>raise(
            |Materialize: batch response ({ ev_response_bytes } bytes) | &&
            |exceeds the { c_max_batch_response_bytes } byte ceiling | &&
            |and cannot be split further| ).
        ENDIF.

        IF lv_action = cs_oversize_action-split.

          split_batch_in_half(
            EXPORTING
              it_batch       = it_batch
            IMPORTING
              et_first_half  = lt_first_half
              et_second_half = lt_second_half ).

          CLEAR:
            lv_child_bytes,
            lv_child_split.

          materialize_batch(
            EXPORTING
              iv_url           = iv_url
              iv_repo_key      = iv_repo_key
              it_batch         = lt_first_half
              iv_server_caps   = iv_server_caps
              io_client        = io_client
              iv_splits_used   = iv_splits_used + 1
            IMPORTING
              ev_response_bytes = lv_child_bytes
              ev_split_used     = lv_child_split ).

          CLEAR:
            lv_child_bytes,
            lv_child_split.

          materialize_batch(
            EXPORTING
              iv_url           = iv_url
              iv_repo_key      = iv_repo_key
              it_batch         = lt_second_half
              iv_server_caps   = iv_server_caps
              io_client        = io_client
              iv_splits_used   = iv_splits_used + 1
            IMPORTING
              ev_response_bytes = lv_child_bytes
              ev_split_used     = lv_child_split ).

          " The original top-level batch required splitting. Its oversized
          " response must not be used to grow the following top-level batch.
          ev_split_used = abap_true.
          CLEAR ev_response_bytes.
          RETURN.

        ENDIF.

        zcl_abapgit_ortec_fastpath=>parse(
          IMPORTING
            ev_pack      = lv_pack
            et_shallow   = lt_shallow
            et_unshallow = lt_unshallow
          CHANGING
            cv_data      = lv_response ).

      CATCH zcx_abapgit_exception INTO DATA(lx_error).

        RAISE EXCEPTION NEW zcx_abapgit_ortec_git( iv_text  =
                                                              |Materialize batch fetch failed: { lx_error->get_text( ) }|
                                                   previous = lx_error ).

    ENDTRY.

    IF lv_pack IS INITIAL
        OR zcl_abapgit_ortec_pack_dec=>peek_object_count( lv_pack ) = 0.
      zcx_abapgit_ortec_git=>raise(
        'Materialize batch fetch returned no pack data' ).
    ENDIF.

    zcl_abapgit_ortec_pack_stream=>decode_streaming(
      iv_data     = lv_pack
      iv_repo_key = iv_repo_key
      iv_url      = iv_url ).

    " No per-batch payload verification. The complete selected-tip blob set
    " is verified exactly once after every adaptive/split batch succeeds.
    ev_split_used = abap_false.

  ENDMETHOD.

  METHOD verify_batch_objects.

    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.

    FIELD-SYMBOLS <ls_object> LIKE LINE OF lt_objects.

    IF it_batch IS INITIAL.
      RETURN.
    ENDIF.

    " get_objects raises zcx_abapgit_ortec_git if ANY requested SHA1 is not
    " present with status 'R' - covers both "server never sent it" and
    " "content hashed to a different real SHA1" identically, since both
    " manifest as "the wanted SHA1 is absent" (design §11 step 4e, INV-B-09).
    lt_objects = zcl_abapgit_ortec_obj_store=>get_objects(
        iv_repo_key   = iv_repo_key
        it_sha1s      = it_batch
        iv_bulk_fetch = abap_false ).

    LOOP AT lt_objects ASSIGNING <ls_object>.
      IF <ls_object>-type <> zif_abapgit_git_definitions=>c_type-blob.
        zcx_abapgit_ortec_git=>raise(
          |Materialize: object { <ls_object>-sha1 } expected type blob, got { <ls_object>-type }| ).
      ENDIF.
    ENDLOOP.

  ENDMETHOD.


  METHOD may_publish_snapshot.
    rv_yes = boolc( it_still_missing IS INITIAL ).
  ENDMETHOD.

  METHOD finalize_snapshot.

    zcl_abapgit_ortec_mat_state=>mark_full_complete(
      iv_repo_key   = iv_repo_key
      iv_commit     = iv_tip_commit
      iv_attempt_id = iv_attempt_id ).

    zcl_abapgit_ortec_repo_state=>prepare_full_snapshot(
      iv_repo_key    = iv_repo_key
      iv_branch_name = iv_branch_name
      iv_url         = iv_url
      iv_commit      = iv_tip_commit ).

    zcl_abapgit_ortec_mat_state=>publish_snapshot_complete(
      iv_repo_key    = iv_repo_key
      iv_branch_name = iv_branch_name
      iv_commit      = iv_tip_commit
      iv_attempt_id  = iv_attempt_id ).

  ENDMETHOD.

  METHOD init_materialize_client.

    DATA lt_headers TYPE zcl_abapgit_http=>ty_headers.
    DATA ls_header LIKE LINE OF lt_headers.
    DATA lv_ref_data TYPE string.

    CLEAR:
      eo_client,
      ev_server_caps.

    TRY.

        ls_header-key = '~request_uri'.
        ls_header-value =
          |{ zcl_abapgit_url=>path_name( iv_url ) }/info/refs?service=git-upload-pack|.
        APPEND ls_header TO lt_headers.

        " CREATE_BY_URL immediately executes the INFO/REFS GET and handles:
        " - stored Basic authorization;
        " - a possible HTTP 401 challenge;
        " - Basic or Digest authentication;
        " - proxy and exit configuration;
        " - HTTP status validation.
        "
        " This exact client remains open and executes every following
        " upload-pack POST belonging to this materialization operation.
        eo_client = zcl_abapgit_http=>create_by_url(
          iv_url     = iv_url
          it_headers = lt_headers ).

        lv_ref_data = eo_client->get_cdata( ).

        ev_server_caps =
          zcl_abapgit_ortec_fetch_req=>parse_capabilities(
            lv_ref_data ).

      CATCH zcx_abapgit_exception INTO DATA(lx_error).

        IF eo_client IS BOUND.
          eo_client->close( ).
          CLEAR eo_client.
        ENDIF.

        RAISE EXCEPTION NEW zcx_abapgit_ortec_git( iv_text  =
                                                              |Materialize client initialization failed: { lx_error->get_text( ) }|
                                                   previous = lx_error ).

    ENDTRY.

  ENDMETHOD.


  METHOD deduplicate_sha1s.

    DATA lt_seen TYPE HASHED TABLE OF
      zif_abapgit_git_definitions=>ty_sha1
      WITH UNIQUE KEY table_line.

    FIELD-SYMBOLS <lv_sha1> LIKE LINE OF it_sha1s.

    LOOP AT it_sha1s ASSIGNING <lv_sha1>
         WHERE table_line IS NOT INITIAL.

      INSERT <lv_sha1> INTO TABLE lt_seen.
      IF sy-subrc = 0.
        APPEND <lv_sha1> TO rt_sha1s.
      ENDIF.

    ENDLOOP.

  ENDMETHOD.


  METHOD take_next_batch.

    DATA lv_index TYPE i.
    DATA lv_added TYPE i.

    CLEAR et_batch.

    lv_index = iv_start_index.
    IF lv_index < 1.
      lv_index = 1.
    ENDIF.

    WHILE lv_index <= lines( it_sha1s )
      AND lv_added < iv_max_rows.

      READ TABLE it_sha1s INDEX lv_index INTO DATA(lv_sha1).
      IF sy-subrc <> 0.
        EXIT.
      ENDIF.

      IF lv_sha1 IS NOT INITIAL.
        APPEND lv_sha1 TO et_batch.
        lv_added = lv_added + 1.
      ENDIF.

      lv_index = lv_index + 1.

    ENDWHILE.

    ev_next_index = lv_index.

  ENDMETHOD.


  METHOD calculate_next_batch_size.

    DATA lv_scaled TYPE int8.
    DATA lv_growth_limit TYPE i.
    DATA lv_next TYPE i.

    IF iv_current_rows <= 0.
      rv_next_rows = c_batch_rows_initial.
      RETURN.
    ENDIF.

    IF iv_split_used = abap_true.
      lv_next = iv_current_rows DIV 2.

    ELSEIF iv_response_bytes > 0.
      lv_scaled =
          CONV int8( iv_current_rows )
        * CONV int8( c_target_response_bytes )
        / CONV int8( iv_response_bytes ).

      lv_next = CONV i( lv_scaled ).

      lv_growth_limit = iv_current_rows * c_max_batch_growth.
      IF lv_next > lv_growth_limit.
        lv_next = lv_growth_limit.
      ENDIF.

    ELSE.
      lv_next = iv_current_rows.
    ENDIF.

    IF lv_next < c_batch_rows_min.
      lv_next = c_batch_rows_min.
    ELSEIF lv_next > c_batch_rows_max.
      lv_next = c_batch_rows_max.
    ENDIF.

    IF lv_next >
         zcl_abapgit_ortec_fetch_req=>c_materialize_batch_max.
      lv_next =
        zcl_abapgit_ortec_fetch_req=>c_materialize_batch_max.
    ENDIF.

    rv_next_rows = lv_next.

  ENDMETHOD.

ENDCLASS.
