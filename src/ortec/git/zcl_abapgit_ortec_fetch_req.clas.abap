"! <p class="shorttext synchronized">ORTEC Git Explicit Fetch Modes And Request Serializer</p>
"! Owns the Variant B explicit fetch-mode taxonomy (INCREMENTAL_THIN /
"! INCREMENTAL_SELF_CONTAINED / INITIAL_BRANCH_BLOBLESS / MATERIALIZE_BLOBS /
"! RECOVERY_BRANCH_FULL) and one pure request serializer that turns a mode
"! plus already-resolved want/have SHA1 lists and an already-parsed
"! capability string into a ready-to-send pkt-line upload-pack request body.
"! BUILD_REQUEST and PARSE_CAPABILITIES perform ZERO SQL and ZERO HTTP calls
"! - certification of which SHA1s are eligible haves and discovery of what
"! the server advertises are the caller's responsibility, done via separate
"! primitives (ZCL_ABAPGIT_ORTEC_FETCH_NEG=>GET_VERIFIED_HAVE_COMMITS, this
"! class's own PARSE_CAPABILITIES). Replaces the interacting
"! IV_ALLOW_THIN/IV_FORCE_FULL boolean pair and progressive-deepen recovery
"! with explicit, non-overlapping modes - none of the five modes' decision
"! branches ever emit a `deepen` or `shallow` token. This class does not
"! call any live ORTEC transport/orchestration method (no productive call
"! site is migrated to it yet - see Slice 2 scope boundary).
CLASS zcl_abapgit_ortec_fetch_req DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    TYPES ty_fetch_mode TYPE c LENGTH 1.

    CONSTANTS: BEGIN OF cs_fetch_mode,
                 incremental_thin           TYPE ty_fetch_mode VALUE 'T',
                 incremental_self_contained TYPE ty_fetch_mode VALUE 'S',
                 initial_branch_blobless    TYPE ty_fetch_mode VALUE 'B',
                 materialize_blobs          TYPE ty_fetch_mode VALUE 'M',
                 recovery_branch_full       TYPE ty_fetch_mode VALUE 'R',
               END OF cs_fetch_mode.

    "! Absolute wire-level maximum for MATERIALIZE_BLOBS wants.2"! The cold-init orchestrator starts
    "! below this limit and adapts subsequent3"! batch sizes from the measured response size.
    "! This constant remains the4"! hard serializer guard and must never be exceeded by a single request.
    CONSTANTS c_materialize_batch_max TYPE i VALUE 1000.

    TYPES: BEGIN OF ty_request,
             mode        TYPE ty_fetch_mode,
             buffer      TYPE string,
             used_thin   TYPE abap_bool,
             used_filter TYPE string,
             have_count  TYPE i,
           END OF ty_request.

    "! Pure request serializer - zero SQL, zero HTTP. See class doc.
    "! @parameter iv_mode |
    "! One of CS_FETCH_MODE
    "! @parameter it_want_hashes |
    "! Target(s) to want. Exactly one entry for every mode except
    "! MATERIALIZE_BLOBS, which accepts 1..C_MATERIALIZE_BATCH_MAX
    "! @parameter it_certified_haves |
    "! Already-certified have SHA1s (see
    "! ZCL_ABAPGIT_ORTEC_FETCH_NEG=>GET_VERIFIED_HAVE_COMMITS). Ignored for
    "! modes that never negotiate haves
    "! @parameter iv_server_caps |
    "! Raw advertised capability line, e.g. from PARSE_CAPABILITIES
    "! @parameter rs_request |
    "! Assembled request
    "! @raising zcx_abapgit_ortec_git |
    "! Invalid want count for the mode, MATERIALIZE_BLOBS batch too large,
    "! or a hard-required capability is not advertised
    "! (MV_UNSUPPORTED_CAPABILITY = ABAP_TRUE)
    CLASS-METHODS build_request
      IMPORTING
        iv_mode            TYPE ty_fetch_mode
        it_want_hashes     TYPE zif_abapgit_git_definitions=>ty_sha1_tt
        it_certified_haves TYPE zif_abapgit_git_definitions=>ty_sha1_tt OPTIONAL
        iv_server_caps     TYPE string OPTIONAL
      RETURNING
        VALUE(rs_request)  TYPE ty_request
      RAISING
        zcx_abapgit_ortec_git.

    "! Extracts the capability substring (post-NUL, pre-newline) from a raw
    "! v1 ref-advertisement payload. Single implementation replacing the
    "! identical logic previously duplicated in
    "! ZCL_ABAPGIT_ORTEC_FASTPATH=>FETCH_TIP_COMMITS and
    "! =>TRY_FILTERED_COMMIT_FETCH (neither call site is migrated to call
    "! this yet - see Slice 2 scope boundary).
    "! @parameter iv_ref_data |
    "! Raw v1 ref advertisement (as returned by
    "! ZCL_ABAPGIT_HTTP_CLIENT=>GET_CDATA)
    "! @parameter rv_caps |
    "! Capability line, or empty if no NUL byte was found
    CLASS-METHODS parse_capabilities
      IMPORTING
        iv_ref_data    TYPE string
      RETURNING
        VALUE(rv_caps) TYPE string.

  PRIVATE SECTION.
    CONSTANTS c_capa_base           TYPE string VALUE 'side-band-64k no-progress multi_ack'.
    CONSTANTS c_cap_filter          TYPE string VALUE 'filter'.
    CONSTANTS c_cap_thin            TYPE string VALUE 'thin-pack'.
    CONSTANTS c_cap_ofs_delta       TYPE string VALUE 'ofs-delta'.
    CONSTANTS c_cap_reachable_want  TYPE string VALUE 'allow-reachable-sha1-in-want'.
    CONSTANTS c_cap_tip_want        TYPE string VALUE 'allow-tip-sha1-in-want'.

    CLASS-METHODS validate_single_want
      IMPORTING
        iv_mode        TYPE ty_fetch_mode
        it_want_hashes TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RAISING
        zcx_abapgit_ortec_git.

    CLASS-METHODS build_want_lines
      IMPORTING
        it_want_hashes  TYPE zif_abapgit_git_definitions=>ty_sha1_tt
        iv_first_capa   TYPE string
      RETURNING
        VALUE(rv_lines) TYPE string
      RAISING
        zcx_abapgit_exception.

    CLASS-METHODS build_have_lines
      IMPORTING
        it_certified_haves TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RETURNING
        VALUE(rv_lines)    TYPE string
      RAISING
        zcx_abapgit_exception.

ENDCLASS.


CLASS zcl_abapgit_ortec_fetch_req IMPLEMENTATION.

  METHOD build_request.

    DATA lv_want_capa          TYPE string.
    DATA lv_matched_capability TYPE string.

    TRY.
        CASE iv_mode.

          WHEN cs_fetch_mode-initial_branch_blobless.

            validate_single_want( iv_mode = iv_mode it_want_hashes = it_want_hashes ).

            IF NOT ( iv_server_caps CS c_cap_filter ).
              zcx_abapgit_ortec_git=>raise_unsupported_capability(
                iv_mode       = iv_mode
                iv_capability = c_cap_filter ).
            ENDIF.

            lv_want_capa = c_capa_base && ` ` && c_cap_filter.

            rs_request-buffer = build_want_lines( it_want_hashes = it_want_hashes
                                                   iv_first_capa  = lv_want_capa ).
            rs_request-buffer = rs_request-buffer &&
              zcl_abapgit_git_utils=>pkt_string( |filter blob:none{ cl_abap_char_utilities=>newline }| ).
            rs_request-buffer = rs_request-buffer && '0000'.
            rs_request-buffer = rs_request-buffer && '0009done' && cl_abap_char_utilities=>newline.

            rs_request-used_filter = 'blob:none'.
            rs_request-have_count  = 0.

          WHEN cs_fetch_mode-incremental_thin.

            validate_single_want( iv_mode = iv_mode it_want_hashes = it_want_hashes ).

            rs_request-used_thin = xsdbool( it_certified_haves IS NOT INITIAL
              AND iv_server_caps CS c_cap_thin
              AND iv_server_caps CS c_cap_ofs_delta ).

            IF rs_request-used_thin = abap_true.
              lv_want_capa = c_capa_base && ` ` && c_cap_thin && ` ` && c_cap_ofs_delta.
            ELSE.
              lv_want_capa = c_capa_base.
            ENDIF.

            rs_request-buffer = build_want_lines( it_want_hashes = it_want_hashes
                                                   iv_first_capa  = lv_want_capa ).
            rs_request-buffer = rs_request-buffer && '0000'.
            rs_request-buffer = rs_request-buffer && build_have_lines( it_certified_haves ).
            rs_request-buffer = rs_request-buffer && '0009done' && cl_abap_char_utilities=>newline.

            rs_request-have_count = lines( it_certified_haves ).

          WHEN cs_fetch_mode-incremental_self_contained.

            validate_single_want( iv_mode = iv_mode it_want_hashes = it_want_hashes ).

            rs_request-buffer = build_want_lines( it_want_hashes = it_want_hashes
                                                   iv_first_capa  = c_capa_base ).
            rs_request-buffer = rs_request-buffer && '0000'.
            rs_request-buffer = rs_request-buffer && build_have_lines( it_certified_haves ).
            rs_request-buffer = rs_request-buffer && '0009done' && cl_abap_char_utilities=>newline.

            rs_request-have_count = lines( it_certified_haves ).

          WHEN cs_fetch_mode-materialize_blobs.

            IF it_want_hashes IS INITIAL.
              zcx_abapgit_ortec_git=>raise( |MATERIALIZE_BLOBS requires at least one blob SHA1| ).
            ENDIF.
            IF lines( it_want_hashes ) > c_materialize_batch_max.
              zcx_abapgit_ortec_git=>raise(
                |MATERIALIZE_BLOBS batch size { lines( it_want_hashes ) } exceeds maximum { c_materialize_batch_max }| ).
            ENDIF.

            IF iv_server_caps CS c_cap_reachable_want.
              lv_matched_capability = c_cap_reachable_want.
            ELSEIF iv_server_caps CS c_cap_tip_want.
              lv_matched_capability = c_cap_tip_want.
            ELSE.
              zcx_abapgit_ortec_git=>raise_unsupported_capability(
                iv_mode       = iv_mode
                iv_capability = c_cap_reachable_want ).
            ENDIF.

            lv_want_capa = c_capa_base && ` ` && lv_matched_capability.

            rs_request-buffer = build_want_lines( it_want_hashes = it_want_hashes
                                                   iv_first_capa  = lv_want_capa ).
            rs_request-buffer = rs_request-buffer && '0000'.
            rs_request-buffer = rs_request-buffer && '0009done' && cl_abap_char_utilities=>newline.

            rs_request-have_count = 0.

          WHEN cs_fetch_mode-recovery_branch_full.

            validate_single_want( iv_mode = iv_mode it_want_hashes = it_want_hashes ).

            rs_request-buffer = build_want_lines( it_want_hashes = it_want_hashes
                                                   iv_first_capa  = c_capa_base ).
            rs_request-buffer = rs_request-buffer && '0000'.
            rs_request-buffer = rs_request-buffer && '0009done' && cl_abap_char_utilities=>newline.

            rs_request-have_count = 0.

          WHEN OTHERS.
            zcx_abapgit_ortec_git=>raise( |Unknown ORTEC fetch mode: { iv_mode }| ).

        ENDCASE.

      CATCH zcx_abapgit_exception INTO DATA(lx_pkt_error).
        " BUILD_REQUEST's approved public contract raises only
        " ZCX_ABAPGIT_ORTEC_GIT (Slice 2 design §2.2) - translate the
        " underlying pkt-line encoding failure (ZCL_ABAPGIT_GIT_UTILS=>
        " PKT_STRING, called directly and via BUILD_WANT_LINES/
        " BUILD_HAVE_LINES) rather than exposing the base exception,
        " preserving it as the previous cause.
        RAISE EXCEPTION TYPE zcx_abapgit_ortec_git
          EXPORTING
            iv_text  = |ORTEC fetch request serialization failed: { lx_pkt_error->get_text( ) }|
            previous = lx_pkt_error.
    ENDTRY.

    rs_request-mode = iv_mode.

  ENDMETHOD.


  METHOD parse_capabilities.

    DATA lv_null     TYPE c LENGTH 1.
    DATA lv_null_pos TYPE i.
    DATA lv_nl_pos   TYPE i.
    DATA lv_offset   TYPE i.
    DATA lv_caps     TYPE string.

    lv_null = zcl_abapgit_git_utils=>get_null( ).
    FIND FIRST OCCURRENCE OF lv_null IN iv_ref_data MATCH OFFSET lv_null_pos.
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    TRY.
        lv_offset = lv_null_pos + 1.
        lv_caps = iv_ref_data+lv_offset.
        FIND FIRST OCCURRENCE OF cl_abap_char_utilities=>newline IN lv_caps
          MATCH OFFSET lv_nl_pos.
        IF sy-subrc = 0 AND lv_nl_pos > 0.
          lv_caps = lv_caps(lv_nl_pos).
        ENDIF.
        rv_caps = lv_caps.
      CATCH cx_sy_range_out_of_bounds.
        CLEAR rv_caps.
    ENDTRY.

  ENDMETHOD.


  METHOD validate_single_want.
    IF lines( it_want_hashes ) <> 1.
      zcx_abapgit_ortec_git=>raise(
        |ORTEC fetch mode { iv_mode } requires exactly one want, got { lines( it_want_hashes ) }| ).
    ENDIF.
  ENDMETHOD.


  METHOD build_want_lines.

    DATA lv_line TYPE string.
    FIELD-SYMBOLS <lv_sha1> LIKE LINE OF it_want_hashes.

    LOOP AT it_want_hashes FROM 1 ASSIGNING <lv_sha1>.
      IF sy-tabix = 1.
        lv_line = |want { <lv_sha1> } { iv_first_capa }{ cl_abap_char_utilities=>newline }|.
      ELSE.
        lv_line = |want { <lv_sha1> }{ cl_abap_char_utilities=>newline }|.
      ENDIF.
      rv_lines = rv_lines && zcl_abapgit_git_utils=>pkt_string( lv_line ).
    ENDLOOP.

  ENDMETHOD.


  METHOD build_have_lines.

    FIELD-SYMBOLS <lv_have> LIKE LINE OF it_certified_haves.

    LOOP AT it_certified_haves ASSIGNING <lv_have>.
      rv_lines = rv_lines && zcl_abapgit_git_utils=>pkt_string(
        |have { <lv_have> }{ cl_abap_char_utilities=>newline }| ).
    ENDLOOP.

  ENDMETHOD.

ENDCLASS.
