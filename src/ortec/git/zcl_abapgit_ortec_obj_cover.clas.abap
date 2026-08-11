"! <p class="shorttext synchronized">ORTEC Git Partial Filter Coverage</p>
"! Persists per-object resolution facts (ZAOG_OBJ_COVER) for FILTERED-mode
"! demand-driven index reads, keyed by a context identity hash that bundles
"! every input the object-to-file mapping is sensitive to (namespace/folder
"! logic, .abapgit config, devclass, algorithm version). See
"! .memory/logs/obj_index_partial_design.md §3/§3.1/§3.2.
CLASS zcl_abapgit_ortec_obj_cover DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    TYPES:
      BEGIN OF ty_coverage,
        obj_type          TYPE zaog_obj_cover-obj_type,
        obj_name          TYPE zaog_obj_cover-obj_name,
        resolution_status TYPE zaog_obj_cover-resolution_status,
        walk_hist_level   TYPE zaog_obj_cover-walk_hist_level,
        resolved_at       TYPE zaog_obj_cover-resolved_at,
      END OF ty_coverage,
      ty_coverage_tt TYPE STANDARD TABLE OF ty_coverage WITH DEFAULT KEY.

    TYPES:
      BEGIN OF ty_cover_diagnostics,
        failure_count TYPE i,
        last_error    TYPE string,
      END OF ty_cover_diagnostics.

    " Resolution-state vocabulary (design doc §4). FOUND/RESOLVED_NO_FILES/
    " RESOLVED_NOT_PRESENT_REMOTE/UNRESOLVED_MISSING_LOCAL_DATA are written
    " by this design; UNRESOLVED_AMBIGUOUS_MAPPING remains reserved (open
    " question 1) and is not written by any method in this class.
    CONSTANTS:
      BEGIN OF cs_resolution,
        found                          TYPE c LENGTH 1 VALUE 'F',
        resolved_no_files              TYPE c LENGTH 1 VALUE 'N',
        resolved_not_present_remote    TYPE c LENGTH 1 VALUE 'D',
        unresolved_missing_local_data  TYPE c LENGTH 1 VALUE 'M',
        unresolved_ambiguous_mapping   TYPE c LENGTH 1 VALUE 'A',
      END OF cs_resolution.

    " Shared by this class' own chunked reads/writes and by
    " zcl_abapgit_ortec_obj_index's context-aware SELECTs (design doc §3.2,
    " AR-1-04) - every SQL statement in this program keyed by a
    " caller-supplied filter chunks at this one named constant. Deliberately
    " NOT c_index_write_chunk_size (30000, COMPLETE-mode F-row case) - this
    " value governs the FILTERED-mode/demand-driven K-row case only.
    CONSTANTS c_filter_chunk_size TYPE i VALUE 5000.

    " How long an UNRESOLVED_MISSING_LOCAL_DATA ('M') row suppresses a
    " repeat walk attempt for the same object before it is treated as plain
    " "uncovered" again (design doc §4.1). Consulted by the Slice 3
    " ensure_filtered_coverage caller - not read by any method in this
    " class itself.
    CONSTANTS c_missing_data_backoff_seconds TYPE i VALUE 300.

    "! Compute the deterministic context identity hash for a resolution
    "! request. Bundles every input file_to_object is sensitive to
    "! (namespace/folder logic via the full serialized .abapgit config,
    "! devclass, algorithm version) so a config/devclass/algorithm change
    "! never silently reuses a stale answer - it simply produces a
    "! different hash, which reads as "no coverage row" rather than a
    "! wrong answer.
    "! @parameter iv_devclass |
    "! Repository package
    "! @parameter io_dot |
    "! Parsed .abapgit configuration
    "! @parameter rv_hash |
    "! Deterministic SHA1 hex context identity
    "! @raising zcx_abapgit_exception |
    "! Propagated from io_dot->serialize( ) on a malformed .abapgit
    CLASS-METHODS compute_context_hash
      IMPORTING
        iv_devclass    TYPE devclass
        io_dot         TYPE REF TO zcl_abapgit_dot_abapgit
      RETURNING
        VALUE(rv_hash) TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING
        zcx_abapgit_exception.

    "! Read previously-persisted resolution facts for a filter set, scoped
    "! to an exact context. A row written under a different context is
    "! invisible (structural, not a special case).
    "! @parameter iv_repo_key |
    "! ORTEC repository key
    "! @parameter iv_commit |
    "! Commit SHA1
    "! @parameter iv_context_hash |
    "! Resolution context identity hash
    "! @parameter it_filter |
    "! Stage object filter (TADIR-like list)
    "! @parameter rt_coverage |
    "! Coverage rows found for the requested filter objects under this
    "! context (fewer or equal rows than it_filter; no row means
    "! "uncovered", never an error)
    CLASS-METHODS get_coverage
      IMPORTING
        iv_repo_key      TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
        iv_commit        TYPE zif_abapgit_git_definitions=>ty_sha1
        iv_context_hash  TYPE zif_abapgit_git_definitions=>ty_sha1
        it_filter        TYPE zif_abapgit_definitions=>ty_tadir_tt
      RETURNING
        VALUE(rt_coverage) TYPE ty_coverage_tt.

    "! Upsert resolution facts for a set of objects under one context.
    "! Never fatal to the caller's own in-flight request - a failure is
    "! recorded via the diagnosable counter (get_diagnostics) and re-raised
    "! for the caller to decide whether to swallow it.
    "! @parameter iv_repo_key |
    "! ORTEC repository key
    "! @parameter iv_commit |
    "! Commit SHA1
    "! @parameter iv_context_hash |
    "! Resolution context identity hash
    "! @parameter iv_walk_hist_level |
    "! Materialization history level recorded alongside each row
    "! @parameter it_results |
    "! Resolution facts to persist (RESOLVED_AT/ALGO_VERSION are always
    "! stamped as "now"/the current algorithm version by this method,
    "! never taken from the caller)
    "! @raising zcx_abapgit_ortec_git |
    "! Raised on a MODIFY failure; gv_write_coverage_failures is
    "! incremented before this is raised
    CLASS-METHODS write_coverage
      IMPORTING
        iv_repo_key        TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
        iv_commit          TYPE zif_abapgit_git_definitions=>ty_sha1
        iv_context_hash    TYPE zif_abapgit_git_definitions=>ty_sha1
        iv_walk_hist_level TYPE zcl_abapgit_ortec_mat_state=>ty_hist_level
        it_results         TYPE ty_coverage_tt
      RAISING
        zcx_abapgit_ortec_git.

    "! Diagnosable, non-fatal write_coverage failure signal (AR-1-08).
    "! Read-only, no reset - a future admin surface can report it.
    "! @parameter rs_diagnostics |
    "! Cumulative failure count and last error text
    CLASS-METHODS get_diagnostics
      RETURNING
        VALUE(rs_diagnostics) TYPE ty_cover_diagnostics.

  PRIVATE SECTION.
    " Embedded as a fixed-length prefix of compute_context_hash's hash
    " input, so a future algorithm change (e.g. a fix to file_to_object's
    " own mapping logic) can force every existing coverage row to become
    " unreachable ("stale context") without any explicit migration step.
    CONSTANTS c_algo_version TYPE c LENGTH 4 VALUE '0001'.

    CLASS-DATA gv_write_coverage_failures    TYPE i.
    CLASS-DATA gv_last_write_coverage_error  TYPE string.
ENDCLASS.


CLASS zcl_abapgit_ortec_obj_cover IMPLEMENTATION.

  METHOD compute_context_hash.
    DATA lv_prefix_string TYPE string.
    DATA lv_prefix_xstr   TYPE xstring.
    DATA lv_dot_xstr      TYPE xstring.

    " Fixed-length ALGO_VERSION prefix makes the algo_version/devclass
    " boundary unambiguous without a separator character.
    lv_prefix_string = |{ c_algo_version }{ iv_devclass }|.
    lv_prefix_xstr = zcl_abapgit_convert=>string_to_xstring_utf8( lv_prefix_string ).

    " Covers folder logic, starting folder, ignore list, i18n languages,
    " everything file_to_object can be sensitive to, without hand-picking
    " individual getters.
    lv_dot_xstr = io_dot->serialize( ).

    rv_hash = zcl_abapgit_hash=>sha1_raw( lv_prefix_xstr && lv_dot_xstr ).
  ENDMETHOD.


  METHOD get_coverage.
    DATA lt_chunk TYPE zif_abapgit_definitions=>ty_tadir_tt.

    FIELD-SYMBOLS <ls_filter> TYPE zif_abapgit_definitions=>ty_tadir.

    IF it_filter IS INITIAL.
      RETURN.
    ENDIF.

    LOOP AT it_filter ASSIGNING <ls_filter>.
      APPEND <ls_filter> TO lt_chunk.

      IF lines( lt_chunk ) >= c_filter_chunk_size.
        SELECT obj_type obj_name resolution_status walk_hist_level resolved_at
          FROM zaog_obj_cover
          APPENDING TABLE rt_coverage
          FOR ALL ENTRIES IN lt_chunk
          WHERE repo_key     = iv_repo_key
            AND commit_sha1  = iv_commit
            AND obj_type     = lt_chunk-object
            AND obj_name     = lt_chunk-obj_name
            AND context_hash = iv_context_hash.
        CLEAR lt_chunk.
      ENDIF.
    ENDLOOP.

    IF lt_chunk IS NOT INITIAL.
      SELECT obj_type obj_name resolution_status walk_hist_level resolved_at
        FROM zaog_obj_cover
        APPENDING TABLE rt_coverage
        FOR ALL ENTRIES IN lt_chunk
        WHERE repo_key     = iv_repo_key
          AND commit_sha1  = iv_commit
          AND obj_type     = lt_chunk-object
          AND obj_name     = lt_chunk-obj_name
          AND context_hash = iv_context_hash.
    ENDIF.
  ENDMETHOD.


  METHOD write_coverage.
    DATA lt_rows TYPE STANDARD TABLE OF zaog_obj_cover WITH DEFAULT KEY.
    DATA ls_row  TYPE zaog_obj_cover.
    DATA lv_now  TYPE timestampl.

    FIELD-SYMBOLS <ls_result> TYPE ty_coverage.

    IF it_results IS INITIAL.
      RETURN.
    ENDIF.

    GET TIME STAMP FIELD lv_now.

    LOOP AT it_results ASSIGNING <ls_result>.
      CLEAR ls_row.
      ls_row-repo_key          = iv_repo_key.
      ls_row-commit_sha1       = iv_commit.
      ls_row-obj_type          = <ls_result>-obj_type.
      ls_row-obj_name          = <ls_result>-obj_name.
      ls_row-context_hash      = iv_context_hash.
      ls_row-resolution_status = <ls_result>-resolution_status.
      ls_row-algo_version      = c_algo_version.
      ls_row-walk_hist_level   = iv_walk_hist_level.
      ls_row-resolved_at       = lv_now.
      APPEND ls_row TO lt_rows.

      IF lines( lt_rows ) >= c_filter_chunk_size.
        MODIFY zaog_obj_cover FROM TABLE lt_rows.
        IF sy-subrc <> 0.
          ADD 1 TO gv_write_coverage_failures.
          gv_last_write_coverage_error = |MODIFY zaog_obj_cover failed for repo { iv_repo_key }, commit { iv_commit }|.
          zcx_abapgit_ortec_git=>raise( gv_last_write_coverage_error ).
        ENDIF.
        CLEAR lt_rows.
      ENDIF.
    ENDLOOP.

    IF lt_rows IS NOT INITIAL.
      MODIFY zaog_obj_cover FROM TABLE lt_rows.
      IF sy-subrc <> 0.
        ADD 1 TO gv_write_coverage_failures.
        gv_last_write_coverage_error = |MODIFY zaog_obj_cover failed for repo { iv_repo_key }, commit { iv_commit }|.
        zcx_abapgit_ortec_git=>raise( gv_last_write_coverage_error ).
      ENDIF.
    ENDIF.
  ENDMETHOD.


  METHOD get_diagnostics.
    rs_diagnostics-failure_count = gv_write_coverage_failures.
    rs_diagnostics-last_error    = gv_last_write_coverage_error.
  ENDMETHOD.

ENDCLASS.
