"! <p class="shorttext synchronized">ORTEC Git Object Filter Index</p>
"! Persists per-commit object-to-file mappings in ZAOG_OBJ_INDEX and
"! resolves filtered remote files without loading all repository objects.
CLASS zcl_abapgit_ortec_obj_index DEFINITION
  PUBLIC
  FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.
    "! Build/read commit index and return filtered remote files.
    "! The method retries once after automatic index rebuild when stale rows are detected.
    "! @parameter iv_repo_key |
    "! ORTEC repository key
    "! @parameter iv_commit |
    "! Commit SHA1
    "! @parameter ii_obj_filter |
    "! Stage object filter (TADIR-like list)
    "! @parameter io_dot |
    "! Parsed .abapgit configuration
    "! @parameter iv_devclass |
    "! Repository package
    "! @parameter iv_url |
    "! Remote URL. Optional; when supplied and the ORTEC write/protocol opt-in is
    "! active for it, a missing blob triggers one targeted negotiated fetch
    "! before falling back to the full remote read. Pass initial to keep the
    "! prior behavior (fall back immediately on any missing blob).
    "! @parameter iv_current_remote |
    "! Best-effort current remote tip SHA1 (zif_abapgit_repo_online=>get_current_remote),
    "! threaded through to ensure_filtered_coverage/walk_filtered. Optional;
    "! callers without an online repo reference in scope simply omit it.
    "! @parameter rt_files |
    "! Filtered remote files with payload
    "! @raising zcx_abapgit_exception |
    "! Raised on unrecoverable index/object-store errors
    CLASS-METHODS get_files_for_filter
      IMPORTING
        iv_repo_key       TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
        iv_commit         TYPE zif_abapgit_git_definitions=>ty_sha1
        ii_obj_filter     TYPE REF TO zif_abapgit_object_filter
        io_dot            TYPE REF TO zcl_abapgit_dot_abapgit
        iv_devclass       TYPE devclass
        iv_url            TYPE string OPTIONAL
        iv_current_remote TYPE zif_abapgit_git_definitions=>ty_sha1 OPTIONAL
      RETURNING
        VALUE(rt_files) TYPE zif_abapgit_git_definitions=>ty_files_tt
      RAISING
        zcx_abapgit_exception.

    "! Check whether a commit's filtered path index is fully built (STRICT
    "! completeness mode - requires the explicit $IDX/__READY__ marker, see
    "! zcl_abapgit_ortec_git_switch=>cs_absent_strictness). Exposed publicly
    "! so other Ortec components (e.g. the delta-base completeness gate) can
    "! reuse the same completeness signal instead of re-deriving it.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_commit |
    "! Commit SHA1
    "! @parameter iv_context_hash |
    "! Resolution context identity hash (zcl_abapgit_ortec_obj_cover=>compute_context_hash).
    "! A marker/row written under a different (or blank/legacy) context is treated as not ready.
    "! @parameter rv_yes |
    "! ABAP_TRUE if the index for this commit is fully built under this exact context
    CLASS-METHODS is_index_ready
      IMPORTING
        iv_repo_key     TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
        iv_commit       TYPE zif_abapgit_git_definitions=>ty_sha1
        iv_context_hash TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING
        VALUE(rv_yes) TYPE abap_bool.

    "! FILTERED-mode fast-path orchestrator (design doc §11 step 3). If
    "! the COMPLETE-mode index is already ready under this context, reuses
    "! it. Otherwise consults ZAOG_OBJ_COVER: when every requested object
    "! already has a terminal coverage fact (FOUND/RESOLVED_NO_FILES/
    "! RESOLVED_NOT_PRESENT_REMOTE), answers from ZAOG_OBJ_PIDX with zero
    "! tree walk. Any object still uncovered (no row, or only a
    "! non-terminal UNRESOLVED_* row) resolves via walk_filtered, unless
    "! every uncovered object is within the missing-data backoff window
    "! (design doc §4.1), in which case the same exception a real walk
    "! attempt would have raised is raised directly, without walking.
    "! @parameter iv_repo_key |
    "! ORTEC repository key
    "! @parameter iv_commit |
    "! Commit SHA1
    "! @parameter io_dot |
    "! Parsed .abapgit configuration
    "! @parameter iv_devclass |
    "! Repository package
    "! @parameter it_filter |
    "! Stage object filter (TADIR-like list)
    "! @parameter iv_context_hash |
    "! Resolution context identity hash (zcl_abapgit_ortec_obj_cover=>compute_context_hash)
    "! @parameter iv_current_remote |
    "! Best-effort current remote tip SHA1, threaded through to
    "! walk_filtered's RESOLVED_NOT_PRESENT_REMOTE gate (design doc §11.4).
    "! @parameter rt_rows |
    "! Filtered index rows (identical shape whether sourced from
    "! ZAOG_OBJ_INDEX, ZAOG_OBJ_PIDX, or a fresh COMPLETE-mode rebuild)
    "! @raising zcx_abapgit_exception |
    "! Propagated from a COMPLETE-mode rebuild fallback, a FILTERED-mode
    "! walk failure, or the missing-data backoff short-circuit
    CLASS-METHODS ensure_filtered_coverage
      IMPORTING
        iv_repo_key       TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
        iv_commit         TYPE zif_abapgit_git_definitions=>ty_sha1
        io_dot            TYPE REF TO zcl_abapgit_dot_abapgit
        iv_devclass       TYPE devclass
        it_filter         TYPE zif_abapgit_definitions=>ty_tadir_tt
        iv_context_hash   TYPE zif_abapgit_git_definitions=>ty_sha1
        iv_current_remote TYPE zif_abapgit_git_definitions=>ty_sha1 OPTIONAL
      RETURNING
        VALUE(rt_rows) TYPE ty_index_rows_tt
      RAISING
        zcx_abapgit_exception.

  PRIVATE SECTION.
    CONSTANTS c_status_ready TYPE c LENGTH 1 VALUE 'R'.
    CONSTANTS c_marker_obj_type TYPE tadir-object VALUE '$IDX'.
    CONSTANTS c_marker_obj_name TYPE tadir-obj_name VALUE '__READY__'.
    CONSTANTS c_marker_path_hash TYPE zif_abapgit_git_definitions=>ty_sha1
      VALUE '0000000000000000000000000000000000000000'.
    " E1-PERF-A (design doc §2, revised): bulk MODIFY chunk size for
    " rebuild_index's index-row persistence. All zaog_obj_index fields are
    " fixed-width CHAR (~730 bytes/row), so 30000 rows is a bounded
    " ~21.9 MB row-payload buffer (row payload only; see the E1-A
    " performance audit for the full peak-memory risk assessment). Raised
    " from an initial, more conservative 5000-row candidate after owner
    " review of measured DB round-trip/array-DML setup cost at 1000 rows.
    CONSTANTS c_index_write_chunk_size TYPE i VALUE 30000.

    TYPES ty_index_rows_tt TYPE STANDARD TABLE OF zaog_obj_index WITH DEFAULT KEY.

    TYPES:
      BEGIN OF ty_tree_work,
        tree_sha1 TYPE zif_abapgit_git_definitions=>ty_sha1,
        path      TYPE string,
      END OF ty_tree_work,
      ty_tree_work_tt TYPE STANDARD TABLE OF ty_tree_work WITH DEFAULT KEY.

    TYPES:
      BEGIN OF ty_tree_data,
        tree_sha1 TYPE zif_abapgit_git_definitions=>ty_sha1,
        data      TYPE xstring,
      END OF ty_tree_data,
      ty_tree_data_tt TYPE HASHED TABLE OF ty_tree_data WITH UNIQUE KEY tree_sha1.

    TYPES ty_sha1_set TYPE HASHED TABLE OF zif_abapgit_git_definitions=>ty_sha1
      WITH UNIQUE KEY table_line.

    TYPES:
      BEGIN OF ty_blob_data,
        sha1 TYPE zif_abapgit_git_definitions=>ty_sha1,
        data TYPE xstring,
      END OF ty_blob_data,
      ty_blob_data_tt TYPE HASHED TABLE OF ty_blob_data WITH UNIQUE KEY sha1.

    CLASS-METHODS ensure_index
      IMPORTING
        iv_repo_key     TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
        iv_commit       TYPE zif_abapgit_git_definitions=>ty_sha1
        io_dot          TYPE REF TO zcl_abapgit_dot_abapgit
        iv_devclass     TYPE devclass
        iv_context_hash TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING
        zcx_abapgit_exception.

    CLASS-METHODS rebuild_index
      IMPORTING
        iv_repo_key     TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
        iv_commit       TYPE zif_abapgit_git_definitions=>ty_sha1
        io_dot          TYPE REF TO zcl_abapgit_dot_abapgit
        iv_devclass     TYPE devclass
        iv_context_hash TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING
        zcx_abapgit_exception.

    CLASS-METHODS select_rows_for_filter
      IMPORTING
        iv_repo_key     TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
        iv_commit       TYPE zif_abapgit_git_definitions=>ty_sha1
        it_filter       TYPE zif_abapgit_definitions=>ty_tadir_tt
        iv_context_hash TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING
        VALUE(rt_rows) TYPE ty_index_rows_tt.

    "! FILTERED-mode positive-row reader (design doc §3.0b, AR-2-01). Reads
    "! ZAOG_OBJ_PIDX (never ZAOG_OBJ_INDEX) with CONTEXT_HASH as a real key
    "! predicate, so a differently-contexted row can never be mistaken for
    "! this caller's own answer. Wired to ensure_filtered_coverage's warm
    "! "fully covered" fast path (Slice 2).
    CLASS-METHODS select_partial_rows_for_filter
      IMPORTING
        iv_repo_key     TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
        iv_commit       TYPE zif_abapgit_git_definitions=>ty_sha1
        iv_context_hash TYPE zif_abapgit_git_definitions=>ty_sha1
        it_filter       TYPE zif_abapgit_definitions=>ty_tadir_tt
      RETURNING
        VALUE(rt_rows) TYPE ty_index_rows_tt.

    CLASS-METHODS build_files_from_rows
      IMPORTING
        iv_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
        it_rows     TYPE ty_index_rows_tt
        iv_url      TYPE string OPTIONAL
        iv_commit   TYPE zif_abapgit_git_definitions=>ty_sha1 OPTIONAL
      RETURNING
        VALUE(rt_files) TYPE zif_abapgit_git_definitions=>ty_files_tt
      RAISING
        zcx_abapgit_exception.

    "! Atomic cross-table invalidation for one (repo_key, commit) - purges
    "! ZAOG_OBJ_INDEX, ZAOG_OBJ_COVER, and ZAOG_OBJ_PIDX rows for that
    "! commit, across every context (context-blind by design, design doc
    "! §5/§13 W4, AR-1-02/AR-2-01). No RAISING - a plain DELETE cannot
    "! itself fail under normal DB operation. No COMMIT WORK - the
    "! caller's own LUW still owns atomicity/rollback.
    CLASS-METHODS invalidate_commit_index
      IMPORTING
        iv_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
        iv_commit   TYPE zif_abapgit_git_definitions=>ty_sha1.

    "! FILTERED-mode demand-driven walk (design doc §11 step 4, §13
    "! W5/W6). Acquires the same repo lock as rebuild_index, performs the
    "! identical commit->tree BFS, but appends a row to the write buffer
    "! only for objects present in it_filter, writing exclusively to
    "! ZAOG_OBJ_PIDX (never ZAOG_OBJ_INDEX, never the COMPLETE-mode
    "! readiness marker). After a successful walk, writes one coverage
    "! fact per it_filter entry (FOUND/RESOLVED_NO_FILES/
    "! RESOLVED_NOT_PRESENT_REMOTE). On a missing-commit/tree failure,
    "! best-effort writes one UNRESOLVED_MISSING_LOCAL_DATA ('M') row per
    "! it_filter entry before re-raising the original exception unchanged
    "! (design doc §4.1/§6 trigger 2, AR-1-07).
    "! @parameter iv_current_remote |
    "! Best-effort current remote tip SHA1. Required (together with
    "! is_graph_have_eligible) before a zero-match result may be recorded
    "! as the strong RESOLVED_NOT_PRESENT_REMOTE fact (design doc §11.4,
    "! §13 W8, AR-2-02) - otherwise the weaker RESOLVED_NO_FILES is used.
    CLASS-METHODS walk_filtered
      IMPORTING
        iv_repo_key       TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
        iv_commit         TYPE zif_abapgit_git_definitions=>ty_sha1
        io_dot            TYPE REF TO zcl_abapgit_dot_abapgit
        iv_devclass       TYPE devclass
        it_filter         TYPE zif_abapgit_definitions=>ty_tadir_tt
        iv_context_hash   TYPE zif_abapgit_git_definitions=>ty_sha1
        iv_current_remote TYPE zif_abapgit_git_definitions=>ty_sha1 OPTIONAL
      RAISING
        zcx_abapgit_exception.
ENDCLASS.


CLASS zcl_abapgit_ortec_obj_index IMPLEMENTATION.

  METHOD get_files_for_filter.
    DATA lt_filter TYPE zif_abapgit_definitions=>ty_tadir_tt.
    DATA lt_rows   TYPE ty_index_rows_tt.
    DATA lo_filter TYPE REF TO zcl_abapgit_repo_filter.
    DATA lt_devc_paths TYPE SORTED TABLE OF string WITH UNIQUE KEY table_line.
    DATA lv_devc_path  TYPE string.

    FIELD-SYMBOLS <ls_filter> TYPE zif_abapgit_definitions=>ty_tadir.
    FIELD-SYMBOLS <ls_row>    TYPE zaog_obj_index.

    lt_filter = ii_obj_filter->get_filter( ).
    IF lt_filter IS INITIAL.
      RETURN.
    ENDIF.

    " Computed once per request (design doc §11 step 2) - pure, no I/O
    " beyond io_dot->serialize( ) - and threaded to every context-aware
    " method below instead of being recomputed at each layer.
    DATA(lv_context_hash) = zcl_abapgit_ortec_obj_cover=>compute_context_hash(
      iv_devclass = iv_devclass
      io_dot      = io_dot ).

    LOOP AT lt_filter ASSIGNING <ls_filter> WHERE object = 'DEVC'.
      TRY.
          lv_devc_path = zcl_abapgit_folder_logic=>get_instance( )->package_to_path(
            iv_top     = iv_devclass
            io_dot     = io_dot
            iv_package = CONV devclass( <ls_filter>-obj_name ) ).
          INSERT lv_devc_path INTO TABLE lt_devc_paths.
        CATCH zcx_abapgit_exception.
          " Keep DEVC baseline unchanged if package-path mapping is not available.
      ENDTRY.
    ENDLOOP.

    lt_rows = ensure_filtered_coverage(
      iv_repo_key       = iv_repo_key
      iv_commit         = iv_commit
      io_dot            = io_dot
      iv_devclass       = iv_devclass
      it_filter         = lt_filter
      iv_context_hash   = lv_context_hash
      iv_current_remote = iv_current_remote ).

    IF lt_devc_paths IS NOT INITIAL.
      LOOP AT lt_rows ASSIGNING <ls_row> WHERE obj_type = 'DEVC'.
        READ TABLE lt_devc_paths WITH TABLE KEY table_line = <ls_row>-file_path
          TRANSPORTING NO FIELDS.
        IF sy-subrc <> 0.
          DELETE lt_rows INDEX sy-tabix.
        ENDIF.
      ENDLOOP.
    ENDIF.

    IF lt_rows IS INITIAL.
      RETURN.
    ENDIF.

    TRY.
        rt_files = build_files_from_rows(
          iv_repo_key = iv_repo_key
          it_rows     = lt_rows
          iv_url      = iv_url
          iv_commit   = iv_commit ).
      CATCH zcx_abapgit_exception.
        " Index rows can become stale after partial cleanups. Rebuild once and retry.
        invalidate_commit_index(
          iv_repo_key = iv_repo_key
          iv_commit   = iv_commit ).

        ensure_index(
          iv_repo_key     = iv_repo_key
          iv_commit       = iv_commit
          io_dot          = io_dot
          iv_devclass     = iv_devclass
          iv_context_hash = lv_context_hash ).

        lt_rows = select_rows_for_filter(
          iv_repo_key     = iv_repo_key
          iv_commit       = iv_commit
          it_filter       = lt_filter
          iv_context_hash = lv_context_hash ).

        IF lt_devc_paths IS NOT INITIAL.
          LOOP AT lt_rows ASSIGNING <ls_row> WHERE obj_type = 'DEVC'.
            READ TABLE lt_devc_paths WITH TABLE KEY table_line = <ls_row>-file_path
              TRANSPORTING NO FIELDS.
            IF sy-subrc <> 0.
              DELETE lt_rows INDEX sy-tabix.
            ENDIF.
          ENDLOOP.
        ENDIF.

        rt_files = build_files_from_rows(
          iv_repo_key = iv_repo_key
          it_rows     = lt_rows
          iv_url      = iv_url
          iv_commit   = iv_commit ).
    ENDTRY.

    " Keep generated-object handling aligned with standard apply_object_filter logic.
    CREATE OBJECT lo_filter.
    lo_filter->apply_object_filter(
      EXPORTING
        it_filter   = lt_filter
        io_dot      = io_dot
        iv_devclass = iv_devclass
      CHANGING
        ct_files    = rt_files ).
  ENDMETHOD.


  METHOD is_index_ready.
    DATA lv_dummy TYPE zaog_obj_index-path_hash.

    IF zcl_abapgit_ortec_git_switch=>cs_absent_strictness-mode
        = zcl_abapgit_ortec_git_switch=>cs_absent_strictness-mode_relaxed.
      " RELAXED (benchmark-only, see cs_absent_strictness doc): trust the
      " first indexed row found for this commit. Does not distinguish a
      " fully-built index from one interrupted mid-rebuild - never ships as
      " default.
      SELECT SINGLE path_hash FROM zaog_obj_index INTO lv_dummy
        WHERE repo_key    = iv_repo_key
          AND commit_sha1 = iv_commit
          AND idx_status  = c_status_ready
          AND context_hash = iv_context_hash.

      rv_yes = boolc( sy-subrc = 0 ).
      RETURN.
    ENDIF.

    " STRICT (default): only the explicit completion marker proves the
    " walk for this commit's index finished fully - see rebuild_index. A
    " rebuild interrupted partway through (corrupt tree, missing object,
    " decode failure) leaves data rows behind without ever reaching the
    " marker write, so it is correctly detected here as NOT ready and gets
    " rebuilt again, instead of silently serving an incomplete index as if
    " it were CONFIRMED_ABSENT/complete.
    SELECT SINGLE path_hash FROM zaog_obj_index INTO lv_dummy
      WHERE repo_key    = iv_repo_key
        AND commit_sha1 = iv_commit
        AND obj_type    = c_marker_obj_type
        AND obj_name    = c_marker_obj_name
        AND idx_status  = c_status_ready
        AND context_hash = iv_context_hash.

    rv_yes = boolc( sy-subrc = 0 ).
  ENDMETHOD.


  METHOD ensure_index.
    IF is_index_ready(
        iv_repo_key     = iv_repo_key
        iv_commit       = iv_commit
        iv_context_hash = iv_context_hash ) = abap_true.
      RETURN.
    ENDIF.

    rebuild_index(
      iv_repo_key     = iv_repo_key
      iv_commit       = iv_commit
      io_dot          = io_dot
      iv_devclass     = iv_devclass
      iv_context_hash = iv_context_hash ).
  ENDMETHOD.


  METHOD invalidate_commit_index.
    " Context-blind by design (§5, §13 W4) - a commit-scoped invalidation
    " must purge every context's rows, not just one caller's own current
    " context, so a superseded context can never resurface after this
    " purge either. Same LUW as the caller, no COMMIT WORK here.
    DELETE FROM zaog_obj_index
      WHERE repo_key    = iv_repo_key
        AND commit_sha1 = iv_commit.
    DELETE FROM zaog_obj_cover
      WHERE repo_key    = iv_repo_key
        AND commit_sha1 = iv_commit.
    DELETE FROM zaog_obj_pidx
      WHERE repo_key    = iv_repo_key
        AND commit_sha1 = iv_commit.
  ENDMETHOD.


  METHOD ensure_filtered_coverage.
    " design doc §11 step 3 (Slice 3): sub-steps 1-2 (warm-complete/
    " warm-coverage fast paths) plus 3a (backoff short-circuit) and 4/5
    " (walk_filtered wiring) - an incomplete, non-backed-off coverage set
    " now resolves via the FILTERED-mode walk_filtered/ZAOG_OBJ_PIDX path
    " instead of forcing a COMPLETE-mode rebuild.
    DATA lt_coverage TYPE zcl_abapgit_ortec_obj_cover=>ty_coverage_tt.
    DATA lt_found_filter TYPE zif_abapgit_definitions=>ty_tadir_tt.
    DATA lt_uncovered TYPE zif_abapgit_definitions=>ty_tadir_tt.
    DATA lv_all_backed_off TYPE abap_bool.
    DATA lv_now TYPE timestampl.
    DATA lv_backoff_cutoff TYPE timestampl.

    TYPES:
      BEGIN OF ty_cov_lookup,
        obj_type          TYPE zaog_obj_cover-obj_type,
        obj_name          TYPE zaog_obj_cover-obj_name,
        resolution_status TYPE zaog_obj_cover-resolution_status,
        resolved_at       TYPE zaog_obj_cover-resolved_at,
      END OF ty_cov_lookup.
    DATA lt_cov_lookup TYPE HASHED TABLE OF ty_cov_lookup WITH UNIQUE KEY obj_type obj_name.
    DATA ls_cov_lookup TYPE ty_cov_lookup.

    FIELD-SYMBOLS <ls_filter> TYPE zif_abapgit_definitions=>ty_tadir.
    FIELD-SYMBOLS <ls_coverage> TYPE zcl_abapgit_ortec_obj_cover=>ty_coverage.
    FIELD-SYMBOLS <ls_cov_lookup> TYPE ty_cov_lookup.

    " Step 1: today's already-optimal warm-complete path, context-checked.
    IF is_index_ready(
        iv_repo_key     = iv_repo_key
        iv_commit       = iv_commit
        iv_context_hash = iv_context_hash ) = abap_true.
      rt_rows = select_rows_for_filter(
        iv_repo_key     = iv_repo_key
        iv_commit       = iv_commit
        it_filter       = it_filter
        iv_context_hash = iv_context_hash ).
      RETURN.
    ENDIF.

    " Step 2: consult per-object resolution facts. A O(1) hashed lookup
    " keyed by obj_type/obj_name avoids an O(n^2) scan of it_filter x
    " lt_coverage for large filter sets.
    lt_coverage = zcl_abapgit_ortec_obj_cover=>get_coverage(
      iv_repo_key     = iv_repo_key
      iv_commit       = iv_commit
      iv_context_hash = iv_context_hash
      it_filter       = it_filter ).

    LOOP AT lt_coverage ASSIGNING <ls_coverage>.
      CLEAR ls_cov_lookup.
      ls_cov_lookup-obj_type          = <ls_coverage>-obj_type.
      ls_cov_lookup-obj_name          = <ls_coverage>-obj_name.
      ls_cov_lookup-resolution_status = <ls_coverage>-resolution_status.
      ls_cov_lookup-resolved_at       = <ls_coverage>-resolved_at.
      INSERT ls_cov_lookup INTO TABLE lt_cov_lookup.
    ENDLOOP.

    " Step 3: partition it_filter into lt_uncovered (no row at all, or
    " only a non-terminal UNRESOLVED_* row) vs. the covered remainder.
    LOOP AT it_filter ASSIGNING <ls_filter>.
      READ TABLE lt_cov_lookup ASSIGNING <ls_cov_lookup>
        WITH TABLE KEY obj_type = <ls_filter>-object
                        obj_name = <ls_filter>-obj_name.
      IF sy-subrc <> 0
          OR ( <ls_cov_lookup>-resolution_status <> zcl_abapgit_ortec_obj_cover=>cs_resolution-found
           AND <ls_cov_lookup>-resolution_status <> zcl_abapgit_ortec_obj_cover=>cs_resolution-resolved_no_files
           AND <ls_cov_lookup>-resolution_status <> zcl_abapgit_ortec_obj_cover=>cs_resolution-resolved_not_present_remote ).
        APPEND <ls_filter> TO lt_uncovered.
      ENDIF.
    ENDLOOP.

    IF lt_uncovered IS NOT INITIAL.
      " Step 3a (design §4.1, AR-1-07): if EVERY uncovered object is
      " live-backed-off (an unexpired 'M' row from a prior doomed
      " attempt), skip walk_filtered entirely and raise the same
      " exception it would have raised, so the existing outer
      " CATCH zcx_abapgit_exception -> get_files_remote() fallback fires
      " without re-attempting the same doomed walk. A MIXED
      " backed-off/fresh set does NOT take this shortcut (§4.1 explicit
      " scope limit) - falls through to walk_filtered for the full set.
      GET TIME STAMP FIELD lv_now.
      lv_backoff_cutoff = lv_now - zcl_abapgit_ortec_obj_cover=>c_missing_data_backoff_seconds.

      lv_all_backed_off = abap_true.
      LOOP AT lt_uncovered ASSIGNING <ls_filter>.
        READ TABLE lt_cov_lookup ASSIGNING <ls_cov_lookup>
          WITH TABLE KEY obj_type = <ls_filter>-object
                          obj_name = <ls_filter>-obj_name.
        IF sy-subrc <> 0
            OR <ls_cov_lookup>-resolution_status <> zcl_abapgit_ortec_obj_cover=>cs_resolution-unresolved_missing_local_data
            OR <ls_cov_lookup>-resolved_at < lv_backoff_cutoff.
          lv_all_backed_off = abap_false.
          EXIT.
        ENDIF.
      ENDLOOP.

      IF lv_all_backed_off = abap_true.
        zcx_abapgit_exception=>raise(
          |Filtered index: all { lines( lt_uncovered ) } uncovered object(s) for | &&
          |repo { iv_repo_key }, commit { iv_commit } are within the missing-data | &&
          |backoff window - not re-attempting the walk yet| ).
      ENDIF.
    ENDIF.

    IF lt_uncovered IS NOT INITIAL.
      " Step 5: genuinely uncovered (and not all backed off) - resolve via
      " a real FILTERED-mode walk, then re-select what it just wrote.
      " The FULL it_filter is passed (not just lt_uncovered) - walk_filtered
      " performs one single tree walk and writes a definitive coverage fact
      " for every object it was given, superseding any stale/expired 'M'
      " row (design §11 step 5, no split-set special-casing per §4.1).
      walk_filtered(
        iv_repo_key       = iv_repo_key
        iv_commit         = iv_commit
        io_dot            = io_dot
        iv_devclass       = iv_devclass
        it_filter         = it_filter
        iv_context_hash   = iv_context_hash
        iv_current_remote = iv_current_remote ).

      rt_rows = select_partial_rows_for_filter(
        iv_repo_key     = iv_repo_key
        iv_commit       = iv_commit
        iv_context_hash = iv_context_hash
        it_filter       = it_filter ).
      RETURN.
    ENDIF.

    " Fully covered under this context - warm via coverage, zero tree
    " walk. FOUND objects have real rows in ZAOG_OBJ_PIDX; RESOLVED_NO_
    " FILES/RESOLVED_NOT_PRESENT_REMOTE objects contribute zero rows by
    " design (no files to return) and are simply not added to the lookup.
    LOOP AT it_filter ASSIGNING <ls_filter>.
      READ TABLE lt_cov_lookup ASSIGNING <ls_cov_lookup>
        WITH TABLE KEY obj_type = <ls_filter>-object
                        obj_name = <ls_filter>-obj_name.
      IF sy-subrc = 0 AND <ls_cov_lookup>-resolution_status = zcl_abapgit_ortec_obj_cover=>cs_resolution-found.
        APPEND <ls_filter> TO lt_found_filter.
      ENDIF.
    ENDLOOP.

    rt_rows = select_partial_rows_for_filter(
      iv_repo_key     = iv_repo_key
      iv_commit       = iv_commit
      iv_context_hash = iv_context_hash
      it_filter       = lt_found_filter ).
  ENDMETHOD.


  METHOD rebuild_index.
    DATA lv_lock_id TYPE zcl_abapgit_ortec_pack_raw=>ty_session_id.
    DATA lt_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_commit_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_commit_object TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_commit TYPE zcl_abapgit_git_pack=>ty_commit.

    DATA lt_pending TYPE ty_tree_work_tt.
    DATA lt_next TYPE ty_tree_work_tt.
    DATA lt_seen_trees TYPE ty_sha1_set.

    DATA lt_tree_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_tree_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_tree_data TYPE ty_tree_data_tt.
    DATA lt_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.

    DATA ls_tree_data TYPE ty_tree_data.
    DATA ls_item TYPE zif_abapgit_definitions=>ty_item.
    DATA ls_row TYPE zaog_obj_index.
    DATA lt_rows TYPE ty_index_rows_tt.
    DATA lv_path_hash TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_next_path TYPE string.

    FIELD-SYMBOLS <ls_work> TYPE ty_tree_work.
    FIELD-SYMBOLS <ls_obj>  TYPE zif_abapgit_definitions=>ty_object.
    FIELD-SYMBOLS <ls_node> TYPE zcl_abapgit_git_pack=>ty_node.

    lv_lock_id = zcl_abapgit_ortec_pack_raw=>acquire_repo_lock( iv_repo_key = iv_repo_key ).

    TRY.
        IF is_index_ready(
            iv_repo_key     = iv_repo_key
            iv_commit       = iv_commit
            iv_context_hash = iv_context_hash ) = abap_true.
          zcl_abapgit_ortec_pack_raw=>release_repo_lock( lv_lock_id ).
          RETURN.
        ENDIF.

        invalidate_commit_index(
          iv_repo_key = iv_repo_key
          iv_commit   = iv_commit ).

        APPEND iv_commit TO lt_commit_sha.
        TRY.
            lt_commit_objects = zcl_abapgit_ortec_obj_store=>get_objects(
              iv_repo_key   = iv_repo_key
              it_sha1s      = lt_commit_sha
              iv_bulk_fetch = abap_true ).
          CATCH zcx_abapgit_ortec_git INTO DATA(lx_store_commit).
            zcx_abapgit_exception=>raise_with_text( lx_store_commit ).
        ENDTRY.

        READ TABLE lt_commit_objects INTO ls_commit_object INDEX 1.
        IF sy-subrc <> 0 OR ls_commit_object-type <> zif_abapgit_git_definitions=>c_type-commit.
          zcx_abapgit_exception=>raise( |Index build: commit { iv_commit } missing| ).
        ENDIF.

        ls_commit = zcl_abapgit_git_pack=>decode_commit( ls_commit_object-data ).
        IF ls_commit-tree IS INITIAL.
          zcx_abapgit_exception=>raise( |Index build: commit { iv_commit } has no tree| ).
        ENDIF.

        APPEND VALUE #( tree_sha1 = ls_commit-tree path = '/' ) TO lt_pending.
        INSERT ls_commit-tree INTO TABLE lt_seen_trees.

        WHILE lt_pending IS NOT INITIAL.
          CLEAR lt_tree_sha1s.
          CLEAR lt_tree_objects.
          CLEAR lt_tree_data.
          CLEAR lt_next.

          LOOP AT lt_pending ASSIGNING <ls_work>.
            APPEND <ls_work>-tree_sha1 TO lt_tree_sha1s.
          ENDLOOP.

          TRY.
              lt_tree_objects = zcl_abapgit_ortec_obj_store=>get_objects(
                iv_repo_key   = iv_repo_key
                it_sha1s      = lt_tree_sha1s
                iv_bulk_fetch = abap_true ).
            CATCH zcx_abapgit_ortec_git INTO DATA(lx_store_tree).
              zcx_abapgit_exception=>raise_with_text( lx_store_tree ).
          ENDTRY.

          LOOP AT lt_tree_objects ASSIGNING <ls_obj>
              WHERE type = zif_abapgit_git_definitions=>c_type-tree.
            CLEAR ls_tree_data.
            ls_tree_data-tree_sha1 = <ls_obj>-sha1.
            ls_tree_data-data      = <ls_obj>-data.
            INSERT ls_tree_data INTO TABLE lt_tree_data.
          ENDLOOP.

          LOOP AT lt_pending ASSIGNING <ls_work>.
            READ TABLE lt_tree_data INTO ls_tree_data
              WITH TABLE KEY tree_sha1 = <ls_work>-tree_sha1.
            IF sy-subrc <> 0.
              CONTINUE.
            ENDIF.

            TRY.
                lt_nodes = zcl_abapgit_git_pack=>decode_tree( ls_tree_data-data ).
              CATCH zcx_abapgit_exception INTO DATA(lx_tree_dec).
                zcx_abapgit_exception=>raise_with_text( lx_tree_dec ).
            ENDTRY.

            LOOP AT lt_nodes ASSIGNING <ls_node>.
              CASE <ls_node>-chmod.
                WHEN zif_abapgit_git_definitions=>c_chmod-dir.
                  CONCATENATE <ls_work>-path <ls_node>-name '/' INTO lv_next_path.
                  READ TABLE lt_seen_trees WITH TABLE KEY table_line = <ls_node>-sha1
                    TRANSPORTING NO FIELDS.
                  IF sy-subrc <> 0.
                    INSERT <ls_node>-sha1 INTO TABLE lt_seen_trees.
                    APPEND VALUE #( tree_sha1 = <ls_node>-sha1
                                    path      = lv_next_path ) TO lt_next.
                  ENDIF.

                WHEN zif_abapgit_git_definitions=>c_chmod-file
                  OR zif_abapgit_git_definitions=>c_chmod-executable
                  OR zif_abapgit_git_definitions=>c_chmod-symbolic_link.

                  TRY.
                      zcl_abapgit_filename_logic=>file_to_object(
                        EXPORTING
                          iv_filename = <ls_node>-name
                          iv_path     = <ls_work>-path
                          iv_devclass = iv_devclass
                          io_dot      = io_dot
                        IMPORTING
                          es_item     = ls_item ).
                    CATCH zcx_abapgit_exception.
                      CONTINUE.
                  ENDTRY.

                  IF ls_item-obj_type IS INITIAL OR ls_item-obj_name IS INITIAL.
                    CONTINUE.
                  ENDIF.

                  IF strlen( <ls_work>-path ) > 255 OR strlen( <ls_node>-name ) > 255.
                    CONTINUE.
                  ENDIF.

                  TRY.
                      lv_path_hash = zcl_abapgit_hash=>sha1_string(
                        |{ <ls_work>-path }{ <ls_node>-name }| ).
                    CATCH zcx_abapgit_exception.
                      CONTINUE.
                  ENDTRY.

                  CLEAR ls_row.
                  ls_row-repo_key    = iv_repo_key.
                  ls_row-commit_sha1 = iv_commit.
                  ls_row-obj_type    = ls_item-obj_type.
                  ls_row-obj_name    = ls_item-obj_name.
                  ls_row-path_hash   = lv_path_hash.
                  ls_row-file_path   = <ls_work>-path.
                  ls_row-file_name   = <ls_node>-name.
                  ls_row-blob_sha1   = <ls_node>-sha1.
                  ls_row-tree_sha1   = <ls_work>-tree_sha1.
                  ls_row-idx_status  = c_status_ready.
                  ls_row-context_hash = iv_context_hash.
                  APPEND ls_row TO lt_rows.

                  IF lines( lt_rows ) >= c_index_write_chunk_size.
                    MODIFY zaog_obj_index FROM TABLE lt_rows.
                    CLEAR lt_rows.
                  ENDIF.

                WHEN OTHERS.
                  CONTINUE.
              ENDCASE.
            ENDLOOP.
          ENDLOOP.

          lt_pending = lt_next.
        ENDWHILE.

        IF lt_rows IS NOT INITIAL.
          MODIFY zaog_obj_index FROM TABLE lt_rows.
        ENDIF.

        " Always write the completion marker as the LAST step of a fully
        " successful walk, regardless of how many filter-relevant rows were
        " found. This is the sole positive signal that this commit's index
        " is completely built (see is_index_ready STRICT mode). Without an
        " unconditional marker write, a walk interrupted partway through
        " (corrupt tree, missing object, decode failure) would leave only
        " partial data rows behind, and any completeness check based on
        " "does at least one row exist" would wrongly treat that partial
        " index as fully built.
        CLEAR ls_row.
        ls_row-repo_key    = iv_repo_key.
        ls_row-commit_sha1 = iv_commit.
        ls_row-obj_type    = c_marker_obj_type.
        ls_row-obj_name    = c_marker_obj_name.
        ls_row-path_hash   = c_marker_path_hash.
        ls_row-idx_status  = c_status_ready.
        MODIFY zaog_obj_index FROM ls_row.

        zcl_abapgit_ortec_pack_raw=>release_repo_lock( lv_lock_id ).
      CATCH zcx_abapgit_exception INTO DATA(lx_index).
        zcl_abapgit_ortec_pack_raw=>release_repo_lock( lv_lock_id ).
        RAISE EXCEPTION lx_index.
      CATCH cx_root INTO DATA(lx_root).
        zcl_abapgit_ortec_pack_raw=>release_repo_lock( lv_lock_id ).
        zcx_abapgit_exception=>raise_with_text( lx_root ).
    ENDTRY.
  ENDMETHOD.


  METHOD walk_filtered.
    " design doc §11 step 4, §13 W5/W6/W8. A careful, deliberate copy of
    " rebuild_index's own commit->tree BFS (not extracted into a shared
    " primitive - see the OBJ-PERF-IMPL-C implementation log for why) with
    " the differences the design mandates: writes are bounded to it_filter,
    " land exclusively in ZAOG_OBJ_PIDX (never ZAOG_OBJ_INDEX, never the
    " COMPLETE-mode readiness marker), and completion writes a coverage
    " fact per it_filter entry instead.
    DATA lv_lock_id TYPE zcl_abapgit_ortec_pack_raw=>ty_session_id.
    DATA lt_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_commit_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_commit_object TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_commit TYPE zcl_abapgit_git_pack=>ty_commit.

    DATA lt_pending TYPE ty_tree_work_tt.
    DATA lt_next TYPE ty_tree_work_tt.
    DATA lt_seen_trees TYPE ty_sha1_set.

    DATA lt_tree_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_tree_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_tree_data TYPE ty_tree_data_tt.
    DATA lt_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.

    DATA ls_tree_data TYPE ty_tree_data.
    DATA ls_item TYPE zif_abapgit_definitions=>ty_item.
    DATA ls_pidx_row TYPE zaog_obj_pidx.
    DATA lt_pidx_rows TYPE STANDARD TABLE OF zaog_obj_pidx WITH DEFAULT KEY.
    DATA lv_path_hash TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_next_path TYPE string.

    TYPES:
      BEGIN OF ty_match_key,
        obj_type TYPE zaog_obj_pidx-obj_type,
        obj_name TYPE zaog_obj_pidx-obj_name,
      END OF ty_match_key,
      ty_match_set TYPE HASHED TABLE OF ty_match_key WITH UNIQUE KEY obj_type obj_name.
    DATA lt_matched TYPE ty_match_set.
    DATA lt_filter_set TYPE ty_match_set.

    DATA lv_have_eligible TYPE abap_bool.
    DATA lv_hist_level TYPE zcl_abapgit_ortec_mat_state=>ty_hist_level.
    DATA lt_coverage_results TYPE zcl_abapgit_ortec_obj_cover=>ty_coverage_tt.
    DATA lt_missing_results TYPE zcl_abapgit_ortec_obj_cover=>ty_coverage_tt.
    DATA ls_coverage_result TYPE zcl_abapgit_ortec_obj_cover=>ty_coverage.

    FIELD-SYMBOLS <ls_work> TYPE ty_tree_work.
    FIELD-SYMBOLS <ls_obj>  TYPE zif_abapgit_definitions=>ty_object.
    FIELD-SYMBOLS <ls_node> TYPE zcl_abapgit_git_pack=>ty_node.
    FIELD-SYMBOLS <ls_filter> TYPE zif_abapgit_definitions=>ty_tadir.

    LOOP AT it_filter ASSIGNING <ls_filter>.
      INSERT VALUE ty_match_key( obj_type = <ls_filter>-object obj_name = <ls_filter>-obj_name )
        INTO TABLE lt_filter_set.
    ENDLOOP.

    lv_lock_id = zcl_abapgit_ortec_pack_raw=>acquire_repo_lock( iv_repo_key = iv_repo_key ).

    TRY.
        " Mirrors rebuild_index's own double-check: avoids a redundant
        " walk if a concurrent COMPLETE rebuild finished first.
        IF is_index_ready(
            iv_repo_key     = iv_repo_key
            iv_commit       = iv_commit
            iv_context_hash = iv_context_hash ) = abap_true.
          zcl_abapgit_ortec_pack_raw=>release_repo_lock( lv_lock_id ).
          RETURN.
        ENDIF.

        APPEND iv_commit TO lt_commit_sha.
        TRY.
            lt_commit_objects = zcl_abapgit_ortec_obj_store=>get_objects(
              iv_repo_key   = iv_repo_key
              it_sha1s      = lt_commit_sha
              iv_bulk_fetch = abap_true ).
          CATCH zcx_abapgit_ortec_git INTO DATA(lx_store_commit).
            zcx_abapgit_exception=>raise_with_text( lx_store_commit ).
        ENDTRY.

        READ TABLE lt_commit_objects INTO ls_commit_object INDEX 1.
        IF sy-subrc <> 0 OR ls_commit_object-type <> zif_abapgit_git_definitions=>c_type-commit.
          zcx_abapgit_exception=>raise( |Filtered walk: commit { iv_commit } missing| ).
        ENDIF.

        ls_commit = zcl_abapgit_git_pack=>decode_commit( ls_commit_object-data ).
        IF ls_commit-tree IS INITIAL.
          zcx_abapgit_exception=>raise( |Filtered walk: commit { iv_commit } has no tree| ).
        ENDIF.

        APPEND VALUE #( tree_sha1 = ls_commit-tree path = '/' ) TO lt_pending.
        INSERT ls_commit-tree INTO TABLE lt_seen_trees.

        WHILE lt_pending IS NOT INITIAL.
          CLEAR lt_tree_sha1s.
          CLEAR lt_tree_objects.
          CLEAR lt_tree_data.
          CLEAR lt_next.

          LOOP AT lt_pending ASSIGNING <ls_work>.
            APPEND <ls_work>-tree_sha1 TO lt_tree_sha1s.
          ENDLOOP.

          TRY.
              lt_tree_objects = zcl_abapgit_ortec_obj_store=>get_objects(
                iv_repo_key   = iv_repo_key
                it_sha1s      = lt_tree_sha1s
                iv_bulk_fetch = abap_true ).
            CATCH zcx_abapgit_ortec_git INTO DATA(lx_store_tree).
              zcx_abapgit_exception=>raise_with_text( lx_store_tree ).
          ENDTRY.

          LOOP AT lt_tree_objects ASSIGNING <ls_obj>
              WHERE type = zif_abapgit_git_definitions=>c_type-tree.
            CLEAR ls_tree_data.
            ls_tree_data-tree_sha1 = <ls_obj>-sha1.
            ls_tree_data-data      = <ls_obj>-data.
            INSERT ls_tree_data INTO TABLE lt_tree_data.
          ENDLOOP.

          LOOP AT lt_pending ASSIGNING <ls_work>.
            READ TABLE lt_tree_data INTO ls_tree_data
              WITH TABLE KEY tree_sha1 = <ls_work>-tree_sha1.
            IF sy-subrc <> 0.
              CONTINUE.
            ENDIF.

            TRY.
                lt_nodes = zcl_abapgit_git_pack=>decode_tree( ls_tree_data-data ).
              CATCH zcx_abapgit_exception INTO DATA(lx_tree_dec).
                zcx_abapgit_exception=>raise_with_text( lx_tree_dec ).
            ENDTRY.

            LOOP AT lt_nodes ASSIGNING <ls_node>.
              CASE <ls_node>-chmod.
                WHEN zif_abapgit_git_definitions=>c_chmod-dir.
                  CONCATENATE <ls_work>-path <ls_node>-name '/' INTO lv_next_path.
                  READ TABLE lt_seen_trees WITH TABLE KEY table_line = <ls_node>-sha1
                    TRANSPORTING NO FIELDS.
                  IF sy-subrc <> 0.
                    INSERT <ls_node>-sha1 INTO TABLE lt_seen_trees.
                    APPEND VALUE #( tree_sha1 = <ls_node>-sha1
                                    path      = lv_next_path ) TO lt_next.
                  ENDIF.

                WHEN zif_abapgit_git_definitions=>c_chmod-file
                  OR zif_abapgit_git_definitions=>c_chmod-executable
                  OR zif_abapgit_git_definitions=>c_chmod-symbolic_link.

                  TRY.
                      zcl_abapgit_filename_logic=>file_to_object(
                        EXPORTING
                          iv_filename = <ls_node>-name
                          iv_path     = <ls_work>-path
                          iv_devclass = iv_devclass
                          io_dot      = io_dot
                        IMPORTING
                          es_item     = ls_item ).
                    CATCH zcx_abapgit_exception.
                      CONTINUE.
                  ENDTRY.

                  IF ls_item-obj_type IS INITIAL OR ls_item-obj_name IS INITIAL.
                    CONTINUE.
                  ENDIF.

                  " Bound ZAOG_OBJ_PIDX writes to the caller's own K
                  " objects, never the full F (design §11 step 4). O(1)
                  " hashed lookup (perf fix PS-001), not a linear it_filter scan.
                  READ TABLE lt_filter_set TRANSPORTING NO FIELDS
                    WITH TABLE KEY obj_type = ls_item-obj_type obj_name = ls_item-obj_name.
                  IF sy-subrc <> 0.
                    CONTINUE.
                  ENDIF.

                  IF strlen( <ls_work>-path ) > 255 OR strlen( <ls_node>-name ) > 255.
                    CONTINUE.
                  ENDIF.

                  TRY.
                      lv_path_hash = zcl_abapgit_hash=>sha1_string(
                        |{ <ls_work>-path }{ <ls_node>-name }| ).
                    CATCH zcx_abapgit_exception.
                      CONTINUE.
                  ENDTRY.

                  CLEAR ls_pidx_row.
                  ls_pidx_row-repo_key     = iv_repo_key.
                  ls_pidx_row-commit_sha1  = iv_commit.
                  ls_pidx_row-obj_type     = ls_item-obj_type.
                  ls_pidx_row-obj_name     = ls_item-obj_name.
                  ls_pidx_row-context_hash = iv_context_hash.
                  ls_pidx_row-path_hash    = lv_path_hash.
                  ls_pidx_row-file_path    = <ls_work>-path.
                  ls_pidx_row-file_name    = <ls_node>-name.
                  ls_pidx_row-blob_sha1    = <ls_node>-sha1.
                  ls_pidx_row-tree_sha1    = <ls_work>-tree_sha1.
                  ls_pidx_row-idx_status   = c_status_ready.
                  APPEND ls_pidx_row TO lt_pidx_rows.

                  INSERT VALUE ty_match_key( obj_type = ls_item-obj_type obj_name = ls_item-obj_name )
                    INTO TABLE lt_matched.

                  IF lines( lt_pidx_rows ) >= zcl_abapgit_ortec_obj_cover=>c_filter_chunk_size.
                    MODIFY zaog_obj_pidx FROM TABLE lt_pidx_rows.
                    CLEAR lt_pidx_rows.
                  ENDIF.

                WHEN OTHERS.
                  CONTINUE.
              ENDCASE.
            ENDLOOP.
          ENDLOOP.

          lt_pending = lt_next.
        ENDWHILE.

        IF lt_pidx_rows IS NOT INITIAL.
          MODIFY zaog_obj_pidx FROM TABLE lt_pidx_rows.
        ENDIF.

        " W8/§11.4 (Slice 4 gate, implemented now alongside walk_filtered
        " per this slice's own instructions): a commit's object graph must
        " be certified complete AND actually be the caller's known current
        " remote tip before a zero-match result may claim the strong
        " RESOLVED_NOT_PRESENT_REMOTE fact - otherwise the weaker
        " RESOLVED_NO_FILES is used, unconditionally.
        lv_have_eligible = zcl_abapgit_ortec_mat_state=>is_graph_have_eligible(
          iv_repo_key = iv_repo_key
          iv_commit   = iv_commit ).
        IF lv_have_eligible = abap_true.
          lv_hist_level = zcl_abapgit_ortec_mat_state=>cs_hist_level-graph_complete.
        ELSE.
          lv_hist_level = zcl_abapgit_ortec_mat_state=>cs_hist_level-unknown.
        ENDIF.

        LOOP AT it_filter ASSIGNING <ls_filter>.
          CLEAR ls_coverage_result.
          ls_coverage_result-obj_type = <ls_filter>-object.
          ls_coverage_result-obj_name = <ls_filter>-obj_name.

          READ TABLE lt_matched TRANSPORTING NO FIELDS
            WITH TABLE KEY obj_type = <ls_filter>-object
                            obj_name = <ls_filter>-obj_name.
          IF sy-subrc = 0.
            ls_coverage_result-resolution_status = zcl_abapgit_ortec_obj_cover=>cs_resolution-found.
          ELSEIF lv_have_eligible = abap_true
              AND iv_current_remote IS NOT INITIAL
              AND iv_commit = iv_current_remote.
            ls_coverage_result-resolution_status = zcl_abapgit_ortec_obj_cover=>cs_resolution-resolved_not_present_remote.
          ELSE.
            ls_coverage_result-resolution_status = zcl_abapgit_ortec_obj_cover=>cs_resolution-resolved_no_files.
          ENDIF.

          APPEND ls_coverage_result TO lt_coverage_results.
        ENDLOOP.

        TRY.
            zcl_abapgit_ortec_obj_cover=>write_coverage(
              iv_repo_key        = iv_repo_key
              iv_commit          = iv_commit
              iv_context_hash    = iv_context_hash
              iv_walk_hist_level = lv_hist_level
              it_results         = lt_coverage_results ).
          CATCH zcx_abapgit_ortec_git.
            " Non-fatal (§6 trigger 4/AR-1-08) - the walk's own resolved
            " rows are already durably written to ZAOG_OBJ_PIDX; a
            " coverage-write failure here must never block or fail the
            " current request.
        ENDTRY.

        zcl_abapgit_ortec_pack_raw=>release_repo_lock( lv_lock_id ).
      CATCH zcx_abapgit_exception INTO DATA(lx_walk).
        " §4.1/§6 trigger 2/AR-1-07: best-effort record one 'M'
        " (unresolved_missing_local_data) row per it_filter entry before
        " re-raising, so an identical repeat request can back off instead
        " of re-attempting the same doomed walk forever. A failure in this
        " best-effort write must never suppress or replace the original
        " exception.
        TRY.
            CLEAR lt_missing_results.
            LOOP AT it_filter ASSIGNING <ls_filter>.
              APPEND VALUE #(
                obj_type          = <ls_filter>-object
                obj_name          = <ls_filter>-obj_name
                resolution_status = zcl_abapgit_ortec_obj_cover=>cs_resolution-unresolved_missing_local_data
              ) TO lt_missing_results.
            ENDLOOP.

            zcl_abapgit_ortec_obj_cover=>write_coverage(
              iv_repo_key        = iv_repo_key
              iv_commit          = iv_commit
              iv_context_hash    = iv_context_hash
              iv_walk_hist_level = zcl_abapgit_ortec_mat_state=>cs_hist_level-unknown
              it_results         = lt_missing_results ).
          CATCH zcx_abapgit_ortec_git.
            " Best-effort - see the STOP_IF above: swallow only, never
            " suppress the original exception being re-raised below.
        ENDTRY.

        zcl_abapgit_ortec_pack_raw=>release_repo_lock( lv_lock_id ).
        RAISE EXCEPTION lx_walk.
      CATCH cx_root INTO DATA(lx_root).
        zcl_abapgit_ortec_pack_raw=>release_repo_lock( lv_lock_id ).
        zcx_abapgit_exception=>raise_with_text( lx_root ).
    ENDTRY.
  ENDMETHOD.


  METHOD select_rows_for_filter.
    " PA-001 fix: chunked at c_filter_chunk_size (AR-1-04) and predicated
    " on context_hash (W2 work order) - it_filter is caller-supplied and
    " can approach F at real Stage-by-Transport scale, exactly like
    " select_partial_rows_for_filter/get_coverage.
    DATA lt_chunk TYPE zif_abapgit_definitions=>ty_tadir_tt.

    FIELD-SYMBOLS <ls_filter> TYPE zif_abapgit_definitions=>ty_tadir.

    IF it_filter IS INITIAL.
      RETURN.
    ENDIF.

    LOOP AT it_filter ASSIGNING <ls_filter>.
      APPEND <ls_filter> TO lt_chunk.

      IF lines( lt_chunk ) >= zcl_abapgit_ortec_obj_cover=>c_filter_chunk_size.
        SELECT * FROM zaog_obj_index
          APPENDING TABLE rt_rows
          FOR ALL ENTRIES IN lt_chunk
          WHERE repo_key     = iv_repo_key
            AND commit_sha1  = iv_commit
            AND idx_status   = c_status_ready
            AND context_hash = iv_context_hash
            AND obj_type     = lt_chunk-object
            AND obj_name     = lt_chunk-obj_name.
        CLEAR lt_chunk.
      ENDIF.
    ENDLOOP.

    IF lt_chunk IS NOT INITIAL.
      SELECT * FROM zaog_obj_index
        APPENDING TABLE rt_rows
        FOR ALL ENTRIES IN lt_chunk
        WHERE repo_key     = iv_repo_key
          AND commit_sha1  = iv_commit
          AND idx_status   = c_status_ready
          AND context_hash = iv_context_hash
          AND obj_type     = lt_chunk-object
          AND obj_name     = lt_chunk-obj_name.
    ENDIF.

    DELETE rt_rows WHERE obj_type = c_marker_obj_type
                     AND obj_name = c_marker_obj_name.
  ENDMETHOD.


  METHOD select_partial_rows_for_filter.
    " FILTERED-mode reader (design doc §3.0b, AR-2-01). ZAOG_OBJ_PIDX has
    " CONTEXT_HASH as a real KEY field, so two contexts' rows for the same
    " object/path physically coexist as distinct rows - binding
    " context_hash here means a differently-contexted row can never be
    " mistaken for this caller's own answer. c_filter_chunk_size-bounded
    " (shared with zcl_abapgit_ortec_obj_cover=>get_coverage/write_coverage,
    " AR-1-04) since it_filter is caller-supplied and can be large.
    DATA lt_chunk TYPE zif_abapgit_definitions=>ty_tadir_tt.
    DATA lt_pidx_rows TYPE STANDARD TABLE OF zaog_obj_pidx WITH DEFAULT KEY.
    DATA ls_row TYPE zaog_obj_index.

    FIELD-SYMBOLS <ls_filter> TYPE zif_abapgit_definitions=>ty_tadir.
    FIELD-SYMBOLS <ls_pidx> TYPE zaog_obj_pidx.

    IF it_filter IS INITIAL.
      RETURN.
    ENDIF.

    LOOP AT it_filter ASSIGNING <ls_filter>.
      APPEND <ls_filter> TO lt_chunk.

      IF lines( lt_chunk ) >= zcl_abapgit_ortec_obj_cover=>c_filter_chunk_size.
        SELECT repo_key commit_sha1 obj_type obj_name context_hash path_hash
               file_path file_name blob_sha1 tree_sha1 idx_status
          FROM zaog_obj_pidx
          APPENDING TABLE lt_pidx_rows
          FOR ALL ENTRIES IN lt_chunk
          WHERE repo_key     = iv_repo_key
            AND commit_sha1  = iv_commit
            AND context_hash = iv_context_hash
            AND idx_status   = c_status_ready
            AND obj_type     = lt_chunk-object
            AND obj_name     = lt_chunk-obj_name.
        CLEAR lt_chunk.
      ENDIF.
    ENDLOOP.

    IF lt_chunk IS NOT INITIAL.
      SELECT repo_key commit_sha1 obj_type obj_name context_hash path_hash
             file_path file_name blob_sha1 tree_sha1 idx_status
        FROM zaog_obj_pidx
        APPENDING TABLE lt_pidx_rows
        FOR ALL ENTRIES IN lt_chunk
        WHERE repo_key     = iv_repo_key
          AND commit_sha1  = iv_commit
          AND context_hash = iv_context_hash
          AND idx_status   = c_status_ready
          AND obj_type     = lt_chunk-object
          AND obj_name     = lt_chunk-obj_name.
    ENDIF.

    " Project ZAOG_OBJ_PIDX (context_hash as a real key column) into the
    " identical ty_index_rows_tt/zaog_obj_index shape build_files_from_rows
    " already consumes, so that caller needs zero changes for either
    " COMPLETE- or FILTERED-mode rows.
    LOOP AT lt_pidx_rows ASSIGNING <ls_pidx>.
      CLEAR ls_row.
      ls_row-repo_key     = <ls_pidx>-repo_key.
      ls_row-commit_sha1  = <ls_pidx>-commit_sha1.
      ls_row-obj_type     = <ls_pidx>-obj_type.
      ls_row-obj_name     = <ls_pidx>-obj_name.
      ls_row-path_hash    = <ls_pidx>-path_hash.
      ls_row-file_path    = <ls_pidx>-file_path.
      ls_row-file_name    = <ls_pidx>-file_name.
      ls_row-blob_sha1    = <ls_pidx>-blob_sha1.
      ls_row-tree_sha1    = <ls_pidx>-tree_sha1.
      ls_row-idx_status   = <ls_pidx>-idx_status.
      ls_row-context_hash = <ls_pidx>-context_hash.
      APPEND ls_row TO rt_rows.
    ENDLOOP.
  ENDMETHOD.


  METHOD build_files_from_rows.
    DATA lt_sha1_set TYPE ty_sha1_set.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_blob_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_blob_data TYPE ty_blob_data_tt.
    DATA ls_blob_data TYPE ty_blob_data.
    DATA ls_file TYPE zif_abapgit_git_definitions=>ty_file.

    FIELD-SYMBOLS <ls_row> TYPE zaog_obj_index.
    FIELD-SYMBOLS <ls_obj> TYPE zif_abapgit_definitions=>ty_object.
    FIELD-SYMBOLS <lv_sha1> TYPE zif_abapgit_git_definitions=>ty_sha1.

    LOOP AT it_rows ASSIGNING <ls_row>.
      IF <ls_row>-blob_sha1 IS NOT INITIAL.
        INSERT <ls_row>-blob_sha1 INTO TABLE lt_sha1_set.
      ENDIF.
    ENDLOOP.

    LOOP AT lt_sha1_set ASSIGNING <lv_sha1>.
      APPEND <lv_sha1> TO lt_sha1s.
    ENDLOOP.

    IF lt_sha1s IS INITIAL.
      RETURN.
    ENDIF.

    " ORTEC: best-effort bulk top-up. If some blobs are missing from the local
    " store, try one targeted negotiated fetch before falling through to the
    " existing get_objects call (which raises on any remaining miss, exactly as
    " before). Never worse than the prior behavior: any failure here is
    " swallowed and the normal miss-handling below still applies.
    IF iv_url IS NOT INITIAL AND iv_commit IS NOT INITIAL.
      TRY.
          zcl_abapgit_ortec_missing_obj=>ensure_available(
            iv_repo_key = iv_repo_key
            iv_url      = iv_url
            iv_commit   = iv_commit
            it_sha1s    = lt_sha1s ).
        CATCH zcx_abapgit_ortec_git.
          " No fast-path benefit available; fall through to the standard miss
          " handling below (raises zcx_abapgit_exception, caller falls back).
      ENDTRY.
    ENDIF.

    TRY.
        lt_blob_objects = zcl_abapgit_ortec_obj_store=>get_objects(
          iv_repo_key   = iv_repo_key
          it_sha1s      = lt_sha1s
          iv_bulk_fetch = abap_true ).
      CATCH zcx_abapgit_ortec_git INTO DATA(lx_store).
        zcx_abapgit_exception=>raise_with_text( lx_store ).
    ENDTRY.

    LOOP AT lt_blob_objects ASSIGNING <ls_obj>
      WHERE type = zif_abapgit_git_definitions=>c_type-blob.
      CLEAR ls_blob_data.
      ls_blob_data-sha1 = <ls_obj>-sha1.
      ls_blob_data-data = <ls_obj>-data.
      INSERT ls_blob_data INTO TABLE lt_blob_data.
    ENDLOOP.

    LOOP AT it_rows ASSIGNING <ls_row>.
      READ TABLE lt_blob_data INTO ls_blob_data
        WITH TABLE KEY sha1 = <ls_row>-blob_sha1.
      IF sy-subrc <> 0.
        " get_objects above already guarantees every requested SHA1 exists as
        " SOME object in the store (it raises otherwise), so reaching here
        " means the SHA1 exists but is not of type blob - a genuine
        " CORRUPT_OR_INCOMPLETE condition (index/store inconsistency), never
        " a legitimate "file deleted" signal. Raise so the caller falls back
        " to the full, always-correct remote read instead of risking a wrong
        " verdict from inconsistent data.
        zcx_abapgit_exception=>raise(
          |{ zcl_abapgit_ortec_obj_store=>cs_object_state-corrupt_or_incomplete }: | &&
          |blob { <ls_row>-blob_sha1 } for { <ls_row>-file_path }{ <ls_row>-file_name } | &&
          |is not a valid blob object in the store| ).
      ENDIF.

      CLEAR ls_file.
      ls_file-path     = <ls_row>-file_path.
      ls_file-filename = <ls_row>-file_name.
      ls_file-sha1     = <ls_row>-blob_sha1.
      ls_file-data     = ls_blob_data-data.
      APPEND ls_file TO rt_files.
    ENDLOOP.
  ENDMETHOD.

ENDCLASS.
