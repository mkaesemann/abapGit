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
    "! @parameter rt_files |
    "! Filtered remote files with payload
    "! @raising zcx_abapgit_exception |
    "! Raised on unrecoverable index/object-store errors
    CLASS-METHODS get_files_for_filter
      IMPORTING
        iv_repo_key   TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
        iv_commit     TYPE zif_abapgit_git_definitions=>ty_sha1
        ii_obj_filter TYPE REF TO zif_abapgit_object_filter
        io_dot        TYPE REF TO zcl_abapgit_dot_abapgit
        iv_devclass   TYPE devclass
        iv_url        TYPE string OPTIONAL
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
    "! @parameter rv_yes |
    "! ABAP_TRUE if the index for this commit is fully built
    CLASS-METHODS is_index_ready
      IMPORTING
        iv_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
        iv_commit   TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING
        VALUE(rv_yes) TYPE abap_bool.

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
        iv_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
        iv_commit   TYPE zif_abapgit_git_definitions=>ty_sha1
        io_dot      TYPE REF TO zcl_abapgit_dot_abapgit
        iv_devclass TYPE devclass
      RAISING
        zcx_abapgit_exception.

    CLASS-METHODS rebuild_index
      IMPORTING
        iv_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
        iv_commit   TYPE zif_abapgit_git_definitions=>ty_sha1
        io_dot      TYPE REF TO zcl_abapgit_dot_abapgit
        iv_devclass TYPE devclass
      RAISING
        zcx_abapgit_exception.

    CLASS-METHODS select_rows_for_filter
      IMPORTING
        iv_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
        iv_commit   TYPE zif_abapgit_git_definitions=>ty_sha1
        it_filter   TYPE zif_abapgit_definitions=>ty_tadir_tt
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

    ensure_index(
      iv_repo_key = iv_repo_key
      iv_commit   = iv_commit
      io_dot      = io_dot
      iv_devclass = iv_devclass ).

    lt_rows = select_rows_for_filter(
      iv_repo_key = iv_repo_key
      iv_commit   = iv_commit
      it_filter   = lt_filter ).

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
        DELETE FROM zaog_obj_index
          WHERE repo_key    = iv_repo_key
            AND commit_sha1 = iv_commit.

        ensure_index(
          iv_repo_key = iv_repo_key
          iv_commit   = iv_commit
          io_dot      = io_dot
          iv_devclass = iv_devclass ).

        lt_rows = select_rows_for_filter(
          iv_repo_key = iv_repo_key
          iv_commit   = iv_commit
          it_filter   = lt_filter ).

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
          AND idx_status  = c_status_ready.

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
        AND idx_status  = c_status_ready.

    rv_yes = boolc( sy-subrc = 0 ).
  ENDMETHOD.


  METHOD ensure_index.
    IF is_index_ready( iv_repo_key = iv_repo_key iv_commit = iv_commit ) = abap_true.
      RETURN.
    ENDIF.

    rebuild_index(
      iv_repo_key = iv_repo_key
      iv_commit   = iv_commit
      io_dot      = io_dot
      iv_devclass = iv_devclass ).
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
        IF is_index_ready( iv_repo_key = iv_repo_key iv_commit = iv_commit ) = abap_true.
          zcl_abapgit_ortec_pack_raw=>release_repo_lock( lv_lock_id ).
          RETURN.
        ENDIF.

        DELETE FROM zaog_obj_index
          WHERE repo_key    = iv_repo_key
            AND commit_sha1 = iv_commit.

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


  METHOD select_rows_for_filter.
    IF it_filter IS INITIAL.
      RETURN.
    ENDIF.

    SELECT * FROM zaog_obj_index
      INTO TABLE rt_rows
      FOR ALL ENTRIES IN it_filter
      WHERE repo_key    = iv_repo_key
        AND commit_sha1 = iv_commit
        AND idx_status  = c_status_ready
        AND obj_type    = it_filter-object
        AND obj_name    = it_filter-obj_name.

    DELETE rt_rows WHERE obj_type = c_marker_obj_type
                     AND obj_name = c_marker_obj_name.
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
