"! <p class="shorttext synchronized">ORTEC Git Object Store - Performance Optimized</p>
"! Strategies 2-5: Session cache, bulk preload (55K threshold), cursor streaming, DB optimization
CLASS zcl_abapgit_ortec_obj_store DEFINITION
  PUBLIC FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    TYPES ty_repo_key TYPE c LENGTH 12.
    TYPES ty_object_state TYPE string.

    "! Explicit object/path buffering state (target design §2 - the boolean
    "! "found / not found" model is deliberately abolished). Every object or
    "! path resolution in the ORTEC fast path lands on exactly one of these
    "! six states. Non-negotiable rule: only CONFIRMED_ABSENT may ever be
    "! classified as remote-Deleted, and only once the remote tip, commit,
    "! parent tree, and path have ALL been positively resolved - NOT_BUFFERED,
    "! UNKNOWN_NEEDS_FETCH, and CORRUPT_OR_INCOMPLETE must never be
    "! classified as deleted, they must trigger a collect/fetch/repair retry
    "! or a safe fallback instead.
    CONSTANTS:
      BEGIN OF cs_object_state,
        "! Object decoded and present in memory for this operation.
        loaded                TYPE ty_object_state VALUE 'LOADED',
        "! Present in the index/store but not yet decoded into memory.
        indexed_needs_load    TYPE ty_object_state VALUE 'INDEXED_NEEDS_LOAD',
        "! Referenced by a resolved tree but absent from the local store.
        not_buffered          TYPE ty_object_state VALUE 'NOT_BUFFERED',
        "! Existence undetermined; remote refs/commit/tree not yet resolved.
        unknown_needs_fetch   TYPE ty_object_state VALUE 'UNKNOWN_NEEDS_FETCH',
        "! Remote tip + commit + parent tree + path all positively resolved,
        "! and the object is genuinely not present. The ONLY state that may
        "! be classified as remote-Deleted.
        confirmed_absent      TYPE ty_object_state VALUE 'CONFIRMED_ABSENT',
        "! Present but fails decode / delta-base resolution / type check.
        corrupt_or_incomplete TYPE ty_object_state VALUE 'CORRUPT_OR_INCOMPLETE',
      END OF cs_object_state.

    CLASS-METHODS store_object
      IMPORTING iv_repo_key TYPE ty_repo_key
                iv_sha1     TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_type     TYPE zif_abapgit_git_definitions=>ty_type
                iv_data     TYPE xstring
                iv_pack_id  TYPE c OPTIONAL
                iv_status   TYPE zaog_obj_store-status DEFAULT 'R'
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS store_objects
      IMPORTING iv_repo_key TYPE ty_repo_key
                it_objects  TYPE zif_abapgit_definitions=>ty_objects_tt
                iv_pack_id  TYPE c OPTIONAL
                iv_status   TYPE zaog_obj_store-status DEFAULT 'R'
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS get_object
      IMPORTING iv_repo_key      TYPE ty_repo_key OPTIONAL
                iv_sha1          TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rs_object) TYPE zif_abapgit_definitions=>ty_object
      RAISING   zcx_abapgit_ortec_git.

    "! Explicitly set the repo_key used by get_object's blank-iv_repo_key
    "! fallback (e.g. delta-base resolution during standard pack decode,
    "! where no repo context is available in the caller's own signature).
    "! Callers that may trigger that fallback must call this with a
    "! definitively-correct value (or blank) immediately before the decode -
    "! never rely on an accidental side effect of an unrelated get_objects/
    "! populate_cache call, which can leak a stale, unrelated repo's key.
    "! @parameter iv_repo_key |
    "! Repository key to use for subsequent blank-iv_repo_key get_object
    "! calls, or blank to clear (safe default - forces get_object to raise
    "! instead of silently resolving against a stale, unrelated repo).
    CLASS-METHODS set_active_repo_key
      IMPORTING iv_repo_key TYPE ty_repo_key.

    "! Bulk, chunked object read with session-cache lookahead. Every SQL
    "! fallback for cache-miss SHA1s is chunked at c_select_package_size
    "! regardless of iv_bulk_fetch (see variant_b_d2_it8_dbsql_stmt_too_large
    "! incident: an unchunked iv_bulk_fetch = abap_true path previously
    "! caused DBSQL_STMNT_TOO_LARGE for a wide cache-miss set). iv_bulk_fetch
    "! is retained for call-site compatibility only and no longer changes
    "! chunking behavior.
    CLASS-METHODS get_objects
      IMPORTING iv_repo_key       TYPE ty_repo_key
                it_sha1s          TYPE zif_abapgit_git_definitions=>ty_sha1_tt
                iv_bulk_fetch     TYPE abap_bool DEFAULT abap_false
      RETURNING VALUE(rt_objects) TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_ortec_git.

    "! ORTEC D2 PERF-B-1/staged-visibility (target_design §5.3/§5.3.1): a
    "! dedicated status-aware read for a pack's own delta temp-key rows
    "! during that SAME pack's own resolution. Same chunked-bulk-read
    "! contract and strict raise-on-any-missing-SHA1 behavior as
    "! get_objects, but reads WHERE status IN ('D','R') instead of
    "! status = 'R' (does not reuse read_object_rows, which is hard-coded
    "! to 'R'). Callable ONLY by zcl_abapgit_ortec_pack_stream - a
    "! 'D'-status row is a decoded-but-not-yet-resolved delta temp object
    "! and must never be exposed via get_object/get_objects/
    "! get_available_objects (those remain strictly status = 'R'-only,
    "! unmodified by this method).
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter it_sha1s |
    "! Temp-key (or real) SHA1s to read, deduplicated internally
    "! @parameter rt_objects |
    "! One object per requested SHA1
    "! @raising zcx_abapgit_ortec_git |
    "! If any requested SHA1 is not found with status 'D' or 'R'
    CLASS-METHODS get_staged_delta_objects
      IMPORTING iv_repo_key       TYPE ty_repo_key
                it_sha1s          TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RETURNING VALUE(rt_objects) TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_ortec_git.

    "! Bulk-read every locally READY object found for the supplied SHA1 set.
    "! Missing SHA1 values are ignored. Input is deduplicated and read in
    "! bounded packages; found rows warm the normal session cache.
    CLASS-METHODS get_available_objects
      IMPORTING iv_repo_key       TYPE ty_repo_key
                it_sha1s          TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RETURNING VALUE(rt_objects) TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS get_reachable_objects
      IMPORTING iv_repo_key       TYPE ty_repo_key
                iv_commit         TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rt_objects) TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_ortec_git.

    "! Lighter-weight counterpart to get_reachable_objects for completeness
    "! verification only: walks the SAME commit -> tree -> blob structure and
    "! raises under the SAME conditions (missing/undecodable commit or tree,
    "! missing blob, unknown chmod), but never materializes blob DATA and
    "! never triggers a full-repo cache preload - only commit/tree objects
    "! (whose total size is bounded by directory structure, not file
    "! content) are ever fetched with their data; blob presence is proven
    "! via a SHA1-only existence check. Callers that actually need object
    "! CONTENT (e.g. building a working tree) must keep using
    "! get_reachable_objects - this method exists purely to answer "is
    "! everything reachable from this commit present", which never needs to
    "! read a single byte of blob data.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_commit |
    "! Commit SHA1 to walk from
    "! @parameter rt_sha1s |
    "! SHA1 of the commit, every reachable tree, and every reachable blob
    "! @raising zcx_abapgit_ortec_git |
    "! If the commit, a tree, or a blob reachable from it is not found or
    "! cannot be decoded
    CLASS-METHODS get_reachable_sha1s
      IMPORTING iv_repo_key     TYPE ty_repo_key
                iv_commit       TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rt_sha1s) TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RAISING   zcx_abapgit_ortec_git.

    "! Variant B / Package B graph-closure verification: walks the SAME
    "! commit -> tree structure as get_reachable_objects/get_reachable_sha1s,
    "! but is deliberately blob-blind - it never collects, checks presence
    "! of, or reads a single blob SHA1/byte. This is the one difference that
    "! matters for a blobless (`filter blob:none`) cold-branch fetch: every
    "! historical blob is legitimately PROMISED, not corrupt
    "! (zcl_abapgit_ortec_mat_state's GRAPH_COMPLETE level is defined as
    "! "requested commit and complete tree closure present; historical
    "! blobs may be promised" - see .github/skills/git-partial-clone/SKILL.md).
    "! get_reachable_sha1s cannot be reused here: it hard-requires every
    "! reachable blob to be present (via get_present_sha1s) and raises
    "! otherwise, which would incorrectly fail a perfectly valid blobless
    "! fetch.
    "! Uses iv_bulk_fetch = abap_false for every frontier read (design
    "! decision INV-B-13, .memory/logs/variant_b_package_b_design.md §6).
    "! Historical note (variant_b_d2_it8_dbsql_stmt_too_large incident):
    "! get_objects' iv_bulk_fetch = abap_true branch used to skip chunking
    "! at c_select_package_size, which is why abap_false was originally
    "! chosen here. Both branches now chunk identically, so this is no
    "! longer a functional requirement for correctness here - kept as-is
    "! for stability, since it was already correct and reviewed.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_commit |
    "! Commit SHA1 whose tree closure must be verified
    "! @raising zcx_abapgit_ortec_git |
    "! If the commit or any tree reachable from it is missing, not the
    "! expected type, undecodable, or contains an unrecognized chmod
    CLASS-METHODS verify_tree_closure
      IMPORTING iv_repo_key TYPE ty_repo_key
                iv_commit   TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING   zcx_abapgit_ortec_git.

    "! Variant B / Package B checkpoint B2: blob-collecting counterpart to
    "! verify_tree_closure - walks the identical commit -> tree structure
    "! (same raise conditions: missing/wrong-type/non-READY commit or tree,
    "! unrecognized chmod) but additionally collects every reachable blob
    "! leaf SHA1 (chmod file/executable/symlink) into a deduplicated result
    "! set. Never fetches blob DATA and never checks blob presence - a
    "! blob SHA1 discovered here is a fact about the tree structure, not a
    "! claim that the blob is materialized; callers (e.g.
    "! zcl_abapgit_ortec_cold_init=>materialize_tip_snapshot) bulk-subtract
    "! READY presence separately via get_missing_sha1s. get_reachable_sha1s
    "! is not reused here for the same reason verify_tree_closure does not
    "! reuse it: that method hard-requires every reachable blob to already
    "! be present, which is exactly backwards for a call whose entire
    "! purpose is to discover which of the tip's blobs are NOT yet present.
    "! Uses iv_bulk_fetch = abap_false for every frontier read (INV-B-13,
    "! same rationale as verify_tree_closure). Historical note
    "! (variant_b_d2_it8_dbsql_stmt_too_large incident): get_objects'
    "! iv_bulk_fetch = abap_true branch used to skip chunking at
    "! c_select_package_size; both branches now chunk identically, so this
    "! is kept as-is for stability rather than out of functional necessity.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_commit |
    "! Commit SHA1 whose tip blob set must be discovered
    "! @parameter rt_sha1s |
    "! Deduplicated SHA1 of every blob reachable from the commit's tree,
    "! unfiltered by presence
    "! @raising zcx_abapgit_ortec_git |
    "! If the commit or any tree reachable from it is missing, not the
    "! expected type, undecodable, or contains an unrecognized chmod
    CLASS-METHODS get_tip_blob_sha1s
      IMPORTING iv_repo_key     TYPE ty_repo_key
                iv_commit       TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rt_sha1s) TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RAISING   zcx_abapgit_ortec_git.

    "! Set-based check for which of the given SHA1s are NOT present (status 'R')
    "! in the persistent store for this repository. One chunked SELECT per
    "! c_select_package_size objects; no per-object DB reads.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter it_sha1s |
    "! Candidate SHA1s to check
    "! @parameter rt_missing |
    "! Subset of it_sha1s (deduplicated) not found in the store
    CLASS-METHODS get_missing_sha1s
      IMPORTING iv_repo_key       TYPE ty_repo_key
                it_sha1s          TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RETURNING VALUE(rt_missing) TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    "! Verify the complete selected-tip blob set without loading OBJ_DATA.
    "! Every requested SHA1 must exist with STATUS = 'R' and OBJ_TYPE = blob.
    "! The implementation is chunked at C_SELECT_PACKAGE_SIZE and performs no
    "! database operation per individual SHA1.
    CLASS-METHODS verify_ready_blobs
      IMPORTING
        iv_repo_key TYPE ty_repo_key
        it_sha1s    TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RAISING
        zcx_abapgit_ortec_git.


    "! Set-based check for whether any of the given SHA1s was originally
    "! decoded as a delta whose recorded base (ZAOG_PACK_IDX-DELTA_BASE) is
    "! not itself present in the store with status 'R'. Used by the
    "! delta-base completeness gate before a commit's objects are trusted as
    "! thin-pack-safe "have" candidates. Chunked, set-based; no per-object
    "! DB reads.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter it_sha1s |
    "! Candidate SHA1s to check (e.g. every object reachable from a commit)
    "! @parameter rv_dangling |
    "! ABAP_TRUE if at least one dangling delta base was found
    CLASS-METHODS has_dangling_delta_base
      IMPORTING iv_repo_key        TYPE ty_repo_key
                it_sha1s           TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RETURNING VALUE(rv_dangling) TYPE abap_bool.

    CLASS-METHODS exists
      IMPORTING iv_repo_key      TYPE ty_repo_key
                iv_sha1          TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rv_exists) TYPE abap_bool.

    CLASS-METHODS get_known_commits
      IMPORTING iv_repo_key       TYPE ty_repo_key
      RETURNING VALUE(rt_commits) TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    CLASS-METHODS get_all_objects
      IMPORTING iv_repo_key       TYPE ty_repo_key
      RETURNING VALUE(rt_objects) TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS clear_repo
      IMPORTING iv_repo_key TYPE ty_repo_key
      RAISING   zcx_abapgit_ortec_git.

    "! STRATEGY 2: Clear session cache
    CLASS-METHODS invalidate_cache.

    "! Parse parent commit SHA1s from a stored commit object.
    "! Reads obj_data from ZAOG_OBJ_STORE and scans the Git commit
    "! header for parent lines (stops at the first empty line).
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_sha1 |
    "! Commit SHA1 to read parents from
    "! @parameter rt_parents |
    "! Zero or more parent SHA1s (0=root, 1=normal, 2+=merge)
    CLASS-METHODS get_commit_parents
      IMPORTING iv_repo_key       TYPE ty_repo_key
                iv_sha1           TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rt_parents) TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

  PROTECTED SECTION.
  PRIVATE SECTION.
    "! STRATEGY 2: Cache table
    TYPES BEGIN OF ty_cache_entry.
    INCLUDE TYPE zaog_obj_store.
    TYPES END OF ty_cache_entry.

    TYPES ty_obj_store_tt TYPE STANDARD TABLE OF zaog_obj_store WITH DEFAULT KEY.
    TYPES ty_sha1_set TYPE HASHED TABLE OF zif_abapgit_git_definitions=>ty_sha1
                      WITH UNIQUE KEY table_line.
    TYPES:
      BEGIN OF ty_sha1_row,
        sha1 TYPE zif_abapgit_git_definitions=>ty_sha1,
      END OF ty_sha1_row.
    TYPES ty_sha1_rows TYPE STANDARD TABLE OF ty_sha1_row WITH DEFAULT KEY.

    CLASS-DATA mt_cache          TYPE SORTED TABLE OF ty_cache_entry
                WITH UNIQUE KEY repo_key obj_sha1.
    CLASS-DATA mv_cache_repo_key TYPE ty_repo_key.
    CLASS-DATA mv_full_cache_repo_key TYPE ty_repo_key.

    "! Approved bounded observability counter (same disposition as
    "! zcl_abapgit_ortec_delta=>gv_bulk_load_calls): counts calls to
    "! read_object_rows, i.e. the number of bounded SQL packages a single
    "! get_objects()/get_available_objects()/has_dangling_delta_base() call
    "! actually issues. A plain integer increment, never persisted, never
    "! logged, test-reset only (LOCAL FRIENDS) - purely a call-shape
    "! verification seam for the variant_b_d2_it8_dbsql_stmt_too_large
    "! incident's bulk_fetch_uses_pkg_size/bulk_fetch_no_per_key_sql tests.
    CLASS-DATA gv_read_object_rows_calls TYPE i.

    "! Running byte size of OBJ_DATA currently held in MT_CACHE, kept in sync
    "! by CACHE_PUT/INVALIDATE_CACHE so the cache can be bounded without
    "! re-summing the table.
    CLASS-DATA gv_cache_bytes TYPE int8.

    CONSTANTS c_select_package_size TYPE i VALUE 1000.
    "! Hard upper bound on MT_CACHE payload bytes. When a CACHE_PUT would
    "! exceed it the cache is evicted first - bounds peak memory on a large
    "! cold-materialize / full-stage read set (HTTP_NO_MEMORY guard) while
    "! still letting stores keep the warm cache (no per-store invalidate
    "! thrash). 128 MB.
    CONSTANTS c_max_cache_bytes TYPE int8 VALUE 134217728.

    "! Insert one row into the bounded session cache, evicting the whole
    "! cache first if adding it would exceed C_MAX_CACHE_BYTES.
    CLASS-METHODS cache_put
      IMPORTING is_row TYPE ty_cache_entry.

    CLASS-METHODS get_timestamp
      RETURNING VALUE(rv_ts) TYPE timestampl.

    CLASS-METHODS is_cache_valid
      IMPORTING iv_repo_key     TYPE ty_repo_key
      RETURNING VALUE(rv_valid) TYPE abap_bool.

    CLASS-METHODS populate_cache
      IMPORTING iv_repo_key TYPE ty_repo_key
      RAISING   zcx_abapgit_ortec_git.

    CLASS-METHODS read_object_rows
      IMPORTING iv_repo_key    TYPE ty_repo_key
                it_sha1s       TYPE ty_sha1_rows
      RETURNING VALUE(rt_rows) TYPE ty_obj_store_tt.

    "! Chunked, obj_data-free existence check against the persistent store:
    "! selects ONLY obj_sha1, never obj_data, so a caller that only needs to
    "! prove presence (not read content) never pays the cost of
    "! transferring potentially large blob data over the DB connection.
    CLASS-METHODS get_present_sha1s
      IMPORTING iv_repo_key       TYPE ty_repo_key
                it_sha1s          TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RETURNING VALUE(rt_present) TYPE ty_sha1_set.
ENDCLASS.



CLASS zcl_abapgit_ortec_obj_store IMPLEMENTATION.


  METHOD store_object.
    DATA ls_row TYPE zaog_obj_store.

    ls_row-repo_key   = iv_repo_key.
    ls_row-obj_sha1   = iv_sha1.
    ls_row-obj_type   = iv_type.
    ls_row-obj_data   = iv_data.
    ls_row-obj_size   = xstrlen( iv_data ).
    ls_row-pack_id    = iv_pack_id.
    ls_row-created_at = get_timestamp( ).
    ls_row-status     = iv_status.
    MODIFY zaog_obj_store FROM ls_row.
    IF sy-subrc <> 0.
      zcx_abapgit_ortec_git=>raise( |Failed to store object { iv_sha1 }| ).
    ENDIF.
    " Stores only ADD content-addressed objects, so warm read-cache entries
    " stay valid - keep them (no per-store re-read thrash); the cache is
    " byte-bounded by CACHE_PUT, so retaining it is memory-safe.
    CLEAR mv_full_cache_repo_key.
  ENDMETHOD.


  METHOD store_objects.
    DATA lv_ts   TYPE timestampl.
    DATA ls_row  TYPE zaog_obj_store.
    DATA lt_rows TYPE STANDARD TABLE OF zaog_obj_store.
    FIELD-SYMBOLS <ls_obj> LIKE LINE OF it_objects.

    lv_ts = get_timestamp( ).
    LOOP AT it_objects ASSIGNING <ls_obj>.
      CLEAR ls_row.
      ls_row-repo_key   = iv_repo_key.
      ls_row-obj_sha1   = <ls_obj>-sha1.
      ls_row-obj_type   = <ls_obj>-type.
      ls_row-obj_data   = <ls_obj>-data.
      ls_row-obj_size   = xstrlen( <ls_obj>-data ).
      ls_row-pack_id    = iv_pack_id.
      ls_row-created_at = lv_ts.
      ls_row-status     = iv_status.
      APPEND ls_row TO lt_rows.
    ENDLOOP.
    IF lt_rows IS NOT INITIAL.
      MODIFY zaog_obj_store FROM TABLE lt_rows.
    ENDIF.
    " Keep warm read cache across stores (byte-bounded by CACHE_PUT); only
    " the full-load flag must be reset.
    CLEAR mv_full_cache_repo_key.
  ENDMETHOD.


  METHOD set_active_repo_key.
    mv_cache_repo_key = iv_repo_key.
  ENDMETHOD.


  METHOD get_object.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_effective_key TYPE ty_repo_key.

    " If no repo_key provided, use the currently cached repo (set by prior populate_cache)
    lv_effective_key = iv_repo_key.
    IF lv_effective_key IS INITIAL AND mv_cache_repo_key IS NOT INITIAL.
      lv_effective_key = mv_cache_repo_key.
    ENDIF.

    IF lv_effective_key IS INITIAL.
      zcx_abapgit_ortec_git=>raise( 'Repository key missing for object store read' ).
    ENDIF.

    APPEND iv_sha1 TO lt_sha1s.
    lt_objects = get_objects( iv_repo_key = lv_effective_key
                              it_sha1s    = lt_sha1s ).
    READ TABLE lt_objects INTO rs_object INDEX 1.
    IF sy-subrc = 0.
      RETURN.
    ENDIF.

    zcx_abapgit_ortec_git=>raise( |Object { iv_sha1 } not found in store| ).
  ENDMETHOD.


  METHOD get_available_objects.
    DATA lt_unique_sha1s TYPE ty_sha1_set.
    DATA lt_package TYPE ty_sha1_rows.
    DATA lt_db_rows TYPE ty_obj_store_tt.
    DATA ls_sha1 TYPE ty_sha1_row.
    DATA ls_cache_entry TYPE ty_cache_entry.
    DATA ls_object TYPE zif_abapgit_definitions=>ty_object.

    FIELD-SYMBOLS <lv_sha1> LIKE LINE OF it_sha1s.
    FIELD-SYMBOLS <ls_row> LIKE LINE OF lt_db_rows.

    IF iv_repo_key IS INITIAL.
      zcx_abapgit_ortec_git=>raise(
        'Repository key missing for available-object read' ).
    ENDIF.

    LOOP AT it_sha1s ASSIGNING <lv_sha1>
         WHERE table_line IS NOT INITIAL.
      INSERT <lv_sha1> INTO TABLE lt_unique_sha1s.
    ENDLOOP.

    LOOP AT lt_unique_sha1s ASSIGNING <lv_sha1>.
      READ TABLE mt_cache INTO ls_cache_entry
        WITH TABLE KEY repo_key = iv_repo_key obj_sha1 = <lv_sha1>.

      IF sy-subrc = 0 AND ls_cache_entry-status = 'R'.
        CLEAR ls_object.
        ls_object-sha1 = ls_cache_entry-obj_sha1.
        ls_object-type = ls_cache_entry-obj_type.
        ls_object-data = ls_cache_entry-obj_data.
        APPEND ls_object TO rt_objects.
        CONTINUE.
      ENDIF.

      ls_sha1-sha1 = <lv_sha1>.
      APPEND ls_sha1 TO lt_package.
      CLEAR ls_sha1.

      IF lines( lt_package ) >= c_select_package_size.
        CLEAR lt_db_rows.
        lt_db_rows = read_object_rows(
          iv_repo_key = iv_repo_key
          it_sha1s    = lt_package ).

        LOOP AT lt_db_rows ASSIGNING <ls_row>.
          CLEAR ls_cache_entry.
          MOVE-CORRESPONDING <ls_row> TO ls_cache_entry.
          cache_put( ls_cache_entry ).

          CLEAR ls_object.
          ls_object-sha1 = <ls_row>-obj_sha1.
          ls_object-type = <ls_row>-obj_type.
          ls_object-data = <ls_row>-obj_data.
          APPEND ls_object TO rt_objects.
        ENDLOOP.

        CLEAR lt_package.
      ENDIF.
    ENDLOOP.

    IF lt_package IS NOT INITIAL.
      CLEAR lt_db_rows.
      lt_db_rows = read_object_rows(
        iv_repo_key = iv_repo_key
        it_sha1s    = lt_package ).

      LOOP AT lt_db_rows ASSIGNING <ls_row>.
        CLEAR ls_cache_entry.
        MOVE-CORRESPONDING <ls_row> TO ls_cache_entry.
        cache_put( ls_cache_entry ).

        CLEAR ls_object.
        ls_object-sha1 = <ls_row>-obj_sha1.
        ls_object-type = <ls_row>-obj_type.
        ls_object-data = <ls_row>-obj_data.
        APPEND ls_object TO rt_objects.
      ENDLOOP.
    ENDIF.

    mv_cache_repo_key = iv_repo_key.
  ENDMETHOD.


  METHOD get_objects.
    DATA lt_unique_sha1s TYPE ty_sha1_set.
    DATA lt_missing_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_package TYPE ty_sha1_rows.
    DATA lt_db_rows TYPE ty_obj_store_tt.
    DATA lt_rows TYPE ty_obj_store_tt.
    DATA lt_found_sha1s TYPE ty_sha1_set.
    DATA ls_cache_entry TYPE ty_cache_entry.
    DATA ls_object TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_sha1 TYPE ty_sha1_row.

    FIELD-SYMBOLS <lv_sha1> LIKE LINE OF it_sha1s.
    FIELD-SYMBOLS <ls_row> LIKE LINE OF lt_rows.

    IF iv_repo_key IS INITIAL.
      zcx_abapgit_ortec_git=>raise( 'Repository key missing for object store read' ).
    ENDIF.

    LOOP AT it_sha1s ASSIGNING <lv_sha1>
        WHERE table_line IS NOT INITIAL.
      INSERT <lv_sha1> INTO TABLE lt_unique_sha1s.
    ENDLOOP.

    IF lt_unique_sha1s IS INITIAL.
      RETURN.
    ENDIF.

    LOOP AT lt_unique_sha1s ASSIGNING <lv_sha1>.
      READ TABLE mt_cache INTO ls_cache_entry
           WITH TABLE KEY repo_key = iv_repo_key obj_sha1 = <lv_sha1>.
      IF sy-subrc = 0 AND ls_cache_entry-status = 'R'.
        APPEND ls_cache_entry TO lt_rows.
      ELSE.
        APPEND <lv_sha1> TO lt_missing_sha1s.
      ENDIF.
    ENDLOOP.

    " INCIDENT variant_b_d2_it8_dbsql_stmt_too_large: iv_bulk_fetch = abap_true
    " used to build the ENTIRE lt_missing_sha1s set into one lt_package and
    " issue a single, unchunked read_object_rows call - unlike every other
    " read_object_rows caller in this class (this method's own abap_false
    " branch, get_available_objects, has_dangling_delta_base), which already
    " chunk at c_select_package_size. For a wide-enough cache-miss set (a
    " real cold branch's full blob frontier, confirmed live at 40,891
    " entries), the resulting single "obj_sha1 IN <range>" statement exceeded
    " HANA/DBSL's per-statement bind-marker ceiling (32,767), causing
    " DBSQL_STMNT_TOO_LARGE. Both branches now chunk identically: for any
    " set at or below c_select_package_size this still executes in exactly
    " one SELECT (no regression for the common case); a set above it now
    " correctly issues ceil(K / c_select_package_size) bounded SELECTs
    " instead of one oversized statement. iv_bulk_fetch no longer has any
    " observable effect on chunking (kept for signature/call-site
    " compatibility - no caller needs to change).
    LOOP AT lt_missing_sha1s ASSIGNING <lv_sha1>.
      ls_sha1-sha1 = <lv_sha1>.
      APPEND ls_sha1 TO lt_package.
      IF lines( lt_package ) >= c_select_package_size.
        lt_db_rows = read_object_rows( iv_repo_key = iv_repo_key
                                       it_sha1s    = lt_package ).
        APPEND LINES OF lt_db_rows TO lt_rows.
        CLEAR lt_package.
      ENDIF.
    ENDLOOP.

    IF lt_package IS NOT INITIAL.
      lt_db_rows = read_object_rows( iv_repo_key = iv_repo_key
                                     it_sha1s    = lt_package ).
      APPEND LINES OF lt_db_rows TO lt_rows.
    ENDIF.

    mv_cache_repo_key = iv_repo_key.

    LOOP AT lt_rows ASSIGNING <ls_row>.
      INSERT <ls_row>-obj_sha1 INTO TABLE lt_found_sha1s.
      MOVE-CORRESPONDING <ls_row> TO ls_cache_entry.
      cache_put( ls_cache_entry ).
      CLEAR ls_object.
      ls_object-sha1 = <ls_row>-obj_sha1.
      ls_object-type = <ls_row>-obj_type.
      ls_object-data = <ls_row>-obj_data.
      APPEND ls_object TO rt_objects.
    ENDLOOP.

    LOOP AT lt_unique_sha1s ASSIGNING <lv_sha1>.
      READ TABLE lt_found_sha1s WITH TABLE KEY table_line = <lv_sha1> TRANSPORTING NO FIELDS.
      IF sy-subrc <> 0.
        zcx_abapgit_ortec_git=>raise( |Object { <lv_sha1> } not found in store| ).
      ENDIF.
    ENDLOOP.
  ENDMETHOD.


  METHOD get_staged_delta_objects.
    DATA lt_unique_sha1s TYPE ty_sha1_set.
    DATA lt_missing_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_package TYPE ty_sha1_rows.
    DATA lt_db_rows TYPE ty_obj_store_tt.
    DATA lt_rows TYPE ty_obj_store_tt.
    DATA lt_found_sha1s TYPE ty_sha1_set.
    DATA ls_cache_entry TYPE ty_cache_entry.
    DATA ls_object TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_sha1 TYPE ty_sha1_row.
    DATA lr_sha1s TYPE RANGE OF zaog_obj_store-obj_sha1.

    FIELD-SYMBOLS <lv_sha1> LIKE LINE OF it_sha1s.
    FIELD-SYMBOLS <ls_row> LIKE LINE OF lt_rows.
    FIELD-SYMBOLS <ls_pkg> LIKE LINE OF lt_package.

    IF iv_repo_key IS INITIAL.
      zcx_abapgit_ortec_git=>raise( 'Repository key missing for object store read' ).
    ENDIF.

    LOOP AT it_sha1s ASSIGNING <lv_sha1>
        WHERE table_line IS NOT INITIAL.
      INSERT <lv_sha1> INTO TABLE lt_unique_sha1s.
    ENDLOOP.

    IF lt_unique_sha1s IS INITIAL.
      RETURN.
    ENDIF.

    " Own cache-hit check (PERF-M-2): admits 'D' as well as 'R', so a
    " temp-key row already warmed by preload_delta_rows registers as a
    " cache hit here, unlike get_objects'/get_available_objects' own
    " status = 'R'-only check.
    LOOP AT lt_unique_sha1s ASSIGNING <lv_sha1>.
      READ TABLE mt_cache INTO ls_cache_entry
           WITH TABLE KEY repo_key = iv_repo_key obj_sha1 = <lv_sha1>.
      IF sy-subrc = 0 AND ( ls_cache_entry-status = 'D' OR ls_cache_entry-status = 'R' ).
        APPEND ls_cache_entry TO lt_rows.
      ELSE.
        APPEND <lv_sha1> TO lt_missing_sha1s.
      ENDIF.
    ENDLOOP.

    " Own SELECT (not read_object_rows, which is hard-coded to status = 'R')
    " - chunked like get_objects' non-bulk branch.
    LOOP AT lt_missing_sha1s ASSIGNING <lv_sha1>.
      ls_sha1-sha1 = <lv_sha1>.
      APPEND ls_sha1 TO lt_package.
      IF lines( lt_package ) >= c_select_package_size.
        CLEAR lr_sha1s.
        LOOP AT lt_package ASSIGNING <ls_pkg>.
          APPEND VALUE #( sign = 'I' option = 'EQ' low = <ls_pkg>-sha1 ) TO lr_sha1s.
        ENDLOOP.
        CLEAR lt_db_rows.
        SELECT * FROM zaog_obj_store
          INTO TABLE lt_db_rows
          WHERE repo_key = iv_repo_key
            AND obj_sha1 IN lr_sha1s
            AND status   IN ( 'D', 'R' ).
        APPEND LINES OF lt_db_rows TO lt_rows.
        CLEAR lt_package.
      ENDIF.
    ENDLOOP.

    IF lt_package IS NOT INITIAL.
      CLEAR lr_sha1s.
      LOOP AT lt_package ASSIGNING <ls_pkg>.
        APPEND VALUE #( sign = 'I' option = 'EQ' low = <ls_pkg>-sha1 ) TO lr_sha1s.
      ENDLOOP.
      CLEAR lt_db_rows.
      SELECT * FROM zaog_obj_store
        INTO TABLE lt_db_rows
        WHERE repo_key = iv_repo_key
          AND obj_sha1 IN lr_sha1s
          AND status   IN ( 'D', 'R' ).
      APPEND LINES OF lt_db_rows TO lt_rows.
    ENDIF.

    mv_cache_repo_key = iv_repo_key.

    LOOP AT lt_rows ASSIGNING <ls_row>.
      INSERT <ls_row>-obj_sha1 INTO TABLE lt_found_sha1s.
      MOVE-CORRESPONDING <ls_row> TO ls_cache_entry.
      cache_put( ls_cache_entry ).
      CLEAR ls_object.
      ls_object-sha1 = <ls_row>-obj_sha1.
      ls_object-type = <ls_row>-obj_type.
      ls_object-data = <ls_row>-obj_data.
      APPEND ls_object TO rt_objects.
    ENDLOOP.

    LOOP AT lt_unique_sha1s ASSIGNING <lv_sha1>.
      READ TABLE lt_found_sha1s WITH TABLE KEY table_line = <lv_sha1> TRANSPORTING NO FIELDS.
      IF sy-subrc <> 0.
        zcx_abapgit_ortec_git=>raise( |Object { <lv_sha1> } not found in store| ).
      ENDIF.
    ENDLOOP.
  ENDMETHOD.


  METHOD get_reachable_objects.
    DATA lt_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_commit_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_current_trees TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_next_trees TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_tree_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_blob_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_blob_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_seen_trees TYPE ty_sha1_set.
    DATA lt_seen_blobs TYPE ty_sha1_set.
    DATA lt_seen_objects TYPE ty_sha1_set.
    DATA ls_commit_object TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_commit TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lt_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.

    FIELD-SYMBOLS <ls_tree_object> LIKE LINE OF lt_tree_objects.
    FIELD-SYMBOLS <ls_blob_object> LIKE LINE OF lt_blob_objects.
    FIELD-SYMBOLS <ls_node> LIKE LINE OF lt_nodes.

    " INCIDENT variant_b_d2_it8_system_no_roll_timeout: the prior pre-load-
    " everything call ("populate_cache( iv_repo_key )") issued one unbounded
    " SELECT * FROM zaog_obj_store WHERE repo_key = iv_repo_key AND status =
    " 'R' - with no row/byte limit - loading every READY object (commits,
    " trees AND full blob payloads) EVER stored for the whole repository
    " into memory before the walk below even started. For a repository whose
    " object store already holds many buffered branches (the normal,
    " intended Package C/D shared-object-store outcome), this single call
    " materializes far more data than this one commit's reachable set could
    " ever need and was reproduced causing SYSTEM_NO_ROLL (see the incident
    " artifact for dump evidence: LT_ROWS[54226x280], ~3.96 GB used memory).
    " The walk below already performs its own correctly bounded, per-level
    " get_objects( iv_bulk_fetch = abap_true ) calls (commit, then each
    " tree level, then the blob set) - each one is a normal object-store
    " read scoped to exactly that level's own SHA1 set, not the whole repo,
    " and already falls back to its own DB read on any cache miss (no
    " pre-warm required for correctness). This mirrors the sibling method
    " get_reachable_sha1s, which never called populate_cache either. No
    " other change was made to this method's tree-walk algorithm.
    APPEND iv_commit TO lt_commit_sha.
    lt_commit_objects = get_objects( iv_repo_key   = iv_repo_key
                                     it_sha1s      = lt_commit_sha
                                     iv_bulk_fetch = abap_true ).
    READ TABLE lt_commit_objects INTO ls_commit_object INDEX 1.
    IF sy-subrc <> 0 OR ls_commit_object-type <> zif_abapgit_git_definitions=>c_type-commit.
      zcx_abapgit_ortec_git=>raise( |Commit { iv_commit } not found in store| ).
    ENDIF.

    TRY.
        ls_commit = zcl_abapgit_git_pack=>decode_commit( ls_commit_object-data ).
      CATCH zcx_abapgit_exception.
        zcx_abapgit_ortec_git=>raise( |Commit { iv_commit } could not be decoded| ).
    ENDTRY.

    APPEND ls_commit_object TO rt_objects.
    INSERT ls_commit_object-sha1 INTO TABLE lt_seen_objects.

    IF ls_commit-tree IS INITIAL.
      zcx_abapgit_ortec_git=>raise( |Commit { iv_commit } has no tree| ).
    ENDIF.

    APPEND ls_commit-tree TO lt_current_trees.
    INSERT ls_commit-tree INTO TABLE lt_seen_trees.

    WHILE lt_current_trees IS NOT INITIAL.
      CLEAR lt_next_trees.
      lt_tree_objects = get_objects( iv_repo_key   = iv_repo_key
                                     it_sha1s      = lt_current_trees
                                     iv_bulk_fetch = abap_true ).

      LOOP AT lt_tree_objects ASSIGNING <ls_tree_object>.
        IF <ls_tree_object>-type <> zif_abapgit_git_definitions=>c_type-tree.
          zcx_abapgit_ortec_git=>raise( |Object { <ls_tree_object>-sha1 } is not a tree| ).
        ENDIF.

        READ TABLE lt_seen_objects WITH TABLE KEY table_line = <ls_tree_object>-sha1 TRANSPORTING NO FIELDS.
        IF sy-subrc <> 0.
          APPEND <ls_tree_object> TO rt_objects.
          INSERT <ls_tree_object>-sha1 INTO TABLE lt_seen_objects.
        ENDIF.

        TRY.
            lt_nodes = zcl_abapgit_git_pack=>decode_tree( <ls_tree_object>-data ).
          CATCH zcx_abapgit_exception.
            zcx_abapgit_ortec_git=>raise( |Tree { <ls_tree_object>-sha1 } could not be decoded| ).
        ENDTRY.

        LOOP AT lt_nodes ASSIGNING <ls_node>.
          CASE <ls_node>-chmod.
            WHEN zif_abapgit_git_definitions=>c_chmod-dir.
              READ TABLE lt_seen_trees WITH TABLE KEY table_line = <ls_node>-sha1 TRANSPORTING NO FIELDS.
              IF sy-subrc <> 0.
                INSERT <ls_node>-sha1 INTO TABLE lt_seen_trees.
                APPEND <ls_node>-sha1 TO lt_next_trees.
              ENDIF.
            WHEN zif_abapgit_git_definitions=>c_chmod-file
              OR zif_abapgit_git_definitions=>c_chmod-executable
              OR zif_abapgit_git_definitions=>c_chmod-symbolic_link.
              READ TABLE lt_seen_blobs WITH TABLE KEY table_line = <ls_node>-sha1 TRANSPORTING NO FIELDS.
              IF sy-subrc <> 0.
                INSERT <ls_node>-sha1 INTO TABLE lt_seen_blobs.
                APPEND <ls_node>-sha1 TO lt_blob_sha1s.
              ENDIF.
            WHEN zif_abapgit_git_definitions=>c_chmod-submodule.
              CONTINUE.
            WHEN OTHERS.
              zcx_abapgit_ortec_git=>raise( |Tree { <ls_tree_object>-sha1 } contains unknown chmod { <ls_node>-chmod }| ).
          ENDCASE.
        ENDLOOP.
      ENDLOOP.

      lt_current_trees = lt_next_trees.
    ENDWHILE.

    IF lt_blob_sha1s IS NOT INITIAL.
      lt_blob_objects = get_objects( iv_repo_key   = iv_repo_key
                                     it_sha1s      = lt_blob_sha1s
                                     iv_bulk_fetch = abap_true ).
      LOOP AT lt_blob_objects ASSIGNING <ls_blob_object>.
        IF <ls_blob_object>-type <> zif_abapgit_git_definitions=>c_type-blob.
          zcx_abapgit_ortec_git=>raise( |Object { <ls_blob_object>-sha1 } is not a blob| ).
        ENDIF.
        READ TABLE lt_seen_objects WITH TABLE KEY table_line = <ls_blob_object>-sha1 TRANSPORTING NO FIELDS.
        IF sy-subrc <> 0.
          APPEND <ls_blob_object> TO rt_objects.
          INSERT <ls_blob_object>-sha1 INTO TABLE lt_seen_objects.
        ENDIF.
      ENDLOOP.
    ENDIF.
  ENDMETHOD.


  METHOD get_reachable_sha1s.
    " Same commit -> tree -> blob walk as get_reachable_objects, but never
    " calls populate_cache (no full-repo preload) and never fetches blob
    " DATA - only tree/commit objects (bounded by directory structure, not
    " file content) are fetched with their data; blob presence is proven
    " via a SHA1-only existence check (get_present_sha1s).
    DATA lt_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_commit_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_current_trees TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_next_trees TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_tree_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_blob_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_seen_trees TYPE ty_sha1_set.
    DATA lt_seen_blobs TYPE ty_sha1_set.
    DATA lt_present TYPE ty_sha1_set.
    DATA ls_commit_object TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_commit TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lt_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.

    FIELD-SYMBOLS <ls_tree_object> LIKE LINE OF lt_tree_objects.
    FIELD-SYMBOLS <ls_node> LIKE LINE OF lt_nodes.
    FIELD-SYMBOLS <lv_blob> LIKE LINE OF lt_blob_sha1s.

    APPEND iv_commit TO lt_commit_sha.
    lt_commit_objects = get_objects( iv_repo_key   = iv_repo_key
                                     it_sha1s      = lt_commit_sha
                                     iv_bulk_fetch = abap_true ).
    READ TABLE lt_commit_objects INTO ls_commit_object INDEX 1.
    IF sy-subrc <> 0 OR ls_commit_object-type <> zif_abapgit_git_definitions=>c_type-commit.
      zcx_abapgit_ortec_git=>raise( |Commit { iv_commit } not found in store| ).
    ENDIF.

    TRY.
        ls_commit = zcl_abapgit_git_pack=>decode_commit( ls_commit_object-data ).
      CATCH zcx_abapgit_exception.
        zcx_abapgit_ortec_git=>raise( |Commit { iv_commit } could not be decoded| ).
    ENDTRY.

    APPEND ls_commit_object-sha1 TO rt_sha1s.

    IF ls_commit-tree IS INITIAL.
      zcx_abapgit_ortec_git=>raise( |Commit { iv_commit } has no tree| ).
    ENDIF.

    APPEND ls_commit-tree TO lt_current_trees.
    INSERT ls_commit-tree INTO TABLE lt_seen_trees.

    WHILE lt_current_trees IS NOT INITIAL.
      CLEAR lt_next_trees.
      lt_tree_objects = get_objects( iv_repo_key   = iv_repo_key
                                     it_sha1s      = lt_current_trees
                                     iv_bulk_fetch = abap_true ).

      LOOP AT lt_tree_objects ASSIGNING <ls_tree_object>.
        IF <ls_tree_object>-type <> zif_abapgit_git_definitions=>c_type-tree.
          zcx_abapgit_ortec_git=>raise( |Object { <ls_tree_object>-sha1 } is not a tree| ).
        ENDIF.

        APPEND <ls_tree_object>-sha1 TO rt_sha1s.

        TRY.
            lt_nodes = zcl_abapgit_git_pack=>decode_tree( <ls_tree_object>-data ).
          CATCH zcx_abapgit_exception.
            zcx_abapgit_ortec_git=>raise( |Tree { <ls_tree_object>-sha1 } could not be decoded| ).
        ENDTRY.

        LOOP AT lt_nodes ASSIGNING <ls_node>.
          CASE <ls_node>-chmod.
            WHEN zif_abapgit_git_definitions=>c_chmod-dir.
              READ TABLE lt_seen_trees WITH TABLE KEY table_line = <ls_node>-sha1 TRANSPORTING NO FIELDS.
              IF sy-subrc <> 0.
                INSERT <ls_node>-sha1 INTO TABLE lt_seen_trees.
                APPEND <ls_node>-sha1 TO lt_next_trees.
              ENDIF.
            WHEN zif_abapgit_git_definitions=>c_chmod-file
              OR zif_abapgit_git_definitions=>c_chmod-executable
              OR zif_abapgit_git_definitions=>c_chmod-symbolic_link.
              READ TABLE lt_seen_blobs WITH TABLE KEY table_line = <ls_node>-sha1 TRANSPORTING NO FIELDS.
              IF sy-subrc <> 0.
                INSERT <ls_node>-sha1 INTO TABLE lt_seen_blobs.
                APPEND <ls_node>-sha1 TO lt_blob_sha1s.
              ENDIF.
            WHEN zif_abapgit_git_definitions=>c_chmod-submodule.
              CONTINUE.
            WHEN OTHERS.
              zcx_abapgit_ortec_git=>raise( |Tree { <ls_tree_object>-sha1 } contains unknown chmod { <ls_node>-chmod }| ).
          ENDCASE.
        ENDLOOP.
      ENDLOOP.

      lt_current_trees = lt_next_trees.
    ENDWHILE.

    IF lt_blob_sha1s IS NOT INITIAL.
      " Existence-only: never fetch blob DATA to prove a blob is present -
      " this is the entire reason this method exists alongside
      " get_reachable_objects.
      lt_present = get_present_sha1s( iv_repo_key = iv_repo_key
                                       it_sha1s    = lt_blob_sha1s ).
      LOOP AT lt_blob_sha1s ASSIGNING <lv_blob>.
        READ TABLE lt_present WITH TABLE KEY table_line = <lv_blob> TRANSPORTING NO FIELDS.
        IF sy-subrc <> 0.
          zcx_abapgit_ortec_git=>raise( |Object { <lv_blob> } not found in store| ).
        ENDIF.
        APPEND <lv_blob> TO rt_sha1s.
      ENDLOOP.
    ENDIF.
  ENDMETHOD.


  METHOD verify_tree_closure.
    " Blob-blind commit -> tree closure walk. See declaration doc for why
    " get_reachable_sha1s/get_reachable_objects are not reused.
    DATA lt_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_commit_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_current_trees TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_next_trees TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_tree_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_seen_trees TYPE ty_sha1_set.
    DATA ls_commit_object TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_commit TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lt_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.

    FIELD-SYMBOLS <ls_tree_object> LIKE LINE OF lt_tree_objects.
    FIELD-SYMBOLS <ls_node> LIKE LINE OF lt_nodes.

    APPEND iv_commit TO lt_commit_sha.
    lt_commit_objects = get_objects( iv_repo_key   = iv_repo_key
                                     it_sha1s      = lt_commit_sha
                                     iv_bulk_fetch = abap_false ).
    READ TABLE lt_commit_objects INTO ls_commit_object INDEX 1.
    IF sy-subrc <> 0 OR ls_commit_object-type <> zif_abapgit_git_definitions=>c_type-commit.
      zcx_abapgit_ortec_git=>raise( |Commit { iv_commit } not found in store| ).
    ENDIF.

    TRY.
        ls_commit = zcl_abapgit_git_pack=>decode_commit( ls_commit_object-data ).
      CATCH zcx_abapgit_exception.
        zcx_abapgit_ortec_git=>raise( |Commit { iv_commit } could not be decoded| ).
    ENDTRY.

    IF ls_commit-tree IS INITIAL.
      zcx_abapgit_ortec_git=>raise( |Commit { iv_commit } has no tree| ).
    ENDIF.

    APPEND ls_commit-tree TO lt_current_trees.
    INSERT ls_commit-tree INTO TABLE lt_seen_trees.

    WHILE lt_current_trees IS NOT INITIAL.
      CLEAR lt_next_trees.
      lt_tree_objects = get_objects( iv_repo_key   = iv_repo_key
                                     it_sha1s      = lt_current_trees
                                     iv_bulk_fetch = abap_false ).

      LOOP AT lt_tree_objects ASSIGNING <ls_tree_object>.
        IF <ls_tree_object>-type <> zif_abapgit_git_definitions=>c_type-tree.
          zcx_abapgit_ortec_git=>raise( |Object { <ls_tree_object>-sha1 } is not a tree| ).
        ENDIF.

        TRY.
            lt_nodes = zcl_abapgit_git_pack=>decode_tree( <ls_tree_object>-data ).
          CATCH zcx_abapgit_exception.
            zcx_abapgit_ortec_git=>raise( |Tree { <ls_tree_object>-sha1 } could not be decoded| ).
        ENDTRY.

        LOOP AT lt_nodes ASSIGNING <ls_node>.
          CASE <ls_node>-chmod.
            WHEN zif_abapgit_git_definitions=>c_chmod-dir.
              READ TABLE lt_seen_trees WITH TABLE KEY table_line = <ls_node>-sha1 TRANSPORTING NO FIELDS.
              IF sy-subrc <> 0.
                INSERT <ls_node>-sha1 INTO TABLE lt_seen_trees.
                APPEND <ls_node>-sha1 TO lt_next_trees.
              ENDIF.
            WHEN zif_abapgit_git_definitions=>c_chmod-file
              OR zif_abapgit_git_definitions=>c_chmod-executable
              OR zif_abapgit_git_definitions=>c_chmod-symbolic_link.
              " Deliberately ignored - blob-blind by design, see class doc.
              CONTINUE.
            WHEN zif_abapgit_git_definitions=>c_chmod-submodule.
              CONTINUE.
            WHEN OTHERS.
              zcx_abapgit_ortec_git=>raise( |Tree { <ls_tree_object>-sha1 } contains unknown chmod { <ls_node>-chmod }| ).
          ENDCASE.
        ENDLOOP.
      ENDLOOP.

      lt_current_trees = lt_next_trees.
    ENDWHILE.

  ENDMETHOD.


  METHOD get_tip_blob_sha1s.
    " Blob-collecting counterpart to verify_tree_closure. See declaration
    " doc for why get_reachable_sha1s is not reused (it hard-requires
    " blob presence, which is exactly backwards here).
    DATA lt_commit_sha TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_commit_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_current_trees TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_next_trees TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_tree_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_seen_trees TYPE ty_sha1_set.
    DATA lt_seen_blobs TYPE ty_sha1_set.
    DATA ls_commit_object TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_commit TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lt_nodes TYPE zcl_abapgit_git_pack=>ty_nodes_tt.

    FIELD-SYMBOLS <ls_tree_object> LIKE LINE OF lt_tree_objects.
    FIELD-SYMBOLS <ls_node> LIKE LINE OF lt_nodes.

    APPEND iv_commit TO lt_commit_sha.
    lt_commit_objects = get_objects( iv_repo_key   = iv_repo_key
                                     it_sha1s      = lt_commit_sha
                                     iv_bulk_fetch = abap_false ).
    READ TABLE lt_commit_objects INTO ls_commit_object INDEX 1.
    IF sy-subrc <> 0 OR ls_commit_object-type <> zif_abapgit_git_definitions=>c_type-commit.
      zcx_abapgit_ortec_git=>raise( |Commit { iv_commit } not found in store| ).
    ENDIF.

    TRY.
        ls_commit = zcl_abapgit_git_pack=>decode_commit( ls_commit_object-data ).
      CATCH zcx_abapgit_exception.
        zcx_abapgit_ortec_git=>raise( |Commit { iv_commit } could not be decoded| ).
    ENDTRY.

    IF ls_commit-tree IS INITIAL.
      zcx_abapgit_ortec_git=>raise( |Commit { iv_commit } has no tree| ).
    ENDIF.

    APPEND ls_commit-tree TO lt_current_trees.
    INSERT ls_commit-tree INTO TABLE lt_seen_trees.

    WHILE lt_current_trees IS NOT INITIAL.
      CLEAR lt_next_trees.
      lt_tree_objects = get_objects( iv_repo_key   = iv_repo_key
                                     it_sha1s      = lt_current_trees
                                     iv_bulk_fetch = abap_false ).

      LOOP AT lt_tree_objects ASSIGNING <ls_tree_object>.
        IF <ls_tree_object>-type <> zif_abapgit_git_definitions=>c_type-tree.
          zcx_abapgit_ortec_git=>raise( |Object { <ls_tree_object>-sha1 } is not a tree| ).
        ENDIF.

        TRY.
            lt_nodes = zcl_abapgit_git_pack=>decode_tree( <ls_tree_object>-data ).
          CATCH zcx_abapgit_exception.
            zcx_abapgit_ortec_git=>raise( |Tree { <ls_tree_object>-sha1 } could not be decoded| ).
        ENDTRY.

        LOOP AT lt_nodes ASSIGNING <ls_node>.
          CASE <ls_node>-chmod.
            WHEN zif_abapgit_git_definitions=>c_chmod-dir.
              READ TABLE lt_seen_trees WITH TABLE KEY table_line = <ls_node>-sha1 TRANSPORTING NO FIELDS.
              IF sy-subrc <> 0.
                INSERT <ls_node>-sha1 INTO TABLE lt_seen_trees.
                APPEND <ls_node>-sha1 TO lt_next_trees.
              ENDIF.
            WHEN zif_abapgit_git_definitions=>c_chmod-file
              OR zif_abapgit_git_definitions=>c_chmod-executable
              OR zif_abapgit_git_definitions=>c_chmod-symbolic_link.
              READ TABLE lt_seen_blobs WITH TABLE KEY table_line = <ls_node>-sha1 TRANSPORTING NO FIELDS.
              IF sy-subrc <> 0.
                INSERT <ls_node>-sha1 INTO TABLE lt_seen_blobs.
                APPEND <ls_node>-sha1 TO rt_sha1s.
              ENDIF.
            WHEN zif_abapgit_git_definitions=>c_chmod-submodule.
              CONTINUE.
            WHEN OTHERS.
              zcx_abapgit_ortec_git=>raise( |Tree { <ls_tree_object>-sha1 } contains unknown chmod { <ls_node>-chmod }| ).
          ENDCASE.
        ENDLOOP.
      ENDLOOP.

      lt_current_trees = lt_next_trees.
    ENDWHILE.

  ENDMETHOD.


  METHOD exists.
    " TODO: variable is assigned but never used (ABAP cleaner)
    DATA lv_dummy TYPE c LENGTH 40.

    SELECT SINGLE obj_sha1 FROM zaog_obj_store
      INTO lv_dummy
      WHERE repo_key = iv_repo_key
        AND obj_sha1 = iv_sha1
        AND status   = 'R'.
    rv_exists = boolc( sy-subrc = 0 ).
  ENDMETHOD.


  METHOD get_known_commits.
    SELECT obj_sha1 FROM zaog_obj_store
      INTO TABLE rt_commits
      WHERE repo_key = iv_repo_key
        AND obj_type = 'commit'
        AND status   = 'R'.
  ENDMETHOD.


  METHOD get_all_objects.
    DATA ls_obj TYPE zif_abapgit_definitions=>ty_object.

    FIELD-SYMBOLS <ls_row> LIKE LINE OF mt_cache.

    " STRATEGY 3: Populate cache (preload small repos < 55K, handle large separately)
    populate_cache( iv_repo_key ).

    " STRATEGY 2: Return all cached objects for repo
    LOOP AT mt_cache ASSIGNING <ls_row>
         WHERE repo_key = iv_repo_key AND status = 'R'.
      CLEAR ls_obj.
      ls_obj-sha1 = <ls_row>-obj_sha1.
      ls_obj-type = <ls_row>-obj_type.
      ls_obj-data = <ls_row>-obj_data.
      APPEND ls_obj TO rt_objects.
    ENDLOOP.
  ENDMETHOD.


  METHOD clear_repo.
    DELETE FROM zaog_obj_store WHERE repo_key = iv_repo_key.
    invalidate_cache( ).
  ENDMETHOD.


  METHOD get_commit_parents.
    DATA lv_data   TYPE xstring.
    DATA lv_text   TYPE string.
    DATA lv_line   TYPE string.
    DATA lv_sha1   TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lt_lines  TYPE TABLE OF string.

    SELECT SINGLE obj_data FROM zaog_obj_store
      INTO lv_data
      WHERE repo_key = iv_repo_key
        AND obj_sha1 = iv_sha1
        AND obj_type = 'commit'
        AND status   = 'R'.
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    " Decode raw bytes to string (UTF-8 / ASCII commit header)
    TRY.
        lv_text = cl_abap_codepage=>convert_from( source   = lv_data
                                                  codepage = '4110' ). " UTF-8
      CATCH cx_parameter_invalid_range cx_sy_conversion_codepage.
        RETURN. " Commit data not valid UTF-8 — cannot parse parents
    ENDTRY.
    SPLIT lv_text AT cl_abap_char_utilities=>newline INTO TABLE lt_lines.
    LOOP AT lt_lines INTO lv_line.
      IF lv_line IS INITIAL.
        EXIT. " End of commit header
      ENDIF.
      IF strlen( lv_line ) >= 47 AND lv_line(7) = 'parent '.
        lv_sha1 = lv_line+7(40).
        APPEND lv_sha1 TO rt_parents.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.


  METHOD invalidate_cache.
    CLEAR: mt_cache,
           mv_cache_repo_key,
           mv_full_cache_repo_key,
           gv_cache_bytes.
  ENDMETHOD.


  METHOD cache_put.
    DATA lv_size TYPE int8.

    lv_size = xstrlen( is_row-obj_data ).
    IF gv_cache_bytes + lv_size > c_max_cache_bytes.
      CLEAR mt_cache.
      CLEAR gv_cache_bytes.
      CLEAR mv_full_cache_repo_key. " a partially-evicted cache is no longer "the full repo"
    ENDIF.
    INSERT is_row INTO TABLE mt_cache.
    IF sy-subrc = 0.
      gv_cache_bytes = gv_cache_bytes + lv_size.
    ENDIF.
  ENDMETHOD.


  METHOD get_timestamp.
    GET TIME STAMP FIELD rv_ts.
  ENDMETHOD.


  METHOD is_cache_valid.
    IF mv_full_cache_repo_key = iv_repo_key AND mv_full_cache_repo_key IS NOT INITIAL.
      rv_valid = abap_true.
    ENDIF.
  ENDMETHOD.


  METHOD populate_cache.
    DATA lt_rows         TYPE STANDARD TABLE OF zaog_obj_store.
    DATA ls_entry        TYPE ty_cache_entry.

    FIELD-SYMBOLS <ls_row> LIKE LINE OF lt_rows.

    " Already cached for this repo? Return.
    IF is_cache_valid( iv_repo_key ) = abap_true.
      RETURN.
    ENDIF.

    SELECT * FROM zaog_obj_store
      INTO TABLE lt_rows
      WHERE repo_key = iv_repo_key AND status = 'R'
      ORDER BY obj_sha1.

    CLEAR mt_cache.
    CLEAR gv_cache_bytes.
    mv_cache_repo_key = iv_repo_key.
    mv_full_cache_repo_key = iv_repo_key.

    LOOP AT lt_rows ASSIGNING <ls_row>.
      MOVE-CORRESPONDING <ls_row> TO ls_entry.
      INSERT ls_entry INTO TABLE mt_cache.
      gv_cache_bytes = gv_cache_bytes + xstrlen( ls_entry-obj_data ).
    ENDLOOP.
  ENDMETHOD.


  METHOD read_object_rows.
    DATA lr_sha1s TYPE RANGE OF zaog_obj_store-obj_sha1.
    FIELD-SYMBOLS <ls_sha1> LIKE LINE OF it_sha1s.

    IF it_sha1s IS INITIAL.
      RETURN.
    ENDIF.

    gv_read_object_rows_calls = gv_read_object_rows_calls + 1.

    LOOP AT it_sha1s ASSIGNING <ls_sha1>.
      APPEND VALUE #( sign   = 'I'
                      option = 'EQ'
                      low    = <ls_sha1>-sha1 ) TO lr_sha1s.
    ENDLOOP.

    SELECT * FROM zaog_obj_store
      INTO TABLE rt_rows
      WHERE repo_key = iv_repo_key
        AND obj_sha1 IN lr_sha1s
        AND status   = 'R'.
  ENDMETHOD.


  METHOD get_present_sha1s.
    DATA lt_unique_sha1s TYPE ty_sha1_set.
    DATA lr_sha1s        TYPE RANGE OF zaog_obj_store-obj_sha1.
    DATA lt_found        TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    FIELD-SYMBOLS <lv_sha1> LIKE LINE OF it_sha1s.
    FIELD-SYMBOLS <lv_found> LIKE LINE OF lt_found.

    IF iv_repo_key IS INITIAL.
      RETURN.
    ENDIF.

    LOOP AT it_sha1s ASSIGNING <lv_sha1> WHERE table_line IS NOT INITIAL.
      INSERT <lv_sha1> INTO TABLE lt_unique_sha1s.
    ENDLOOP.

    IF lt_unique_sha1s IS INITIAL.
      RETURN.
    ENDIF.

    LOOP AT lt_unique_sha1s ASSIGNING <lv_sha1>.
      APPEND VALUE #( sign   = 'I'
                      option = 'EQ'
                      low    = <lv_sha1> ) TO lr_sha1s.
      IF lines( lr_sha1s ) >= c_select_package_size.
        CLEAR lt_found.
        SELECT obj_sha1 FROM zaog_obj_store
          INTO TABLE lt_found
          WHERE repo_key = iv_repo_key
            AND obj_sha1 IN lr_sha1s
            AND status   = 'R'.
        LOOP AT lt_found ASSIGNING <lv_found>.
          INSERT <lv_found> INTO TABLE rt_present.
        ENDLOOP.
        CLEAR lr_sha1s.
      ENDIF.
    ENDLOOP.

    IF lr_sha1s IS NOT INITIAL.
      CLEAR lt_found.
      SELECT obj_sha1 FROM zaog_obj_store
        INTO TABLE lt_found
        WHERE repo_key = iv_repo_key
          AND obj_sha1 IN lr_sha1s
          AND status   = 'R'.
      LOOP AT lt_found ASSIGNING <lv_found>.
        INSERT <lv_found> INTO TABLE rt_present.
      ENDLOOP.
    ENDIF.
  ENDMETHOD.


  METHOD get_missing_sha1s.
    DATA lt_unique_sha1s TYPE ty_sha1_set.
    DATA lt_present      TYPE ty_sha1_set.

    FIELD-SYMBOLS <lv_sha1> LIKE LINE OF it_sha1s.

    IF iv_repo_key IS INITIAL.
      RETURN.
    ENDIF.

    LOOP AT it_sha1s ASSIGNING <lv_sha1> WHERE table_line IS NOT INITIAL.
      INSERT <lv_sha1> INTO TABLE lt_unique_sha1s.
    ENDLOOP.

    IF lt_unique_sha1s IS INITIAL.
      RETURN.
    ENDIF.

    " Existence-only (never loads obj_data) - see get_present_sha1s.
    lt_present = get_present_sha1s( iv_repo_key = iv_repo_key
                                     it_sha1s    = it_sha1s ).

    LOOP AT lt_unique_sha1s ASSIGNING <lv_sha1>.
      READ TABLE lt_present WITH TABLE KEY table_line = <lv_sha1>
        TRANSPORTING NO FIELDS.
      IF sy-subrc <> 0.
        APPEND <lv_sha1> TO rt_missing.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.


  METHOD has_dangling_delta_base.
    DATA lt_package TYPE ty_sha1_rows.
    DATA lt_bases   TYPE ty_sha1_set.
    DATA lt_present TYPE ty_sha1_set.
    DATA lt_db_rows TYPE ty_obj_store_tt.
    DATA lt_chunk_bases TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    FIELD-SYMBOLS <lv_sha1> LIKE LINE OF it_sha1s.
    FIELD-SYMBOLS <lv_base> LIKE LINE OF lt_bases.
    FIELD-SYMBOLS <ls_row>  LIKE LINE OF lt_db_rows.

    IF iv_repo_key IS INITIAL OR it_sha1s IS INITIAL.
      RETURN.
    ENDIF.

    " Step 1: collect the DELTA_BASE values recorded for these objects.
    LOOP AT it_sha1s ASSIGNING <lv_sha1> WHERE table_line IS NOT INITIAL.
      APPEND VALUE #( sha1 = <lv_sha1> ) TO lt_package.
      IF lines( lt_package ) >= c_select_package_size.
        CLEAR lt_chunk_bases.
        SELECT delta_base FROM zaog_pack_idx
          INTO TABLE @lt_chunk_bases
          FOR ALL ENTRIES IN @lt_package
          WHERE repo_key   = @iv_repo_key
            AND obj_sha1   = @lt_package-sha1
            AND delta_base <> @space.
        LOOP AT lt_chunk_bases INTO DATA(lv_chunk_base).
          INSERT lv_chunk_base INTO TABLE lt_bases.
        ENDLOOP.
        CLEAR lt_package.
      ENDIF.
    ENDLOOP.

    IF lt_package IS NOT INITIAL.
      CLEAR lt_chunk_bases.
      SELECT delta_base FROM zaog_pack_idx
        INTO TABLE @lt_chunk_bases
        FOR ALL ENTRIES IN @lt_package
        WHERE repo_key   = @iv_repo_key
          AND obj_sha1   = @lt_package-sha1
          AND delta_base <> @space.
      LOOP AT lt_chunk_bases INTO DATA(lv_chunk_base2).
        INSERT lv_chunk_base2 INTO TABLE lt_bases.
      ENDLOOP.
    ENDIF.

    IF lt_bases IS INITIAL.
      RETURN.
    ENDIF.

    " Step 2: verify each referenced base is present with status = 'R'
    " (reuse the existing chunked object-store reader).
    CLEAR lt_package.
    LOOP AT lt_bases ASSIGNING <lv_base>.
      APPEND VALUE #( sha1 = <lv_base> ) TO lt_package.
      IF lines( lt_package ) >= c_select_package_size.
        lt_db_rows = read_object_rows( iv_repo_key = iv_repo_key
                                       it_sha1s    = lt_package ).
        LOOP AT lt_db_rows ASSIGNING <ls_row>.
          INSERT <ls_row>-obj_sha1 INTO TABLE lt_present.
        ENDLOOP.
        CLEAR lt_package.
      ENDIF.
    ENDLOOP.

    IF lt_package IS NOT INITIAL.
      lt_db_rows = read_object_rows( iv_repo_key = iv_repo_key
                                     it_sha1s    = lt_package ).
      LOOP AT lt_db_rows ASSIGNING <ls_row>.
        INSERT <ls_row>-obj_sha1 INTO TABLE lt_present.
      ENDLOOP.
    ENDIF.

    LOOP AT lt_bases ASSIGNING <lv_base>.
      IF NOT line_exists( lt_present[ table_line = <lv_base> ] ).
        rv_dangling = abap_true.
        RETURN.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.

  METHOD verify_ready_blobs.

    TYPES:
      BEGIN OF ty_blob_meta,
        obj_sha1 TYPE zaog_obj_store-obj_sha1,
        obj_type TYPE zaog_obj_store-obj_type,
      END OF ty_blob_meta,
      ty_blob_meta_tt TYPE STANDARD TABLE OF ty_blob_meta
        WITH EMPTY KEY.

    DATA lt_unique TYPE ty_sha1_set.
    DATA lt_package TYPE ty_sha1_rows.
    DATA lt_rows TYPE ty_blob_meta_tt.
    DATA lt_found TYPE ty_sha1_set.
    DATA lt_wrong_type TYPE
      zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_missing TYPE
      zif_abapgit_git_definitions=>ty_sha1_tt.

    FIELD-SYMBOLS <lv_sha1> LIKE LINE OF it_sha1s.
    FIELD-SYMBOLS <ls_row> LIKE LINE OF lt_rows.

    IF iv_repo_key IS INITIAL.
      zcx_abapgit_ortec_git=>raise(
        'Ready-blob verification requires a repository key' ).
    ENDIF.

    LOOP AT it_sha1s ASSIGNING <lv_sha1>
         WHERE table_line IS NOT INITIAL.
      INSERT <lv_sha1> INTO TABLE lt_unique.
    ENDLOOP.

    IF lt_unique IS INITIAL.
      RETURN.
    ENDIF.

    LOOP AT lt_unique ASSIGNING <lv_sha1>.

      APPEND VALUE #( sha1 = <lv_sha1> ) TO lt_package.

      IF lines( lt_package ) >= c_select_package_size.

        CLEAR lt_rows.

        SELECT obj_sha1, obj_type
          FROM zaog_obj_store
          FOR ALL ENTRIES IN @lt_package
          WHERE repo_key = @iv_repo_key
            AND obj_sha1 = @lt_package-sha1
            AND status   = 'R'
          INTO TABLE @lt_rows.

        LOOP AT lt_rows ASSIGNING <ls_row>.
          INSERT <ls_row>-obj_sha1 INTO TABLE lt_found.

          IF <ls_row>-obj_type <>
               zif_abapgit_git_definitions=>c_type-blob.
            APPEND <ls_row>-obj_sha1 TO lt_wrong_type.
          ENDIF.
        ENDLOOP.

        CLEAR lt_package.
      ENDIF.

    ENDLOOP.

    IF lt_package IS NOT INITIAL.

      CLEAR lt_rows.

      SELECT obj_sha1, obj_type
        FROM zaog_obj_store
        FOR ALL ENTRIES IN @lt_package
        WHERE repo_key = @iv_repo_key
          AND obj_sha1 = @lt_package-sha1
          AND status   = 'R'
        INTO TABLE @lt_rows.

      LOOP AT lt_rows ASSIGNING <ls_row>.
        INSERT <ls_row>-obj_sha1 INTO TABLE lt_found.

        IF <ls_row>-obj_type <>
             zif_abapgit_git_definitions=>c_type-blob.
          APPEND <ls_row>-obj_sha1 TO lt_wrong_type.
        ENDIF.
      ENDLOOP.

    ENDIF.

    IF lt_wrong_type IS NOT INITIAL.
      zcx_abapgit_ortec_git=>raise(
        |Materialize: { lines( lt_wrong_type ) } selected object(s) | &&
        |are READY but are not blobs| ).
    ENDIF.

    LOOP AT lt_unique ASSIGNING <lv_sha1>.
      READ TABLE lt_found
        WITH TABLE KEY table_line = <lv_sha1>
        TRANSPORTING NO FIELDS.

      IF sy-subrc <> 0.
        APPEND <lv_sha1> TO lt_missing.
      ENDIF.
    ENDLOOP.

    IF lt_missing IS NOT INITIAL.
      zcx_abapgit_ortec_git=>raise(
        |Materialize: { lines( lt_missing ) } selected blob(s) | &&
        |still missing after all adaptive batches; snapshot not published| ).
    ENDIF.

  ENDMETHOD.

ENDCLASS.

