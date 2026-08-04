CLASS zcl_abapgit_ortec_porcelain DEFINITION
  PUBLIC FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    TYPES:
      BEGIN OF ty_pull_result,
        files   TYPE zif_abapgit_git_definitions=>ty_files_tt,
        objects TYPE zif_abapgit_definitions=>ty_objects_tt,
        commit  TYPE zif_abapgit_git_definitions=>ty_sha1,
      END OF ty_pull_result.

    CLASS-METHODS pull_by_branch
      IMPORTING iv_url           TYPE string
                iv_branch_name   TYPE string
                iv_deepen_level  TYPE i      DEFAULT 1
                iv_pull_url      TYPE string OPTIONAL
      RETURNING VALUE(rs_result) TYPE ty_pull_result
      RAISING   zcx_abapgit_exception.

    CLASS-METHODS pull_by_commit
      IMPORTING iv_url           TYPE string
                iv_commit_hash   TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_deepen_level  TYPE i      DEFAULT 1
                iv_pull_url      TYPE string OPTIONAL
      RETURNING VALUE(rs_result) TYPE ty_pull_result
      RAISING   zcx_abapgit_exception.

    "! Buffer-aware equivalent of ZCL_ABAPGIT_GIT_PORCELAIN=>full_tree - reads
    "! the parent commit from IT_OBJECTS if present, otherwise ZAOG_OBJ_STORE,
    "! then walks its tree via the existing buffer-aware WALK_TREE. This is
    "! the fix for the "tree not found" push/commit failure: a PULL that took
    "! ORTEC's WARM_UNCHANGED/COLD_BRANCH path only seeds the commit object
    "! itself (not the full tree/blob graph) into IT_OBJECTS, which the plain
    "! ZCL_ABAPGIT_GIT_PORCELAIN=>full_tree cannot see past.
    CLASS-METHODS full_tree
      IMPORTING it_objects         TYPE zif_abapgit_definitions=>ty_objects_tt    OPTIONAL
                iv_parent          TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_url             TYPE string                                    OPTIONAL
                iv_repo_key        TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key OPTIONAL
      RETURNING VALUE(rt_expanded) TYPE zif_abapgit_git_definitions=>ty_expanded_tt
      RAISING   zcx_abapgit_exception.

    "! ORTEC-aware equivalent of ZCL_ABAPGIT_GIT_PORCELAIN=>push - identical
    "! stage-merge/tree-build/network-push orchestration, routed here so an
    "! ORTEC-active repo never falls through to the plain FULL_TREE/WALK_TREE
    "! (see FULL_TREE doc). Uses this class's own BUILD_TREES/
    "! RECEIVE_PACK_PUSH clones (private, below) instead of the standard
    "! class's - kept as a deliberate duplicate rather than widening the
    "! standard class's visibility, to minimize the diff against upstream
    "! abapGit and keep future updates to it low-friction.
    CLASS-METHODS push
      IMPORTING is_comment       TYPE zif_abapgit_git_definitions=>ty_comment
                io_stage         TYPE REF TO zcl_abapgit_stage
                it_old_objects   TYPE zif_abapgit_definitions=>ty_objects_tt
                iv_parent        TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_url           TYPE string
                iv_branch_name   TYPE string
      RETURNING VALUE(rs_result) TYPE zcl_abapgit_git_porcelain=>ty_push_result
      RAISING   zcx_abapgit_exception.

  PRIVATE SECTION.
    " Clones of ZCL_ABAPGIT_GIT_PORCELAIN's own private TY_TREE/TY_TREES_TT/
    " FIND_FOLDERS/BUILD_TREES/RECEIVE_PACK_PUSH (unchanged logic - pure
    " tree-encoding/pack-transport, no object-graph dependency) - duplicated
    " here rather than widened to PUBLIC on the standard class, to keep the
    " diff against upstream abapGit minimal.
    TYPES:
      BEGIN OF ty_tree,
        path TYPE string,
        data TYPE xstring,
        sha1 TYPE zif_abapgit_git_definitions=>ty_sha1,
      END OF ty_tree.
    TYPES ty_trees_tt TYPE STANDARD TABLE OF ty_tree WITH DEFAULT KEY.
    TYPES:
      BEGIN OF ty_folder,
        path  TYPE string,
        count TYPE i,
        sha1  TYPE zif_abapgit_git_definitions=>ty_sha1,
      END OF ty_folder.
    TYPES ty_folders_tt TYPE STANDARD TABLE OF ty_folder WITH DEFAULT KEY.

    CLASS-METHODS find_folders
      IMPORTING it_expanded       TYPE zif_abapgit_git_definitions=>ty_expanded_tt
      RETURNING VALUE(rt_folders) TYPE ty_folders_tt.

    CLASS-METHODS build_trees
      IMPORTING it_expanded     TYPE zif_abapgit_git_definitions=>ty_expanded_tt
      RETURNING VALUE(rt_trees) TYPE ty_trees_tt
      RAISING   zcx_abapgit_exception.

    CLASS-METHODS receive_pack_push
      IMPORTING is_comment     TYPE zif_abapgit_git_definitions=>ty_comment
                it_trees       TYPE ty_trees_tt
                it_blobs       TYPE zif_abapgit_git_definitions=>ty_files_tt
                iv_parent      TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_parent2     TYPE zif_abapgit_git_definitions=>ty_sha1 OPTIONAL
                iv_url         TYPE string
                iv_branch_name TYPE string
      EXPORTING ev_new_commit  TYPE zif_abapgit_git_definitions=>ty_sha1
                et_new_objects TYPE zif_abapgit_definitions=>ty_objects_tt
                ev_new_tree    TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING   zcx_abapgit_exception.

    CLASS-METHODS pull
      IMPORTING iv_commit         TYPE zif_abapgit_git_definitions=>ty_sha1
                it_objects        TYPE zif_abapgit_definitions=>ty_objects_tt
                iv_repo_key       TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key OPTIONAL
                iv_url            TYPE string                                   OPTIONAL
                ii_progress       TYPE REF TO zif_abapgit_progress               OPTIONAL
      RETURNING VALUE(rt_files)   TYPE zif_abapgit_git_definitions=>ty_files_tt
      RAISING   zcx_abapgit_exception.

    "! No-op if ii_progress is unbound; swallows zcx_abapgit_exception from
    "! show() - progress reporting must never be able to fail a pull.
    CLASS-METHODS report_progress
      IMPORTING ii_progress TYPE REF TO zif_abapgit_progress OPTIONAL
                iv_current  TYPE i
                iv_text     TYPE string.

    CLASS-METHODS walk
      IMPORTING it_objects       TYPE zif_abapgit_definitions=>ty_objects_tt
                iv_sha1          TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_path          TYPE string
                iv_repo_key      TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key    OPTIONAL
                iv_url           TYPE string                                      OPTIONAL
                iv_commit        TYPE zif_abapgit_git_definitions=>ty_sha1        OPTIONAL
                it_blob_objects  TYPE zif_abapgit_definitions=>ty_objects_tt      OPTIONAL
                it_blob_manifest TYPE zif_abapgit_git_definitions=>ty_expanded_tt OPTIONAL
      CHANGING  ct_files         TYPE zif_abapgit_git_definitions=>ty_files_tt
      RAISING   zcx_abapgit_exception.

    CLASS-METHODS walk_tree
      IMPORTING it_objects         TYPE zif_abapgit_definitions=>ty_objects_tt
                iv_tree            TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_base            TYPE string
                iv_repo_key        TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key OPTIONAL
      RETURNING VALUE(rt_expanded) TYPE zif_abapgit_git_definitions=>ty_expanded_tt
      RAISING   zcx_abapgit_exception.

    CLASS-METHODS materialize_from_manifest
      IMPORTING it_objects       TYPE zif_abapgit_definitions=>ty_objects_tt
                it_blob_manifest TYPE zif_abapgit_git_definitions=>ty_expanded_tt
      CHANGING  ct_files         TYPE zif_abapgit_git_definitions=>ty_files_tt
      RAISING   zcx_abapgit_exception.
ENDCLASS.


CLASS zcl_abapgit_ortec_porcelain IMPLEMENTATION.
  METHOD report_progress.
    IF ii_progress IS NOT BOUND.
      RETURN.
    ENDIF.
    TRY.
        ii_progress->show( iv_current = iv_current iv_text = iv_text ).
      CATCH zcx_abapgit_exception.
        " Progress reporting is never allowed to fail the pull itself.
    ENDTRY.
  ENDMETHOD.

  METHOD pull.
    DATA lt_blob_manifest   TYPE zif_abapgit_git_definitions=>ty_expanded_tt.
    DATA ls_object          TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_commit          TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lv_root_tree       TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lt_blob_sha1s      TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_objects         TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_blob_manifest   LIKE LINE OF lt_blob_manifest.
    DATA lt_remaining_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_batch_objects   TYPE zif_abapgit_definitions=>ty_objects_tt.

    READ TABLE it_objects INTO ls_object
         WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-commit
                                  sha1 = iv_commit.
    IF sy-subrc <> 0.
      zcx_abapgit_exception=>raise( 'Commit/Branch not found.' ).
    ENDIF.

    ls_commit = zcl_abapgit_git_pack=>decode_commit( ls_object-data ).
    lv_root_tree = ls_commit-tree.

    report_progress( ii_progress = ii_progress iv_current = 1
      iv_text = 'Git: reading objects from buffer (ZAOG_OBJ_STORE)' ).

    TRY.
        lt_blob_sha1s = zcl_abapgit_ortec_walk_prep=>prewarm(
                          EXPORTING
                            iv_repo_key  = iv_repo_key
                            iv_commit    = iv_commit
                            iv_url       = iv_url
                            iv_root_tree = lv_root_tree
                            it_objects   = it_objects
                          CHANGING
                            ct_objects   = lt_objects ).
      CATCH zcx_abapgit_ortec_git INTO DATA(lx_prewarm).
        zcx_abapgit_exception=>raise_with_text( lx_prewarm ).
    ENDTRY.

    DATA(lt_objects_complete) = VALUE zif_abapgit_definitions=>ty_objects_tt( ( LINES OF it_objects )
                                                                              ( LINES OF lt_objects ) ).

    report_progress( ii_progress = ii_progress iv_current = 1
      iv_text = 'Git: walking file tree' ).

    lt_blob_manifest = walk_tree(
                           it_objects  = lt_objects_complete
                           iv_tree     = lv_root_tree
                           iv_base     = '/'
                           iv_repo_key = iv_repo_key ).

    CLEAR lt_blob_sha1s.
    LOOP AT lt_blob_manifest INTO ls_blob_manifest.
      IF ls_blob_manifest-chmod <> zif_abapgit_git_definitions=>c_chmod-file.
        CONTINUE.
      ENDIF.

      IF NOT line_exists( lt_objects_complete[
                              KEY type
                              type = zif_abapgit_git_definitions=>c_type-blob
                              sha1 = ls_blob_manifest-sha1 ] ).
        " Only blobs NOT already resident in it_objects need batch-fetching -
        " walk_tree's manifest lists EVERY reachable blob unconditionally, so
        " without this filter a COMPLETE (non-sparse) it_objects would still
        " force the batching path for every blob in the repo, defeating the
        " no-op equivalence for today's standard full-pull case and risking
        " an infinite/empty-yield loop for a first-ever pull whose blobs
        " live only in it_objects, not yet in the persistent object store.
        APPEND ls_blob_manifest-sha1 TO lt_blob_sha1s.
      ENDIF.
    ENDLOOP.

    materialize_from_manifest(
      EXPORTING
        it_objects       = lt_objects_complete
        it_blob_manifest = lt_blob_manifest
      CHANGING
        ct_files         = rt_files ).

    SORT lt_blob_sha1s.
    DELETE ADJACENT DUPLICATES FROM lt_blob_sha1s.

    IF lt_blob_sha1s IS NOT INITIAL.
      report_progress( ii_progress = ii_progress iv_current = 1
        iv_text = |Git: reading { lines( lt_blob_sha1s ) } blobs from buffer (ZAOG_OBJ_STORE)| ).
    ENDIF.

    lt_remaining_sha1s = lt_blob_sha1s.
    WHILE lt_remaining_sha1s IS NOT INITIAL.
      CLEAR lt_batch_objects.
      TRY.
          lt_batch_objects = zcl_abapgit_ortec_walk_prep=>fetch_blobs_bulk(
                               EXPORTING
                                 iv_repo_key        = iv_repo_key
                                 it_sha1s           = lt_remaining_sha1s
                               CHANGING
                                 ct_remaining_sha1s = lt_remaining_sha1s ).
        CATCH zcx_abapgit_ortec_git INTO DATA(lx_fetch_blobs).
          zcx_abapgit_exception=>raise_with_text( lx_fetch_blobs ).
      ENDTRY.

      materialize_from_manifest(
        EXPORTING
          it_objects       = lt_batch_objects
          it_blob_manifest = lt_blob_manifest
        CHANGING
          ct_files         = rt_files ).
    ENDWHILE.
  ENDMETHOD.

  METHOD pull_by_branch.
    DATA lv_ortec_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.
    DATA lx_pull           TYPE REF TO zcx_abapgit_exception.
    DATA lv_pull_error     TYPE string.
    DATA lv_deepen_used    TYPE i.
    DATA lv_ortec_active   TYPE abap_bool.
    DATA lv_target_commit  TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_op_class       TYPE zcl_abapgit_ortec_have_policy=>ty_op_class.
    DATA lv_backfilled     TYPE abap_bool.
    DATA ls_seed_object    TYPE zif_abapgit_definitions=>ty_object.
    DATA lt_seed_objects   TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_tip_blob_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv_porc_lock_id   TYPE zcl_abapgit_ortec_pack_dec=>ty_session_id.
    DATA lv_porc_lock_held TYPE abap_bool.
    DATA lv_porc_attempt_id TYPE zcl_abapgit_ortec_mat_state=>ty_attempt_id.

    " Progress ownership: this method (via ZCL_ABAPGIT_GIT_PORCELAIN's own
    " ORTEC routing hook) is the true orchestration boundary for an
    " ORTEC-enabled Pull - there is no ii_progress parameter threaded down
    " from the standard callers (ZCL_ABAPGIT_REPO_ONLINE/ZCL_ABAPGIT_GIT_
    " PORCELAIN), which is deliberately left unmodified. get_instance(1)
    " returns the same session-global singleton the standard caller already
    " obtained for its own "Fetch remote files" message - reusing it here
    " only resets iv_total (already 1) and throttle timestamps, both inert
    " in the current no-throttle diagnostic build.
    DATA(li_progress) = zcl_abapgit_progress=>get_instance( 1 ).
    report_progress( ii_progress = li_progress iv_current = 1 iv_text = 'Git: checking local cache' ).

    lv_ortec_active = zcl_abapgit_ortec_git_switch=>is_active_for_repo( iv_url ).

    IF lv_ortec_active = abap_true.
      lv_ortec_repo_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( iv_url ).
    ELSE.
      lv_ortec_repo_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( iv_url ).
    ENDIF.

    " Package C C2 (design §1/§2/§3): classify BEFORE any upload-pack POST,
    " using the advertised remote branch tip resolved via one lightweight
    " info/refs GET - disclosed-but-accepted redundant with the cascade's
    " own GET below (design §2). ORTEC-inactive or never-seen repos fall
    " through with the default INCREMENTAL_UPDATE classification, which is
    " exactly this method's unconditional pre-C2 behavior - standard
    " abapGit behavior is unchanged when ORTEC is disabled.
    lv_op_class = zcl_abapgit_ortec_have_policy=>cs_op_class-incremental_update.

    IF lv_ortec_active = abap_true AND lv_ortec_repo_key IS NOT INITIAL.
      lv_target_commit = zcl_abapgit_git_transport=>branches( iv_url )->find_by_name( iv_branch_name )-sha1.

      IF lv_target_commit IS NOT INITIAL.
        lv_op_class = zcl_abapgit_ortec_have_policy=>classify_operation(
          iv_repo_key      = lv_ortec_repo_key
          iv_target_commit = lv_target_commit ).

        IF lv_op_class = zcl_abapgit_ortec_have_policy=>cs_op_class-cold_branch.
          " At most one opportunistic, bounded, local-only backfill attempt
          " (design §3a) - never retried within the same call.
          TRY.
              lv_backfilled = zcl_abapgit_ortec_have_policy=>try_backfill_target(
                iv_repo_key      = lv_ortec_repo_key
                iv_target_commit = lv_target_commit
                iv_branch_name   = iv_branch_name ).
            CATCH zcx_abapgit_ortec_git INTO DATA(lx_backfill).
              zcx_abapgit_exception=>raise_with_text( lx_backfill ).
          ENDTRY.

          IF lv_backfilled = abap_true.
            lv_op_class = zcl_abapgit_ortec_have_policy=>classify_operation(
              iv_repo_key      = lv_ortec_repo_key
              iv_target_commit = lv_target_commit ).
          ENDIF.
        ENDIF.
      ENDIF.
    ENDIF.

    CASE lv_op_class.
      WHEN zcl_abapgit_ortec_have_policy=>cs_op_class-warm_unchanged.
        " Design §3 rule 1/§4: already SNAPSHOT_COMPLETE locally - no
        " upload-pack POST, no Package B network call, no re-certification.
        " Seed the existing PULL/WALK/WALK_TREE reconstruction with exactly
        " one bounded commit-object read; WALK_TREE/WALK's own bulk
        " PREWARM/FETCH_BLOBS_BULK calls (inside PULL) avoid any
        " repository-wide or per-object read for the reachable set.
        report_progress( ii_progress = li_progress iv_current = 1
          iv_text = 'Git: reusing cached objects (no fetch needed)' ).

        TRY.
            ls_seed_object = zcl_abapgit_ortec_obj_store=>get_object(
              iv_repo_key = lv_ortec_repo_key
              iv_sha1     = lv_target_commit ).
          CATCH zcx_abapgit_ortec_git INTO DATA(lx_warm_seed).
            zcx_abapgit_exception=>raise_with_text( lx_warm_seed ).
        ENDTRY.
        CLEAR lt_seed_objects.
        APPEND ls_seed_object TO lt_seed_objects.

        rs_result-commit  = lv_target_commit.
        rs_result-objects = lt_seed_objects.
        rs_result-files   = pull(
                                iv_commit   = lv_target_commit
                                it_objects  = lt_seed_objects
                                iv_repo_key = lv_ortec_repo_key
                                iv_url      = iv_pull_url
                                ii_progress = li_progress ).

        report_progress( ii_progress = li_progress iv_current = 1
          iv_text = |Git: completed ({ lines( rs_result-files ) } files, cache reuse)| ).
        RETURN.

      WHEN zcl_abapgit_ortec_have_policy=>cs_op_class-cold_branch.
        " Design §4/§6: still cold after the single backfill attempt above
        " - route through the validated Package B cold-graph + tip-snapshot
        " APIs (each verifies and certifies/commits its own work; no
        " duplicate certification is performed here), then reconstruct
        " locally exactly like WARM_UNCHANGED - no separate cold
        " reconstruction implementation.
        report_progress( ii_progress = li_progress iv_current = 1
          iv_text = 'Git: initializing repository from remote (cold start)' ).

        TRY.
            CLEAR lt_tip_blob_sha1s.
            zcl_abapgit_ortec_cold_init=>acquire_blobless_graph(
              EXPORTING
                iv_url        = iv_url
                iv_repo_key   = lv_ortec_repo_key
                iv_tip_commit = lv_target_commit
              IMPORTING
                et_tip_blob_sha1s = lt_tip_blob_sha1s ).

            report_progress( ii_progress = li_progress iv_current = 1
              iv_text = |Git: materializing tip snapshot ({ lines( lt_tip_blob_sha1s ) } blobs)| ).

            zcl_abapgit_ortec_cold_init=>materialize_tip_snapshot(
              iv_url            = iv_url
              iv_repo_key       = lv_ortec_repo_key
              iv_branch_name    = iv_branch_name
              iv_tip_commit     = lv_target_commit
              it_tip_blob_sha1s = lt_tip_blob_sha1s ).
            ls_seed_object = zcl_abapgit_ortec_obj_store=>get_object(
              iv_repo_key = lv_ortec_repo_key
              iv_sha1     = lv_target_commit ).
          CATCH zcx_abapgit_ortec_git INTO DATA(lx_cold).
            zcx_abapgit_exception=>raise_with_text( lx_cold ).
        ENDTRY.
        CLEAR lt_seed_objects.
        APPEND ls_seed_object TO lt_seed_objects.

        rs_result-commit  = lv_target_commit.
        rs_result-objects = lt_seed_objects.
        rs_result-files   = pull(
                                iv_commit   = lv_target_commit
                                it_objects  = lt_seed_objects
                                iv_repo_key = lv_ortec_repo_key
                                iv_url      = iv_pull_url
                                ii_progress = li_progress ).

        report_progress( ii_progress = li_progress iv_current = 1
          iv_text = |Git: completed ({ lines( rs_result-files ) } files, cold start)| ).
        RETURN.
    ENDCASE.

    " INCREMENTAL_UPDATE (including the ORTEC-inactive/no-repo-key
    " fallback): unchanged thin -> self-contained -> at most one recovery
    " cascade. zcl_abapgit_git_transport=>upload_pack_by_branch routes to
    " zcl_abapgit_ortec_fastpath=>upload_pack_by_branch when ORTEC is
    " active, which already applies C1's certified-have policy.
    report_progress( ii_progress = li_progress iv_current = 1
      iv_text = 'Git: requesting objects from remote' ).

    zcl_abapgit_git_transport=>upload_pack_by_branch(
      EXPORTING
        iv_url          = iv_url
        iv_branch_name  = iv_branch_name
        iv_deepen_level = iv_deepen_level
      IMPORTING
        et_objects      = rs_result-objects
        ev_branch       = rs_result-commit
        ev_deepen_used  = lv_deepen_used ).

    report_progress( ii_progress = li_progress iv_current = 1
      iv_text = |Git: reconstructing files ({ lines( rs_result-objects ) } objects fetched)| ).

    TRY.
        rs_result-files = pull(
                              iv_commit   = rs_result-commit
                              it_objects  = rs_result-objects
                              iv_repo_key = lv_ortec_repo_key
                              iv_url      = iv_pull_url
                              ii_progress = li_progress ).
      CATCH zcx_abapgit_exception INTO lx_pull.
        lv_pull_error = lx_pull->get_text( ).
        IF     zcl_abapgit_ortec_git_switch=>is_active_for_repo( iv_url )  = abap_true
           AND lv_ortec_repo_key IS NOT INITIAL
           AND lv_pull_error CS zcl_abapgit_ortec_git_switch=>c_walk_error_prefix.
          " The walk failed because the persistent store has some objects
          " but not every blob/tree reachable from the fetched commit, even
          " though ZAOG_COMMIT_HIST/ZAOG_REPO_STATE claim otherwise for at
          " least one advertised have. Repair strategy: invalidate ALL
          " recorded history/have-state for the whole repo (not just this
          " branch's fetch_commit) so the retry cannot advertise ANY commit
          " as already complete, forcing the server to fall back to a
          " full/deepen, self-contained pack. Per-branch/per-commit
          " invalidation is not reliable here because haves are shared
          " across all branches of a repo, and we don't know which shared
          " ancestor is actually incomplete.
          " The object store itself is kept intact: its objects still serve
          " as delta-base context inside decode_and_persist, and other
          " branches cached for the same repo simply redo have-negotiation
          " on their own next fetch.
          TRY.
              zcl_abapgit_ortec_repo_state=>invalidate_all_history( iv_repo_key = lv_ortec_repo_key ).
              COMMIT WORK.

              CLEAR rs_result.
              report_progress( ii_progress = li_progress iv_current = 1
                iv_text = 'Git: retrying with full history (recovery)' ).

              zcl_abapgit_git_transport=>upload_pack_by_branch(
                EXPORTING
                  iv_url          = iv_url
                  iv_branch_name  = iv_branch_name
                  iv_deepen_level = iv_deepen_level
                IMPORTING
                  et_objects      = rs_result-objects
                  ev_branch       = rs_result-commit
                  ev_deepen_used  = lv_deepen_used ).

              rs_result-files = pull(
                                    iv_commit   = rs_result-commit
                                    it_objects  = rs_result-objects
                                    iv_repo_key = lv_ortec_repo_key
                                    iv_url      = iv_pull_url
                                    ii_progress = li_progress ).
            CATCH zcx_abapgit_ortec_git.
              RAISE EXCEPTION lx_pull.
            CATCH zcx_abapgit_exception.
              RAISE EXCEPTION lx_pull.
          ENDTRY.
        ENDIF.
        RAISE EXCEPTION lx_pull.
    ENDTRY.

    report_progress( ii_progress = li_progress iv_current = 1
      iv_text = |Git: completed ({ lines( rs_result-files ) } files)| ).

    " ORTEC: persist objects in persistent store after successful pull - this
    " mirrors the standard zcl_abapgit_git_porcelain=>pull_by_branch's own
    " post-pull persistence hook exactly. Without this call, the persistent
    " object store/index (ZAOG_OBJ_STORE/ZAOG_OBJ_INDEX/ZAOG_REPO_STATE) never
    " learns about files pulled through this mirror, leaving the Stage/Diff/
    " status overview's filtered read path comparing against a STALE snapshot
    " that predates this pull - the exact cause of a live bug (2026-07-17)
    " where freshly-pulled, unchanged classes were wrongly shown with a
    " "deleted in remote" status badge, even though a direct diff correctly
    " reported no differences. Package C C2: this call now also runs the
    " full certification lifecycle (see
    " zcl_abapgit_ortec_fastpath=>persist_pull_result) - reached only by
    " this INCREMENTAL_UPDATE branch, never by WARM_UNCHANGED/COLD_BRANCH.
    " ORTEC D2b2: acquire the canonical repo lock and mint an attempt id for
    " this Publication Unit #2 span. The lock/attempt setup deliberately
    " starts HERE (not before the preceding upload_pack_by_branch/pull(...)
    " calls above) and wraps ONLY the persist_pull_result call below -
    " lock/attempt failures are non-critical (graceful degrade, same
    " pattern as the existing persist_pull_result TRY/CATCH one line down).
    " Guarded on a non-initial repo key: never acquire the canonical lock
    " with a blank/session-global key (invariant) - persist_pull_result
    " itself already handles a blank key by returning early with no writes.
    IF lv_ortec_repo_key IS NOT INITIAL.
      TRY.
          lv_porc_lock_id = zcl_abapgit_ortec_pack_dec=>acquire_repo_lock( iv_repo_key = lv_ortec_repo_key ).
          lv_porc_lock_held = abap_true.
          lv_porc_attempt_id = zcl_abapgit_ortec_mat_state=>begin_attempt(
                                    iv_repo_key = lv_ortec_repo_key
                                    iv_commit   = rs_result-commit ).
        CATCH zcx_abapgit_exception zcx_abapgit_ortec_git.
          " Lock or attempt-id acquisition failed - persist_pull_result below
          " still runs (it mints its own attempt id internally when none is
          " supplied), just without this caller's explicit correlation.
          CLEAR lv_porc_attempt_id.
      ENDTRY.
    ENDIF.

    TRY.
        zcl_abapgit_ortec_fastpath=>persist_pull_result(
          iv_url         = iv_url
          iv_branch_name = iv_branch_name
          iv_commit      = rs_result-commit
          it_objects     = rs_result-objects
          iv_repo_key    = lv_ortec_repo_key
          iv_deepen_used = lv_deepen_used
          iv_attempt_id  = lv_porc_attempt_id ).
      CATCH zcx_abapgit_ortec_git.
        " ORTEC: persistence failure is non-critical, continue normally
    ENDTRY.

    " Release unconditionally - covers both the success path above and the
    " already-caught-exception path. Never spans back into
    " upload_pack_by_branch/pull(...) (acquired only just above).
    IF lv_porc_lock_held = abap_true.
      zcl_abapgit_ortec_pack_dec=>release_repo_lock( lv_porc_lock_id ).
      CLEAR lv_porc_lock_held.
    ENDIF.
  ENDMETHOD.

  METHOD pull_by_commit.
    DATA lv_ortec_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.

    " Same singleton-reuse rationale as pull_by_branch - this is the true
    " orchestration boundary for pull-by-commit, no ii_progress is threaded
    " down from the standard (unmodified) caller.
    DATA(li_progress) = zcl_abapgit_progress=>get_instance( 1 ).
    report_progress( ii_progress = li_progress iv_current = 1
      iv_text = 'Git: requesting commit from remote' ).

    zcl_abapgit_git_transport=>upload_pack_by_commit(
      EXPORTING
        iv_url          = iv_url
        iv_hash         = iv_commit_hash
        iv_deepen_level = iv_deepen_level
      IMPORTING
        et_objects      = rs_result-objects
        ev_commit       = rs_result-commit ).

    IF zcl_abapgit_ortec_git_switch=>is_active_for_repo( iv_url ) = abap_true.
      lv_ortec_repo_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( iv_url ).
    ELSE.
      lv_ortec_repo_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( iv_url ).
    ENDIF.

    report_progress( ii_progress = li_progress iv_current = 1
      iv_text = |Git: reconstructing files ({ lines( rs_result-objects ) } objects fetched)| ).

    rs_result-files = pull(
                          iv_commit   = rs_result-commit
                          it_objects  = rs_result-objects
                          iv_repo_key = lv_ortec_repo_key
                          iv_url      = iv_pull_url
                          ii_progress = li_progress ).

    report_progress( ii_progress = li_progress iv_current = 1
      iv_text = |Git: completed ({ lines( rs_result-files ) } files)| ).
  ENDMETHOD.

  METHOD full_tree.
    DATA lv_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.
    DATA ls_object   TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_commit   TYPE zcl_abapgit_git_pack=>ty_commit.

    lv_repo_key = iv_repo_key.
    IF lv_repo_key IS INITIAL AND iv_url IS NOT INITIAL.
      lv_repo_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( iv_url ).
    ENDIF.

    READ TABLE it_objects INTO ls_object
         WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-commit
                                  sha1 = iv_parent.
    IF sy-subrc <> 0.
      TRY.
          ls_object = zcl_abapgit_ortec_obj_store=>get_object(
                          iv_repo_key = lv_repo_key
                          iv_sha1     = iv_parent ).
          IF ls_object-type <> zif_abapgit_git_definitions=>c_type-commit.
            zcx_abapgit_exception=>raise( 'commit not found' ).
          ENDIF.
        CATCH zcx_abapgit_ortec_git INTO DATA(lx_commit).
          zcx_abapgit_exception=>raise_with_text( lx_commit ).
      ENDTRY.
    ENDIF.

    ls_commit = zcl_abapgit_git_pack=>decode_commit( ls_object-data ).

    rt_expanded = walk_tree( it_objects  = it_objects
                             iv_tree     = ls_commit-tree
                             iv_base     = '/'
                             iv_repo_key = lv_repo_key ).
  ENDMETHOD.

  METHOD push.
    DATA lv_repo_key   TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.
    DATA lt_expanded   TYPE zif_abapgit_git_definitions=>ty_expanded_tt.
    DATA lt_blobs      TYPE zif_abapgit_git_definitions=>ty_files_tt.
    DATA lv_sha1       TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lt_stage      TYPE zif_abapgit_definitions=>ty_stage_tt.
    DATA lv_new_tree   TYPE zif_abapgit_git_definitions=>ty_sha1.

    FIELD-SYMBOLS <ls_stage>   LIKE LINE OF lt_stage.
    FIELD-SYMBOLS <ls_updated> LIKE LINE OF rs_result-updated_files.
    FIELD-SYMBOLS <ls_exp>     LIKE LINE OF lt_expanded.

    lv_repo_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( iv_url ).

    lt_expanded = full_tree( it_objects  = it_old_objects
                             iv_parent   = iv_parent
                             iv_repo_key = lv_repo_key ).

    lt_stage = io_stage->get_all( ).
    LOOP AT lt_stage ASSIGNING <ls_stage>.

      APPEND INITIAL LINE TO rs_result-updated_files ASSIGNING <ls_updated>.
      MOVE-CORRESPONDING <ls_stage>-file TO <ls_updated>.

      CASE <ls_stage>-method.
        WHEN zif_abapgit_definitions=>c_method-add.

          APPEND <ls_stage>-file TO lt_blobs.

          READ TABLE lt_expanded ASSIGNING <ls_exp> WITH TABLE KEY path_name COMPONENTS
            name = <ls_stage>-file-filename
            path = <ls_stage>-file-path.
          IF sy-subrc <> 0. " new files
            APPEND INITIAL LINE TO lt_expanded ASSIGNING <ls_exp>.
            <ls_exp>-name  = <ls_stage>-file-filename.
            <ls_exp>-path  = <ls_stage>-file-path.
            <ls_exp>-chmod = zif_abapgit_git_definitions=>c_chmod-file.
          ENDIF.

          lv_sha1 = zcl_abapgit_hash=>sha1_blob( <ls_stage>-file-data ).
          IF <ls_exp>-sha1 <> lv_sha1.
            <ls_exp>-sha1 = lv_sha1.
          ENDIF.

          <ls_updated>-sha1 = lv_sha1.   "New sha1

        WHEN zif_abapgit_definitions=>c_method-rm.
          READ TABLE lt_expanded ASSIGNING <ls_exp> WITH TABLE KEY path_name COMPONENTS
            name = <ls_stage>-file-filename
            path = <ls_stage>-file-path.
          ASSERT sy-subrc = 0.

          CLEAR <ls_exp>-sha1.           " Mark as deleted
          CLEAR <ls_updated>-sha1.       " Mark as deleted

        WHEN OTHERS.
          zcx_abapgit_exception=>raise( 'stage method not supported, todo' ).
      ENDCASE.
    ENDLOOP.

    DELETE lt_expanded WHERE sha1 IS INITIAL.

    DATA(lt_trees) = build_trees( lt_expanded ).

    receive_pack_push(
      EXPORTING
        is_comment     = is_comment
        it_trees       = lt_trees
        iv_branch_name = iv_branch_name
        iv_url         = iv_url
        iv_parent      = iv_parent
        iv_parent2     = io_stage->get_merge_source( )
        it_blobs       = lt_blobs
      IMPORTING
        ev_new_commit  = rs_result-branch
        et_new_objects = rs_result-new_objects
        ev_new_tree    = lv_new_tree ).

    IF rs_result IS SUPPLIED.
      APPEND LINES OF it_old_objects TO rs_result-new_objects.

      " Buffer-aware WALK (not the plain one) - the new tree only carries
      " NEWLY created tree/blob objects (see RECEIVE_PACK_PUSH); every
      " unchanged sibling file's blob still only exists in ZAOG_OBJ_STORE
      " when the base tree came from FULL_TREE's buffer fallback above.
      walk( EXPORTING it_objects  = rs_result-new_objects
                      iv_sha1     = lv_new_tree
                      iv_path     = '/'
                      iv_repo_key = lv_repo_key
            CHANGING  ct_files    = rs_result-new_files ).
    ENDIF.
  ENDMETHOD.

  METHOD find_folders.

    DATA: lt_paths TYPE TABLE OF string,
          lv_split TYPE string,
          lv_path  TYPE string.

    FIELD-SYMBOLS: <ls_folder> LIKE LINE OF rt_folders,
                   <ls_new>    LIKE LINE OF rt_folders,
                   <ls_exp>    LIKE LINE OF it_expanded.

    LOOP AT it_expanded ASSIGNING <ls_exp>.
      READ TABLE rt_folders WITH KEY path = <ls_exp>-path TRANSPORTING NO FIELDS.
      IF sy-subrc <> 0.
        APPEND INITIAL LINE TO rt_folders ASSIGNING <ls_folder>.
        <ls_folder>-path = <ls_exp>-path.
      ENDIF.
    ENDLOOP.

* add empty folders
    LOOP AT rt_folders ASSIGNING <ls_folder>.
      SPLIT <ls_folder>-path AT '/' INTO TABLE lt_paths.

      CLEAR lv_path.
      LOOP AT lt_paths INTO lv_split.
        CONCATENATE lv_path lv_split '/' INTO lv_path.
        READ TABLE rt_folders WITH KEY path = lv_path TRANSPORTING NO FIELDS.
        IF sy-subrc <> 0.
          APPEND INITIAL LINE TO rt_folders ASSIGNING <ls_new>.
          <ls_new>-path = lv_path.
        ENDIF.
      ENDLOOP.
    ENDLOOP.

    LOOP AT rt_folders ASSIGNING <ls_folder>.
      FIND ALL OCCURRENCES OF '/' IN <ls_folder>-path MATCH COUNT <ls_folder>-count.
    ENDLOOP.
  ENDMETHOD.

  METHOD build_trees.
    DATA: lt_nodes   TYPE zcl_abapgit_git_pack=>ty_nodes_tt,
          ls_tree    LIKE LINE OF rt_trees,
          lv_len     TYPE i,
          lt_folders TYPE ty_folders_tt.

    FIELD-SYMBOLS: <ls_folder> LIKE LINE OF lt_folders,
                   <ls_node>   LIKE LINE OF lt_nodes,
                   <ls_sub>    LIKE LINE OF lt_folders,
                   <ls_exp>    LIKE LINE OF it_expanded.

    lt_folders = find_folders( it_expanded ).

* start with the deepest folders
    SORT lt_folders BY count DESCENDING.

    LOOP AT lt_folders ASSIGNING <ls_folder>.
      CLEAR lt_nodes.

* files
      LOOP AT it_expanded ASSIGNING <ls_exp> USING KEY path_name WHERE path = <ls_folder>-path.
        APPEND INITIAL LINE TO lt_nodes ASSIGNING <ls_node>.
        <ls_node>-chmod = <ls_exp>-chmod.
        <ls_node>-name  = <ls_exp>-name.
        <ls_node>-sha1  = <ls_exp>-sha1.
      ENDLOOP.

* folders
      LOOP AT lt_folders ASSIGNING <ls_sub> WHERE count = <ls_folder>-count + 1.
        lv_len = strlen( <ls_folder>-path ).
        IF strlen( <ls_sub>-path ) > lv_len AND <ls_sub>-path(lv_len) = <ls_folder>-path.
          APPEND INITIAL LINE TO lt_nodes ASSIGNING <ls_node>.
          <ls_node>-chmod = zif_abapgit_git_definitions=>c_chmod-dir.

* extract folder name, this can probably be done easier using regular expressions
          <ls_node>-name = <ls_sub>-path+lv_len.
          lv_len = strlen( <ls_node>-name ) - 1.
          <ls_node>-name = <ls_node>-name(lv_len).

          <ls_node>-sha1 = <ls_sub>-sha1.
        ENDIF.
      ENDLOOP.

      CLEAR ls_tree.
      ls_tree-path = <ls_folder>-path.
      ls_tree-data = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
      ls_tree-sha1 = zcl_abapgit_hash=>sha1_tree( ls_tree-data ).
      APPEND ls_tree TO rt_trees.

      <ls_folder>-sha1 = ls_tree-sha1.
    ENDLOOP.
  ENDMETHOD.

  METHOD receive_pack_push.
    DATA: lv_time   TYPE zcl_abapgit_git_time=>ty_unixtime,
          lv_commit TYPE xstring,
          lv_pack   TYPE xstring,
          ls_object LIKE LINE OF et_new_objects,
          ls_commit TYPE zcl_abapgit_git_pack=>ty_commit,
          lv_uindex TYPE sy-index.

    FIELD-SYMBOLS: <ls_tree> LIKE LINE OF it_trees,
                   <ls_blob> LIKE LINE OF it_blobs.

    lv_time = zcl_abapgit_git_time=>get_unix( ).

    READ TABLE it_trees ASSIGNING <ls_tree> WITH KEY path = '/'.
    ASSERT sy-subrc = 0.

* new commit
    ls_commit-committer = |{ is_comment-committer-name
      } <{ is_comment-committer-email }> { lv_time }|.
    IF is_comment-author-name IS NOT INITIAL.
      ls_commit-author = |{ is_comment-author-name
        } <{ is_comment-author-email }> { lv_time }|.
    ELSE.
      ls_commit-author = ls_commit-committer.
    ENDIF.

    ls_commit-tree      = <ls_tree>-sha1.
    ls_commit-parent    = iv_parent.
    ls_commit-parent2   = iv_parent2.
    ls_commit-body      = is_comment-comment.
    lv_commit = zcl_abapgit_git_pack=>encode_commit( ls_commit ).

    ls_object-sha1 = zcl_abapgit_hash=>sha1_commit( lv_commit ).
    ls_object-type = zif_abapgit_git_definitions=>c_type-commit.
    ls_object-data = lv_commit.
    APPEND ls_object TO et_new_objects.

    LOOP AT it_trees ASSIGNING <ls_tree>.
      CLEAR ls_object.
      ls_object-sha1 = <ls_tree>-sha1.

      READ TABLE et_new_objects
        WITH KEY type COMPONENTS
          type = zif_abapgit_git_definitions=>c_type-tree
          sha1 = ls_object-sha1
        TRANSPORTING NO FIELDS.
      IF sy-subrc = 0.
* two identical trees added at the same time, only add one to the pack
        CONTINUE.
      ENDIF.

      ls_object-type = zif_abapgit_git_definitions=>c_type-tree.
      ls_object-data = <ls_tree>-data.
      lv_uindex = lv_uindex + 1.
      ls_object-index = lv_uindex.
      APPEND ls_object TO et_new_objects.
    ENDLOOP.

    LOOP AT it_blobs ASSIGNING <ls_blob>.
      CLEAR ls_object.
      ls_object-sha1 = zcl_abapgit_hash=>sha1_blob( <ls_blob>-data ).

      READ TABLE et_new_objects
        WITH KEY type COMPONENTS
          type = zif_abapgit_git_definitions=>c_type-blob
          sha1 = ls_object-sha1
        TRANSPORTING NO FIELDS.
      IF sy-subrc = 0.
* two identical files added at the same time, only add one blob to the pack
        CONTINUE.
      ENDIF.

      ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
* note <ls_blob>-data can be empty, #1857 allow empty files - some more checks needed?
      ls_object-data = <ls_blob>-data.
      lv_uindex = lv_uindex + 1.
      ls_object-index = lv_uindex.
      APPEND ls_object TO et_new_objects.
    ENDLOOP.

    lv_pack = zcl_abapgit_git_pack=>encode( et_new_objects ).

    ev_new_commit = zcl_abapgit_hash=>sha1_commit( lv_commit ).

    zcl_abapgit_git_transport=>receive_pack(
      iv_url         = iv_url
      iv_old         = iv_parent
      iv_new         = ev_new_commit
      iv_branch_name = iv_branch_name
      iv_pack        = lv_pack ).

    ev_new_tree = ls_commit-tree.
  ENDMETHOD.

  METHOD materialize_from_manifest.
    DATA ls_ortec_object        TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_file                LIKE LINE OF ct_files.
    DATA ls_blob_manifest       LIKE LINE OF it_blob_manifest.
    DATA lt_blob_object_lookup  TYPE HASHED TABLE OF zif_abapgit_definitions=>ty_object WITH UNIQUE KEY sha1.
    DATA lt_blob_sha1_lookup    TYPE HASHED TABLE OF zif_abapgit_git_definitions=>ty_sha1 WITH UNIQUE KEY table_line.

    FIELD-SYMBOLS <ls_blob> LIKE LINE OF it_objects.

    LOOP AT it_objects ASSIGNING <ls_blob>.
      IF <ls_blob>-type <> zif_abapgit_git_definitions=>c_type-blob.
        CONTINUE.
      ENDIF.
      IF <ls_blob>-sha1 IS INITIAL.
        CONTINUE.
      ENDIF.
      INSERT <ls_blob> INTO TABLE lt_blob_object_lookup.
      INSERT <ls_blob>-sha1 INTO TABLE lt_blob_sha1_lookup.
    ENDLOOP.

    LOOP AT it_blob_manifest INTO ls_blob_manifest.
      IF ls_blob_manifest-chmod <> zif_abapgit_git_definitions=>c_chmod-file.
        CONTINUE.
      ENDIF.

      IF NOT line_exists( lt_blob_sha1_lookup[ table_line = ls_blob_manifest-sha1 ] ).
        CONTINUE.
      ENDIF.

      READ TABLE lt_blob_object_lookup INTO ls_ortec_object
           WITH TABLE KEY sha1 = ls_blob_manifest-sha1.
      IF sy-subrc <> 0.
        CONTINUE.
      ENDIF.

      CLEAR ls_file.
      ls_file-path = ls_blob_manifest-path.
      ls_file-filename = ls_blob_manifest-name.
      ls_file-data = ls_ortec_object-data.
      ls_file-sha1 = ls_ortec_object-sha1.
      APPEND ls_file TO ct_files.
    ENDLOOP.
  ENDMETHOD.

  METHOD walk.
    DATA lt_nodes        TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_ortec_object TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_file_path    TYPE string.
    DATA lv_path         TYPE string.

    FIELD-SYMBOLS <ls_node> LIKE LINE OF lt_nodes.

    DATA ls_file          LIKE LINE OF ct_files.
    DATA ls_blob_manifest LIKE LINE OF it_blob_manifest.

    FIELD-SYMBOLS <ls_tree> LIKE LINE OF it_objects.
    FIELD-SYMBOLS <ls_blob> LIKE LINE OF it_objects.

    READ TABLE it_objects ASSIGNING <ls_tree>
         WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-tree
                                  sha1 = iv_sha1.
    IF sy-subrc = 0.
      lt_nodes = zcl_abapgit_git_pack=>decode_tree( <ls_tree>-data ).
    ELSE.
      TRY.
          ls_ortec_object = zcl_abapgit_ortec_obj_store=>get_object(
                                iv_repo_key = iv_repo_key
                                iv_sha1     = iv_sha1 ).
          IF ls_ortec_object-type <> zif_abapgit_git_definitions=>c_type-tree.
            zcx_abapgit_exception=>raise( |{ zcl_abapgit_ortec_git_switch=>c_walk_error_prefix } tree not found| ).
          ENDIF.
          lt_nodes = zcl_abapgit_git_pack=>decode_tree( ls_ortec_object-data ).
        CATCH zcx_abapgit_ortec_git.
          zcx_abapgit_exception=>raise( |{ zcl_abapgit_ortec_git_switch=>c_walk_error_prefix } tree not found| ).
      ENDTRY.
    ENDIF.

    LOOP AT lt_nodes ASSIGNING <ls_node>.
      IF <ls_node>-chmod <> zif_abapgit_git_definitions=>c_chmod-file.
        CONTINUE.
      ENDIF.

      CLEAR ls_file.
      lv_file_path = iv_path.
      ls_file-filename = <ls_node>-name.
      READ TABLE it_blob_manifest INTO ls_blob_manifest
           WITH KEY sha1 = <ls_node>-sha1.
      IF sy-subrc = 0.
        lv_file_path = ls_blob_manifest-path.
        ls_file-filename = ls_blob_manifest-name.
      ENDIF.

      IF it_blob_objects IS NOT INITIAL.
        READ TABLE it_blob_objects INTO ls_ortec_object
             WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-blob
                                      sha1 = <ls_node>-sha1.
        IF sy-subrc = 0.
          ls_file-path = lv_file_path.
          ls_file-data = ls_ortec_object-data.
          ls_file-sha1 = ls_ortec_object-sha1.
          APPEND ls_file TO ct_files.
          CONTINUE.
        ENDIF.
        CONTINUE.
      ENDIF.

      READ TABLE it_objects ASSIGNING <ls_blob>
           WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-blob
                                    sha1 = <ls_node>-sha1.
      IF sy-subrc = 0.
        ls_file-path = lv_file_path.
        ls_file-data = <ls_blob>-data.
        ls_file-sha1 = <ls_blob>-sha1.
        APPEND ls_file TO ct_files.
        CONTINUE.
      ENDIF.

      TRY.
          ls_ortec_object = zcl_abapgit_ortec_obj_store=>get_object(
                                iv_repo_key = iv_repo_key
                                iv_sha1     = <ls_node>-sha1 ).
          IF ls_ortec_object-type <> zif_abapgit_git_definitions=>c_type-blob.
            zcx_abapgit_exception=>raise( |{ zcl_abapgit_ortec_git_switch=>c_walk_error_prefix } blob not found| ).
          ENDIF.
          ls_file-path = lv_file_path.
          ls_file-data = ls_ortec_object-data.
          ls_file-sha1 = ls_ortec_object-sha1.
          APPEND ls_file TO ct_files.
          CONTINUE.
        CATCH zcx_abapgit_ortec_git.
          zcx_abapgit_exception=>raise( |{ zcl_abapgit_ortec_git_switch=>c_walk_error_prefix } blob not found| ).
      ENDTRY.
    ENDLOOP.

    LOOP AT lt_nodes ASSIGNING <ls_node> WHERE chmod = zif_abapgit_git_definitions=>c_chmod-dir.
      CONCATENATE iv_path <ls_node>-name '/' INTO lv_path.

      walk(
        EXPORTING
          it_objects       = it_objects
          iv_sha1          = <ls_node>-sha1
          iv_path          = lv_path
          iv_repo_key      = iv_repo_key
          iv_url           = iv_url
          iv_commit        = iv_commit
          it_blob_objects  = it_blob_objects
          it_blob_manifest = it_blob_manifest
        CHANGING
          ct_files         = ct_files ).
    ENDLOOP.
  ENDMETHOD.

  METHOD walk_tree.
    DATA lt_nodes        TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_object       TYPE zif_abapgit_definitions=>ty_object.
    DATA ls_ortec_object TYPE zif_abapgit_definitions=>ty_object.
    DATA lt_expanded     TYPE zif_abapgit_git_definitions=>ty_expanded_tt.

    FIELD-SYMBOLS <ls_node> LIKE LINE OF lt_nodes.
    FIELD-SYMBOLS <ls_exp>  LIKE LINE OF rt_expanded.

    READ TABLE it_objects INTO ls_object
         WITH KEY type COMPONENTS type = zif_abapgit_git_definitions=>c_type-tree
                                  sha1 = iv_tree.
    IF sy-subrc = 0.
      lt_nodes = zcl_abapgit_git_pack=>decode_tree( ls_object-data ).
    ELSE.
      TRY.
          ls_ortec_object = zcl_abapgit_ortec_obj_store=>get_object(
                                iv_repo_key = iv_repo_key
                                iv_sha1     = iv_tree ).
          IF ls_ortec_object-type <> zif_abapgit_git_definitions=>c_type-tree.
            zcx_abapgit_exception=>raise( 'walk_tree, tree not found' ).
          ENDIF.
          lt_nodes = zcl_abapgit_git_pack=>decode_tree( ls_ortec_object-data ).
        CATCH zcx_abapgit_ortec_git.
          zcx_abapgit_exception=>raise( 'tree not found' ).
      ENDTRY.
    ENDIF.

    LOOP AT lt_nodes ASSIGNING <ls_node>.
      CASE <ls_node>-chmod.
        WHEN zif_abapgit_git_definitions=>c_chmod-file
            OR zif_abapgit_git_definitions=>c_chmod-executable
            OR zif_abapgit_git_definitions=>c_chmod-symbolic_link
            OR zif_abapgit_git_definitions=>c_chmod-submodule.
          APPEND INITIAL LINE TO rt_expanded ASSIGNING <ls_exp>.
          <ls_exp>-path  = iv_base.
          <ls_exp>-name  = <ls_node>-name.
          <ls_exp>-sha1  = <ls_node>-sha1.
          <ls_exp>-chmod = <ls_node>-chmod.
        WHEN zif_abapgit_git_definitions=>c_chmod-dir.
          lt_expanded = walk_tree(
                            it_objects  = it_objects
                            iv_tree     = <ls_node>-sha1
                            iv_base     = |{ iv_base }{ <ls_node>-name }/|
                            iv_repo_key = iv_repo_key ).
          APPEND LINES OF lt_expanded TO rt_expanded.
        WHEN OTHERS.
          zcx_abapgit_exception=>raise( |walk_tree: unknown chmod { <ls_node>-chmod }| ).
      ENDCASE.
    ENDLOOP.
  ENDMETHOD.
ENDCLASS.

