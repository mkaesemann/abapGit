"! <p class="shorttext synchronized">ORTEC Git Materialization State</p>
"! Durable per-commit materialization certification and per-branch
"! denormalized snapshot readiness, backed by ZAOG_COMMIT_HIST /
"! ZAOG_REPO_STATE. Replaces tree-walk-based completeness checks with O(1)
"! keyed reads. This class issues NO COMMIT WORK anywhere - all writes
"! happen in the caller's open LUW; the calling orchestrator is
"! responsible for committing once all related object-store writes have
"! also succeeded, so a crash/rollback before that COMMIT WORK discards
"! everything a Slice-1 call did too.
CLASS zcl_abapgit_ortec_mat_state DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.
    TYPES ty_repo_key   TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.
    TYPES ty_hist_level TYPE c LENGTH 1.
    TYPES ty_snap_state TYPE c LENGTH 1.
    TYPES ty_attempt_id TYPE c LENGTH 32.

    CONSTANTS: BEGIN OF cs_hist_level,
                 unknown        TYPE ty_hist_level VALUE 'U',
                 graph_complete TYPE ty_hist_level VALUE 'G',
                 full_complete  TYPE ty_hist_level VALUE 'F',
               END OF cs_hist_level.

    CONSTANTS: BEGIN OF cs_snap_state,
                 none     TYPE ty_snap_state VALUE 'N',
                 pending  TYPE ty_snap_state VALUE 'P',
                 complete TYPE ty_snap_state VALUE 'C',
                 invalid  TYPE ty_snap_state VALUE 'I',
               END OF cs_snap_state.

    TYPES: BEGIN OF ty_state,
             repo_key    TYPE ty_repo_key,
             commit_sha1 TYPE zif_abapgit_git_definitions=>ty_sha1,
             hist_level  TYPE ty_hist_level,
             snap_state  TYPE ty_snap_state,
             attempt_id  TYPE ty_attempt_id,
             verified_at TYPE timestampl,
             updated_at  TYPE timestampl,
           END OF ty_state.

    "! Single-row keyed read. Returns an initial-state row (hist_level
    "! space, snap_state space) if no row exists - callers must treat that
    "! identically to explicit UNKNOWN/NONE, never as an error.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_commit |
    "! Commit SHA1
    "! @parameter rs_state |
    "! Materialization state record
    CLASS-METHODS get_state
      IMPORTING iv_repo_key     TYPE ty_repo_key
                iv_commit       TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rs_state) TYPE ty_state.

    "! Start (or resume) a materialization attempt for one commit. Creates
    "! the row if absent (hist_level=UNKNOWN, snap_state=NONE). Never
    "! downgrades an existing hist_level. Sets snap_state=PENDING only when
    "! it is currently NONE or INVALID (leaves COMPLETE/PENDING untouched).
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_commit |
    "! Commit SHA1
    "! @parameter rv_attempt_id |
    "! Newly generated attempt ID for this materialization attempt
    "! @raising zcx_abapgit_ortec_git |
    "! On UUID generation or persistence failure
    CLASS-METHODS begin_attempt
      IMPORTING iv_repo_key          TYPE ty_repo_key
                iv_commit            TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rv_attempt_id) TYPE ty_attempt_id
      RAISING   zcx_abapgit_ortec_git.

    "! Upgrade hist_level to GRAPH_COMPLETE. Raises if iv_attempt_id does
    "! not match the row's current attempt_id (stale/superseded attempt).
    "! No-op-safe (idempotent) if already GRAPH_COMPLETE or FULL_COMPLETE.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_commit |
    "! Commit SHA1
    "! @parameter iv_attempt_id |
    "! Attempt ID returned by begin_attempt
    "! @raising zcx_abapgit_ortec_git |
    "! No attempt in progress, stale attempt ID, or persistence failure
    CLASS-METHODS mark_graph_complete
      IMPORTING iv_repo_key   TYPE ty_repo_key
                iv_commit     TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_attempt_id TYPE ty_attempt_id
      RAISING   zcx_abapgit_ortec_git.

    "! Orchestrator-owned publication boundary: marks the commit's
    "! snapshot COMPLETE and atomically (same LUW, no COMMIT WORK) updates
    "! the branch's materialized commit pointer + denormalized snap_state.
    "! Raises unless hist_level = FULL_COMPLETE (snapshot cannot precede
    "! graph) or if iv_attempt_id is stale. Issues NO COMMIT WORK - the
    "! calling orchestrator commits once, after this call and any related
    "! object-store writes all succeed, so a crash/rollback before that
    "! single COMMIT WORK discards everything this call did too.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_branch_name |
    "! Branch whose materialized commit pointer is being published
    "! @parameter iv_commit |
    "! Commit SHA1 being published as the branch's materialized snapshot
    "! @parameter iv_attempt_id |
    "! Attempt ID returned by begin_attempt
    "! @raising zcx_abapgit_ortec_git |
    "! Snapshot precedes graph, stale attempt ID, or persistence failure
    CLASS-METHODS publish_snapshot_complete
      IMPORTING iv_repo_key    TYPE ty_repo_key
                iv_branch_name TYPE string
                iv_commit      TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_attempt_id  TYPE ty_attempt_id
      RAISING   zcx_abapgit_ortec_git.

    "! Upgrade hist_level to FULL_COMPLETE. Raises unless current
    "! hist_level is already GRAPH_COMPLETE (or FULL_COMPLETE, idempotent).
    "! Does not touch snap_state.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_commit |
    "! Commit SHA1
    "! @parameter iv_attempt_id |
    "! Attempt ID returned by begin_attempt
    "! @raising zcx_abapgit_ortec_git |
    "! No attempt in progress, not yet graph-complete, stale attempt ID,
    "! or persistence failure
    CLASS-METHODS mark_full_complete
      IMPORTING iv_repo_key   TYPE ty_repo_key
                iv_commit     TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_attempt_id TYPE ty_attempt_id
      RAISING   zcx_abapgit_ortec_git.

    "! Blunt, always-safe reset: hist_level -> UNKNOWN, snap_state ->
    "! INVALID, attempt_id cleared. verified_at is left untouched (last
    "! known-good verification time kept for diagnostics); updated_at is
    "! refreshed. Cascades to every ZAOG_REPO_STATE row of this repo whose
    "! materialized pointer (fetch_commit) equals iv_commit, forcing their
    "! denormalized snap_state to INVALID too (bounded by branch count for
    "! this repo, never repo-object-count).
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_commit |
    "! Commit SHA1 to invalidate
    "! @raising zcx_abapgit_ortec_git |
    "! On persistence failure
    CLASS-METHODS invalidate_commit
      IMPORTING iv_repo_key TYPE ty_repo_key
                iv_commit   TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING   zcx_abapgit_ortec_git.

    "! True iff hist_level IN (GRAPH_COMPLETE, FULL_COMPLETE). Replaces
    "! zcl_abapgit_ortec_fetch_neg=>is_commit_complete's tree walk with an
    "! O(1) certificate read.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_commit |
    "! Commit SHA1
    "! @parameter rv_yes |
    "! ABAP_TRUE if the commit's object graph is certified complete
    CLASS-METHODS is_graph_have_eligible
      IMPORTING iv_repo_key   TYPE ty_repo_key
                iv_commit     TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rv_yes) TYPE abap_bool.

    "! True iff hist_level = FULL_COMPLETE.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_commit |
    "! Commit SHA1
    "! @parameter rv_yes |
    "! ABAP_TRUE if the commit is fully materialized
    CLASS-METHODS is_full_have_eligible
      IMPORTING iv_repo_key   TYPE ty_repo_key
                iv_commit     TYPE zif_abapgit_git_definitions=>ty_sha1
      RETURNING VALUE(rv_yes) TYPE abap_bool.

    "! Set-based cleanup of orphaned in-flight attempts for one repository
    "! (attempt_id populated, updated_at older than iv_max_age_hours).
    "! Clears attempt_id only; does not alter hist_level/snap_state (a
    "! partially-finished attempt simply becomes retryable at its last
    "! certified level). One bulk UPDATE, never a per-row loop.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_max_age_hours |
    "! Age threshold in hours; attempts older than this are cleared
    "! @parameter rv_cleaned |
    "! Number of rows cleared
    "! @raising zcx_abapgit_ortec_git |
    "! On persistence failure
    CLASS-METHODS clean_incomplete_attempts
      IMPORTING iv_repo_key       TYPE ty_repo_key
                iv_max_age_hours  TYPE i DEFAULT 24
      RETURNING VALUE(rv_cleaned) TYPE i
      RAISING   zcx_abapgit_ortec_git.
  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.


CLASS zcl_abapgit_ortec_mat_state IMPLEMENTATION.

  METHOD get_state.
    DATA ls_row TYPE zaog_commit_hist.

    SELECT SINGLE * FROM zaog_commit_hist INTO ls_row
      WHERE repo_key    = iv_repo_key
        AND commit_sha1 = iv_commit.
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    rs_state-repo_key    = ls_row-repo_key.
    rs_state-commit_sha1 = ls_row-commit_sha1.
    rs_state-hist_level  = ls_row-hist_level.
    rs_state-snap_state  = ls_row-snap_state.
    rs_state-attempt_id  = ls_row-attempt_id.
    rs_state-verified_at = ls_row-verified_at.
    rs_state-updated_at  = ls_row-updated_at.
  ENDMETHOD.

  METHOD begin_attempt.
    DATA ls_row TYPE zaog_commit_hist.
    DATA lv_ts  TYPE timestampl.

    TRY.
        rv_attempt_id = cl_system_uuid=>create_uuid_c32_static( ).
      CATCH cx_uuid_error.
        zcx_abapgit_ortec_git=>raise( |Materialization: UUID generation failed| ).
    ENDTRY.

    GET TIME STAMP FIELD lv_ts.

    SELECT SINGLE * FROM zaog_commit_hist INTO ls_row
      WHERE repo_key    = iv_repo_key
        AND commit_sha1 = iv_commit.
    IF sy-subrc <> 0.
      CLEAR ls_row.
      ls_row-repo_key    = iv_repo_key.
      ls_row-commit_sha1 = iv_commit.
      ls_row-hist_level  = cs_hist_level-unknown.
      ls_row-snap_state  = cs_snap_state-none.
      ls_row-fetched_at  = lv_ts.
    ENDIF.

    " hist_level is never modified here - only initialized above when the
    " row was absent; an existing hist_level is never downgraded.
    IF ls_row-snap_state = cs_snap_state-none OR ls_row-snap_state = cs_snap_state-invalid.
      ls_row-snap_state = cs_snap_state-pending.
    ENDIF.

    ls_row-attempt_id = rv_attempt_id.
    ls_row-updated_at = lv_ts.

    MODIFY zaog_commit_hist FROM ls_row.
    IF sy-subrc <> 0.
      zcx_abapgit_ortec_git=>raise( |Materialization: failed to persist attempt for { iv_commit }| ).
    ENDIF.
  ENDMETHOD.

  METHOD mark_graph_complete.
    DATA ls_row TYPE zaog_commit_hist.
    DATA lv_ts  TYPE timestampl.

    SELECT SINGLE * FROM zaog_commit_hist INTO ls_row
      WHERE repo_key    = iv_repo_key
        AND commit_sha1 = iv_commit.
    IF sy-subrc <> 0.
      zcx_abapgit_ortec_git=>raise( |Materialization: no attempt in progress for { iv_commit }| ).
    ENDIF.

    IF ls_row-hist_level = cs_hist_level-graph_complete OR ls_row-hist_level = cs_hist_level-full_complete.
      RETURN. " idempotent no-op
    ENDIF.

    IF ls_row-attempt_id <> iv_attempt_id.
      zcx_abapgit_ortec_git=>raise( |Materialization: stale attempt ID for { iv_commit }| ).
    ENDIF.

    GET TIME STAMP FIELD lv_ts.
    ls_row-hist_level  = cs_hist_level-graph_complete.
    ls_row-verified_at = lv_ts.
    ls_row-updated_at  = lv_ts.

    MODIFY zaog_commit_hist FROM ls_row.
    IF sy-subrc <> 0.
      zcx_abapgit_ortec_git=>raise( |Materialization: failed to persist graph-complete state for { iv_commit }| ).
    ENDIF.
  ENDMETHOD.

  METHOD publish_snapshot_complete.
    DATA ls_row    TYPE zaog_commit_hist.
    DATA ls_repo   TYPE zaog_repo_state.
    DATA lv_branch TYPE c LENGTH 255.
    DATA lv_ts     TYPE timestampl.

    SELECT SINGLE * FROM zaog_commit_hist
      INTO ls_row
      WHERE repo_key    = iv_repo_key
        AND commit_sha1 = iv_commit.
    IF    sy-subrc          <> 0
       OR ls_row-hist_level <> cs_hist_level-full_complete.
      zcx_abapgit_ortec_git=>raise( |Materialization: snapshot requires full completion for { iv_commit }| ).
    ENDIF.

    IF ls_row-attempt_id <> iv_attempt_id.
      zcx_abapgit_ortec_git=>raise( |Materialization: stale attempt ID for { iv_commit }| ).
    ENDIF.

    GET TIME STAMP FIELD lv_ts.
    ls_row-snap_state  = cs_snap_state-complete.
    ls_row-verified_at = lv_ts.
    ls_row-updated_at  = lv_ts.


    " Diagnostic only. The certificate key remains REPO_KEY + COMMIT_SHA1
    " because one commit can be referenced by multiple branches.
    IF ls_row-branch_name IS INITIAL.
      ls_row-branch_name = iv_branch_name.
    ENDIF.

    MODIFY zaog_commit_hist FROM ls_row.
    IF sy-subrc <> 0.
      zcx_abapgit_ortec_git=>raise( |Materialization: failed to persist snapshot state for { iv_commit }| ).
    ENDIF.

    lv_branch = iv_branch_name.

    SELECT SINGLE * FROM zaog_repo_state INTO ls_repo
      WHERE repo_key    = iv_repo_key
        AND branch_name = lv_branch.
    IF sy-subrc <> 0.
      CLEAR ls_repo.
      ls_repo-repo_key    = iv_repo_key.
      ls_repo-branch_name = lv_branch.
    ENDIF.

    ls_repo-fetch_commit = iv_commit.
    ls_repo-snap_state   = cs_snap_state-complete.
    ls_repo-changed_by   = sy-uname.
    ls_repo-changed_at   = lv_ts.

    MODIFY zaog_repo_state FROM ls_repo.
    IF sy-subrc <> 0.
      zcx_abapgit_ortec_git=>raise( |Materialization: failed to publish branch pointer for { iv_commit }| ).
    ENDIF.
  ENDMETHOD.

  METHOD mark_full_complete.
    DATA ls_row TYPE zaog_commit_hist.
    DATA lv_ts  TYPE timestampl.

    SELECT SINGLE * FROM zaog_commit_hist INTO ls_row
      WHERE repo_key    = iv_repo_key
        AND commit_sha1 = iv_commit.
    IF sy-subrc <> 0.
      zcx_abapgit_ortec_git=>raise( |Materialization: no attempt in progress for { iv_commit }| ).
    ENDIF.

    IF ls_row-hist_level = cs_hist_level-full_complete.
      RETURN. " idempotent no-op
    ENDIF.

    IF ls_row-hist_level <> cs_hist_level-graph_complete.
      zcx_abapgit_ortec_git=>raise( |Materialization: full completion requires graph-complete for { iv_commit }| ).
    ENDIF.

    IF ls_row-attempt_id <> iv_attempt_id.
      zcx_abapgit_ortec_git=>raise( |Materialization: stale attempt ID for { iv_commit }| ).
    ENDIF.

    GET TIME STAMP FIELD lv_ts.
    ls_row-hist_level  = cs_hist_level-full_complete.
    ls_row-verified_at = lv_ts.
    ls_row-updated_at  = lv_ts.

    MODIFY zaog_commit_hist FROM ls_row.
    IF sy-subrc <> 0.
      zcx_abapgit_ortec_git=>raise( |Materialization: failed to persist full-complete state for { iv_commit }| ).
    ENDIF.
  ENDMETHOD.

  METHOD invalidate_commit.
    DATA ls_row TYPE zaog_commit_hist.
    DATA lv_ts  TYPE timestampl.

    GET TIME STAMP FIELD lv_ts.

    SELECT SINGLE * FROM zaog_commit_hist INTO ls_row
      WHERE repo_key    = iv_repo_key
        AND commit_sha1 = iv_commit.
    IF sy-subrc <> 0.
      CLEAR ls_row.
      ls_row-repo_key    = iv_repo_key.
      ls_row-commit_sha1 = iv_commit.
    ENDIF.

    ls_row-hist_level = cs_hist_level-unknown.
    ls_row-snap_state = cs_snap_state-invalid.
    CLEAR ls_row-attempt_id.
    ls_row-updated_at = lv_ts.

    MODIFY zaog_commit_hist FROM ls_row.
    IF sy-subrc <> 0.
      zcx_abapgit_ortec_git=>raise( |Materialization: failed to invalidate { iv_commit }| ).
    ENDIF.

    " Bounded by branch count for this one repository - repo_key is the
    " leading key component of ZAOG_REPO_STATE, never a cross-repo scan.
    UPDATE zaog_repo_state SET snap_state = cs_snap_state-invalid
                               changed_at = lv_ts
      WHERE repo_key     = iv_repo_key
        AND fetch_commit = iv_commit.
  ENDMETHOD.

  METHOD is_graph_have_eligible.
    DATA lv_hist_level TYPE ty_hist_level.

    SELECT SINGLE hist_level FROM zaog_commit_hist INTO lv_hist_level
      WHERE repo_key    = iv_repo_key
        AND commit_sha1 = iv_commit.
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    IF lv_hist_level = cs_hist_level-graph_complete OR lv_hist_level = cs_hist_level-full_complete.
      rv_yes = abap_true.
    ENDIF.
  ENDMETHOD.

  METHOD is_full_have_eligible.
    DATA lv_hist_level TYPE ty_hist_level.

    SELECT SINGLE hist_level FROM zaog_commit_hist INTO lv_hist_level
      WHERE repo_key    = iv_repo_key
        AND commit_sha1 = iv_commit.
    IF sy-subrc <> 0.
      RETURN.
    ENDIF.

    IF lv_hist_level = cs_hist_level-full_complete.
      rv_yes = abap_true.
    ENDIF.
  ENDMETHOD.

  METHOD clean_incomplete_attempts.
    DATA lv_cutoff TYPE timestampl.

    GET TIME STAMP FIELD lv_cutoff.

    TRY.
        lv_cutoff = cl_abap_tstmp=>subtractsecs(
          tstmp = lv_cutoff
          secs  = iv_max_age_hours * 3600 ).
      CATCH cx_parameter_invalid_range cx_parameter_invalid_type.
        zcx_abapgit_ortec_git=>raise( |Materialization: invalid max age { iv_max_age_hours }| ).
    ENDTRY.

    " Single set-based statement - never a per-row loop.
    UPDATE zaog_commit_hist SET attempt_id = space
      WHERE repo_key   = iv_repo_key
        AND attempt_id <> space
        AND updated_at < lv_cutoff.

    rv_cleaned = sy-dbcnt.
  ENDMETHOD.

ENDCLASS.
