"! <p class="shorttext synchronized">ORTEC Git Repository State Manager</p>
"! Manages persistent repository/branch state in ZAOG_REPO_STATE.
CLASS zcl_abapgit_ortec_repo_state DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.

    TYPES ty_repo_key TYPE c LENGTH 12.

    TYPES: BEGIN OF ty_state,
             repo_key     TYPE ty_repo_key,
             branch_name  TYPE string,
             remote_url   TYPE string,
             curr_commit  TYPE zif_abapgit_git_definitions=>ty_sha1,
             fetch_commit TYPE zif_abapgit_git_definitions=>ty_sha1,
             fetch_ts     TYPE timestampl,
             is_shallow   TYPE abap_bool,
             deepen_lvl   TYPE i,
           END OF ty_state.

    "! Get state for repo+branch.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_branch_name |
    "! Branch ref name
    "! @parameter rs_state |
    "! State record
    CLASS-METHODS get_state
      IMPORTING iv_repo_key     TYPE ty_repo_key
                iv_branch_name  TYPE string
      RETURNING VALUE(rs_state) TYPE ty_state.

    "! Update state after successful fetch.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_branch_name |
    "! Branch ref name
    "! @parameter iv_url |
    "! Remote URL
    "! @parameter iv_commit |
    "! Fetched commit SHA1
    "! @parameter iv_deepen |
    "! Deepen level
    "! @raising zcx_abapgit_ortec_git |
    "! On error
    CLASS-METHODS update_after_fetch
      IMPORTING iv_repo_key    TYPE ty_repo_key
                iv_branch_name TYPE string
                iv_url         TYPE string
                iv_commit      TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_deepen      TYPE i DEFAULT 1
      RAISING   zcx_abapgit_ortec_git.

    "! Check if state exists for repo+branch.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_branch_name |
    "! Branch ref name
    "! @parameter rv_has |
    "! ABAP_TRUE if state exists
    CLASS-METHODS has_state
      IMPORTING iv_repo_key    TYPE ty_repo_key
                iv_branch_name TYPE string
      RETURNING VALUE(rv_has)  TYPE abap_bool.

    "! Clear state for a repository.
    "! @parameter iv_repo_key |
    "! Repository key
    CLASS-METHODS clear_state
      IMPORTING iv_repo_key TYPE ty_repo_key.

    "! Reset fetch_commit for a single branch so the next fetch negotiation
    "! sends no have-lines for that branch, forcing the server to deliver
    "! a complete (non-thin) pack. The object store is NOT touched; existing
    "! objects remain as delta-base context for the fresh fetch.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter iv_branch_name |
    "! Branch ref name whose fetch_commit should be blanked
    CLASS-METHODS reset_fetch_commit
      IMPORTING iv_repo_key    TYPE ty_repo_key
                iv_branch_name TYPE string.

    "! Derive repo_key from URL.
    "! Looks up existing entries by URL hash.
    "! @parameter iv_url |
    "! Remote URL
    "! @parameter rv_key |
    "! Repository key (empty if not found)
    CLASS-METHODS get_repo_key_for_url
      IMPORTING iv_url        TYPE string
      RETURNING VALUE(rv_key) TYPE ty_repo_key.

    "! Get all fully-materialised commit SHA1s for a repository.
    "! Returns commits recorded in ZAOG_COMMIT_HIST; falls back to
    "! ZAOG_REPO_STATE fetch_commit entries if the history table is empty.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter rt_commits |
    "! Table of known-complete commit SHA1s
    CLASS-METHODS get_complete_commits
      IMPORTING iv_repo_key       TYPE ty_repo_key
      RETURNING VALUE(rt_commits) TYPE zif_abapgit_git_definitions=>ty_sha1_tt.

    "! Derive repo_key from URL, creating a new key if none exists.
    "! Uses first 12 chars of SHA1(URL) as key.
    "! @parameter iv_url |
    "! Remote URL
    "! @parameter rv_key |
    "! Repository key (always non-empty)
    CLASS-METHODS get_or_create_repo_key_for_url
      IMPORTING iv_url        TYPE string
      RETURNING VALUE(rv_key) TYPE ty_repo_key.

    "! Invalidate a tip commit from history and state so the next have-negotiation
    "! does not advertise it as fully materialised. Used when the object store
    "! is found to be tree-incomplete for this commit. Existing objects are kept
    "! as delta bases for the subsequent repair fetch.
    "! @parameter iv_repo_key | Repository key
    "! @parameter iv_commit | Commit SHA1 to de-register as complete
    "! @parameter iv_branch_name | Branch ref name (optional; if omitted, all
    "!   state rows for this commit are cleared)
    CLASS-METHODS invalidate_tip_commit
      IMPORTING iv_repo_key    TYPE ty_repo_key
                iv_commit      TYPE zif_abapgit_git_definitions=>ty_sha1
                iv_branch_name TYPE string OPTIONAL.

    "! Invalidate ALL fully-materialised commit history and ALL branches'
    "! fetch_commit pointers for this repository (not just one commit or
    "! branch). Stronger self-heal than invalidate_tip_commit/
    "! reset_fetch_commit, used when a walk failure shows the persistent
    "! store is missing objects for a commit ZAOG_COMMIT_HIST claims is
    "! complete, but it is unclear WHICH commit or shared ancestor is
    "! actually incomplete - all branches of a repo share the same have-set,
    "! so per-commit/per-branch invalidation is not reliable in that case.
    "! With no haves left to advertise, the next fetch negotiation degrades
    "! to a deepen/full-clone request, guaranteeing a complete,
    "! self-contained pack from the server. Existing objects in the
    "! persistent store are NOT deleted; they remain available as
    "! delta-base/walk-recovery context.
    "! @parameter iv_repo_key | Repository key
    CLASS-METHODS invalidate_all_history
      IMPORTING iv_repo_key TYPE ty_repo_key.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.


CLASS zcl_abapgit_ortec_repo_state IMPLEMENTATION.

  METHOD get_state.
    DATA ls_row TYPE zaog_repo_state.
    DATA lv_branch TYPE c LENGTH 255.
    lv_branch = iv_branch_name.
    SELECT SINGLE * FROM zaog_repo_state INTO ls_row
      WHERE repo_key    = iv_repo_key
        AND branch_name = lv_branch.
    IF sy-subrc = 0.
      rs_state-repo_key     = ls_row-repo_key.
      rs_state-branch_name  = ls_row-branch_name.
      rs_state-remote_url   = ls_row-remote_url.
      rs_state-curr_commit  = ls_row-curr_commit.
      rs_state-fetch_commit = ls_row-fetch_commit.
      rs_state-fetch_ts     = ls_row-fetch_ts.
      rs_state-is_shallow   = ls_row-is_shallow.
      rs_state-deepen_lvl   = ls_row-deepen_lvl.
    ENDIF.
  ENDMETHOD.

  METHOD update_after_fetch.
    DATA ls_row TYPE zaog_repo_state.
    DATA lv_ts  TYPE timestampl.
    GET TIME STAMP FIELD lv_ts.
    ls_row-repo_key     = iv_repo_key.
    ls_row-branch_name  = iv_branch_name.
    ls_row-remote_url   = iv_url.
    TRY.
        ls_row-url_hash = zcl_abapgit_hash=>sha1_string( iv_url ).
      CATCH zcx_abapgit_exception.
        ls_row-url_hash = ''.
    ENDTRY.
    ls_row-curr_commit  = iv_commit.
    ls_row-fetch_commit = iv_commit.
    ls_row-fetch_ts     = lv_ts.
    ls_row-is_shallow   = abap_true.
    ls_row-deepen_lvl   = iv_deepen.
    ls_row-changed_by   = sy-uname.
    ls_row-changed_at   = lv_ts.
    MODIFY zaog_repo_state FROM ls_row.
    IF sy-subrc <> 0.
      zcx_abapgit_ortec_git=>raise( |Failed to update repo state| ).
    ENDIF.
  ENDMETHOD.

  METHOD has_state.
    DATA lv_dummy TYPE c LENGTH 12.
    DATA lv_branch TYPE c LENGTH 255.
    lv_branch = iv_branch_name.
    SELECT SINGLE repo_key FROM zaog_repo_state INTO lv_dummy
      WHERE repo_key    = iv_repo_key
        AND branch_name = lv_branch.
    rv_has = boolc( sy-subrc = 0 ).
  ENDMETHOD.

  METHOD reset_fetch_commit.
    DATA lv_branch TYPE c LENGTH 255.
    lv_branch = iv_branch_name.
    UPDATE zaog_repo_state
      SET fetch_commit = ''
      WHERE repo_key    = iv_repo_key
        AND branch_name = lv_branch.
  ENDMETHOD.

  METHOD clear_state.
    DELETE FROM zaog_repo_state WHERE repo_key = iv_repo_key.
  ENDMETHOD.

  METHOD get_repo_key_for_url.
    DATA lv_url_hash TYPE c LENGTH 40.
    TRY.
        lv_url_hash = zcl_abapgit_hash=>sha1_string( iv_url ).
      CATCH zcx_abapgit_exception.
        RETURN.
    ENDTRY.
    SELECT SINGLE repo_key FROM zaog_repo_state INTO rv_key
      WHERE url_hash = lv_url_hash.
  ENDMETHOD.

  METHOD get_complete_commits.
    DATA lv_fc TYPE zaog_repo_state-fetch_commit.
    DATA lt_fc TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    " Primary: fully-materialised commits from history table
    SELECT DISTINCT commit_sha1 FROM zaog_commit_hist
      INTO TABLE rt_commits
      WHERE repo_key = iv_repo_key.
    IF rt_commits IS NOT INITIAL.
      RETURN.
    ENDIF.
    " Fallback: use fetch_commit entries from repo state table
    SELECT DISTINCT fetch_commit FROM zaog_repo_state
      INTO TABLE @lt_fc
      WHERE repo_key    = @iv_repo_key
        AND fetch_commit <> ''.
    LOOP AT lt_fc INTO lv_fc.
      APPEND lv_fc TO rt_commits.
    ENDLOOP.
  ENDMETHOD.

  METHOD get_or_create_repo_key_for_url.
    " Try existing lookup first
    rv_key = get_repo_key_for_url( iv_url ).
    IF rv_key IS NOT INITIAL.
      RETURN.
    ENDIF.
    " Generate new key: first 12 chars of SHA1(URL)
    DATA lv_sha TYPE c LENGTH 40.
    TRY.
        lv_sha = zcl_abapgit_hash=>sha1_string( iv_url ).
      CATCH zcx_abapgit_exception.
        lv_sha = '000000000000'.
    ENDTRY.
    rv_key = lv_sha(12).
  ENDMETHOD.

  METHOD invalidate_all_history.
    DELETE FROM zaog_commit_hist WHERE repo_key = iv_repo_key.
    UPDATE zaog_repo_state
      SET fetch_commit = ''
      WHERE repo_key = iv_repo_key.
  ENDMETHOD.

  METHOD invalidate_tip_commit.
    DATA lv_branch TYPE c LENGTH 255.
    " Remove this commit from the complete-commit history so have-negotiation
    " no longer advertises it as fully materialised.
    DELETE FROM zaog_commit_hist
      WHERE repo_key    = iv_repo_key
        AND commit_sha1 = iv_commit.
    " Blank fetch_commit in state row(s) referencing this commit so Phase 3
    " (reconstitute from store) does not fire for it again.
    IF iv_branch_name IS SUPPLIED AND iv_branch_name IS NOT INITIAL.
      lv_branch = iv_branch_name.
      UPDATE zaog_repo_state
        SET fetch_commit = ''
        WHERE repo_key    = iv_repo_key
          AND branch_name = lv_branch
          AND fetch_commit = iv_commit.
    ELSE.
      " Branch unknown - clear all state rows with this commit as tip (safe:
      " forces a fresh fetch for every branch that had it as their tip).
      UPDATE zaog_repo_state
        SET fetch_commit = ''
        WHERE repo_key    = iv_repo_key
          AND fetch_commit = iv_commit.
    ENDIF.
  ENDMETHOD.

ENDCLASS.
