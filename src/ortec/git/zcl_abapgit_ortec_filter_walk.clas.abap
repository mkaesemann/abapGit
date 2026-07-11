CLASS zcl_abapgit_ortec_filter_walk DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    "! Resolve filtered remote files for stage-by-transport.
    "! The method validates cached ORTEC commit state against current remote tip and
    "! falls back to standard get_files_remote when fast-path preconditions are not met.
    "! @parameter ii_repo_online |
    "! Repository instance from stage logic
    "! @parameter ii_obj_filter |
    "! Object filter for staged comparison
    "! @parameter rt_files |
    "! Filtered remote files
    "! @raising zcx_abapgit_exception |
    "! Raised only when both fast and fallback paths fail
    CLASS-METHODS get_remote_files_for_stage
      IMPORTING
        ii_repo_online TYPE REF TO zif_abapgit_repo
        ii_obj_filter  TYPE REF TO zif_abapgit_object_filter
      RETURNING
        VALUE(rt_files) TYPE zif_abapgit_git_definitions=>ty_files_tt
      RAISING
        zcx_abapgit_exception.

    "! Resolve filtered remote files for the diff single-object/file flow.
    "! Uses the same fast-path/fallback behavior as stage filtering.
    "! @parameter ii_repo_online |
    "! Repository instance from diff page
    "! @parameter ii_obj_filter |
    "! Object filter for single object/file diff
    "! @parameter rt_files |
    "! Filtered remote files
    "! @raising zcx_abapgit_exception |
    "! Raised only when both fast and fallback paths fail
    CLASS-METHODS get_remote_files_for_diff
      IMPORTING
        ii_repo_online TYPE REF TO zif_abapgit_repo
        ii_obj_filter  TYPE REF TO zif_abapgit_object_filter
      RETURNING
        VALUE(rt_files) TYPE zif_abapgit_git_definitions=>ty_files_tt
      RAISING
        zcx_abapgit_exception.

    "! Resolve filtered files for a known commit/repository key.
    "! The object table parameter is kept for compatibility with existing callers.
    "! @parameter iv_commit |
    "! Commit SHA1
    "! @parameter it_objects |
    "! Currently unused compatibility parameter
    "! @parameter ii_obj_filter |
    "! Object filter used by stage-by-transport flow
    "! @parameter io_dot |
    "! Loaded .abapgit configuration
    "! @parameter iv_devclass |
    "! Repository package/devclass
    "! @parameter iv_repo_key |
    "! ORTEC object-store repository key
    "! @parameter rt_files |
    "! Filtered remote files
    "! @raising zcx_abapgit_exception |
    "! Raised on index or fallback resolution errors
    CLASS-METHODS pull_filtered
      IMPORTING
        iv_commit      TYPE zif_abapgit_git_definitions=>ty_sha1
        it_objects     TYPE zif_abapgit_definitions=>ty_objects_tt
        ii_obj_filter  TYPE REF TO zif_abapgit_object_filter
        io_dot         TYPE REF TO zcl_abapgit_dot_abapgit
        iv_devclass    TYPE devclass
        iv_repo_key    TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key OPTIONAL
      RETURNING
        VALUE(rt_files) TYPE zif_abapgit_git_definitions=>ty_files_tt
      RAISING
        zcx_abapgit_exception.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.


CLASS zcl_abapgit_ortec_filter_walk IMPLEMENTATION.

  METHOD get_remote_files_for_diff.
    rt_files = get_remote_files_for_stage(
      ii_repo_online = ii_repo_online
      ii_obj_filter  = ii_obj_filter ).
  ENDMETHOD.

  METHOD get_remote_files_for_stage.
    DATA li_repo_online TYPE REF TO zif_abapgit_repo_online.
    DATA lv_url         TYPE string.
    DATA lv_branch      TYPE string.
    DATA lv_commit      TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_repo_key    TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.
    DATA ls_state       TYPE zcl_abapgit_ortec_repo_state=>ty_state.
    DATA li_branches    TYPE REF TO zif_abapgit_git_branch_list.
    DATA ls_branch      TYPE zif_abapgit_git_definitions=>ty_git_branch.

    TRY.
        li_repo_online ?= ii_repo_online.
      CATCH cx_sy_move_cast_error.
        rt_files = ii_repo_online->get_files_remote( ii_obj_filter ).
        RETURN.
    ENDTRY.

    TRY.
        lv_url = li_repo_online->get_url( ).

        lv_repo_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( lv_url ).

        IF lv_repo_key IS INITIAL.
          rt_files = ii_repo_online->get_files_remote( ii_obj_filter ).
          RETURN.
        ENDIF.

        lv_commit = li_repo_online->get_selected_commit( ).

        IF lv_commit IS INITIAL.
          lv_branch = li_repo_online->get_selected_branch( ).
          ls_state = zcl_abapgit_ortec_repo_state=>get_state(
            iv_repo_key    = lv_repo_key
            iv_branch_name = lv_branch ).
          lv_commit = ls_state-fetch_commit.

          IF lv_commit IS INITIAL.
            rt_files = ii_repo_online->get_files_remote( ii_obj_filter ).
            RETURN.
          ENDIF.

          TRY.
              li_branches = zcl_abapgit_git_transport=>branches( lv_url ).
              ls_branch = li_branches->find_by_name( lv_branch ).
            CATCH zcx_abapgit_exception.
              CLEAR ls_branch.
          ENDTRY.

          IF ls_branch-sha1 IS INITIAL OR ls_branch-sha1 <> lv_commit.
            rt_files = ii_repo_online->get_files_remote( ii_obj_filter ).
            RETURN.
          ENDIF.
        ENDIF.

        rt_files = zcl_abapgit_ortec_obj_index=>get_files_for_filter(
          iv_repo_key   = lv_repo_key
          iv_commit     = lv_commit
          ii_obj_filter = ii_obj_filter
          io_dot        = ii_repo_online->get_dot_abapgit( )
          iv_devclass   = ii_repo_online->get_package( )
          iv_url        = lv_url ).
      CATCH zcx_abapgit_exception.
        rt_files = ii_repo_online->get_files_remote( ii_obj_filter ).
    ENDTRY.
  ENDMETHOD.


  METHOD pull_filtered.
    DATA lv_commit TYPE zif_abapgit_git_definitions=>ty_sha1.

    IF iv_repo_key IS INITIAL.
      zcx_abapgit_exception=>raise( 'Filtered walk requires repository key' ).
    ENDIF.

    lv_commit = iv_commit.
    IF lv_commit IS INITIAL.
      zcx_abapgit_exception=>raise( 'Filtered walk requires commit SHA1' ).
    ENDIF.

    rt_files = zcl_abapgit_ortec_obj_index=>get_files_for_filter(
      iv_repo_key   = iv_repo_key
      iv_commit     = lv_commit
      ii_obj_filter = ii_obj_filter
      io_dot        = io_dot
      iv_devclass   = iv_devclass ).
  ENDMETHOD.

ENDCLASS.
