"! <p class="shorttext synchronized">ORTEC Git FastPath Orchestrator</p>
"! Entry point called from standard abapGit hooks.
"! Orchestrates incremental fetch with persistent object store.
CLASS zcl_abapgit_ortec_fastpath DEFINITION
  PUBLIC
  FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.

    "! Attempt ORTEC fast-path pull by branch.
    "! Returns INITIAL result if fast-path cannot be applied
    "! (no stored state, first fetch, etc.).
    "! @parameter iv_url |
    "! Remote URL
    "! @parameter iv_branch_name |
    "! Branch ref name
    "! @parameter iv_deepen_level |
    "! Deepen level
    "! @parameter rs_result |
    "! Pull result (INITIAL if fast-path not applicable)
    "! @raising zcx_abapgit_ortec_git |
    "! On ORTEC-specific error (caller falls back)
    CLASS-METHODS pull_by_branch
      IMPORTING iv_url           TYPE string
                iv_branch_name   TYPE string
                iv_deepen_level  TYPE i DEFAULT 1
      RETURNING VALUE(rs_result) TYPE zcl_abapgit_git_porcelain=>ty_pull_result
      RAISING   zcx_abapgit_ortec_git.

    "! Persist objects and state after a successful pull.
    "! Called as post-pull hook. Silently ignored on error.
    "! @parameter iv_url |
    "! Remote URL
    "! @parameter iv_branch_name |
    "! Branch ref name
    "! @parameter iv_commit |
    "! Fetched commit SHA1
    "! @parameter it_objects |
    "! Decoded objects
    "! @parameter iv_repo_key |
    "! Optional repo key (if known)
    "! @raising zcx_abapgit_ortec_git |
    "! On error
    CLASS-METHODS persist_pull_result
      IMPORTING iv_url         TYPE string
                iv_branch_name TYPE string
                iv_commit      TYPE zif_abapgit_git_definitions=>ty_sha1
                it_objects     TYPE zif_abapgit_definitions=>ty_objects_tt
                iv_repo_key    TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key OPTIONAL
      RAISING   zcx_abapgit_ortec_git.

  PROTECTED SECTION.
  PRIVATE SECTION.

    "! Resolve repo key from URL, looking up existing state.
    "! @parameter iv_url |
    "! Remote URL
    "! @parameter rv_key |
    "! Repository key
    CLASS-METHODS resolve_repo_key
      IMPORTING iv_url        TYPE string
      RETURNING VALUE(rv_key) TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.

ENDCLASS.


CLASS zcl_abapgit_ortec_fastpath IMPLEMENTATION.

  METHOD pull_by_branch.

    DATA lv_repo_key   TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.
    DATA ls_state      TYPE zcl_abapgit_ortec_repo_state=>ty_state.
    DATA lv_remote_sha TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA li_branches   TYPE REF TO zif_abapgit_git_branch_list.
    DATA ls_file       TYPE zif_abapgit_git_definitions=>ty_file.
    DATA lt_expanded   TYPE zif_abapgit_git_definitions=>ty_expanded_tt.

    FIELD-SYMBOLS <ls_exp>  LIKE LINE OF lt_expanded.
    FIELD-SYMBOLS <ls_blob> LIKE LINE OF rs_result-objects.

    " Check master switch
    IF zcl_abapgit_ortec_git_switch=>is_active( ) = abap_false.
      RETURN.
    ENDIF.

    " Resolve repo key
    lv_repo_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( iv_url ).
    IF lv_repo_key IS INITIAL.
      RETURN.
    ENDIF.

    " Phase 1: Check for incomplete decode session and attempt resume
    IF zcl_abapgit_ortec_git_switch=>is_decode_active( ) = abap_true.
      TRY.
          DATA lt_resumed TYPE zif_abapgit_definitions=>ty_objects_tt.
          lt_resumed = zcl_abapgit_ortec_pack_dec=>resume_decode( lv_repo_key ).
          " Resume completed (or nothing to resume) - objects are now in store
        CATCH zcx_abapgit_exception.
          " Resume failed - continue normally
      ENDTRY.
    ENDIF.

    " Phase 2: Check if remote tip matches stored state
    ls_state = zcl_abapgit_ortec_repo_state=>get_state(
      iv_repo_key    = lv_repo_key
      iv_branch_name = iv_branch_name ).
    IF ls_state-fetch_commit IS INITIAL.
      RETURN. " No previous fetch -> standard path
    ENDIF.

    " Discover remote branch tip
    TRY.
        li_branches = zcl_abapgit_git_transport=>branches( iv_url ).
        lv_remote_sha = li_branches->find_by_name( iv_branch_name )-sha1.
      CATCH zcx_abapgit_exception.
        RETURN.
    ENDTRY.

    " Remote changed -> standard path (negotiation reduces pack size)
    IF lv_remote_sha <> ls_state-fetch_commit.
      RETURN.
    ENDIF.

    " Phase 3: Reconstitute from stored objects
    rs_result-objects = zcl_abapgit_ortec_obj_store=>get_all_objects( lv_repo_key ).
    rs_result-commit  = ls_state-fetch_commit.

    IF rs_result-objects IS INITIAL.
      CLEAR rs_result.
      RETURN.
    ENDIF.

    " Phase 4: Walk tree to produce files
    TRY.
        lt_expanded = zcl_abapgit_git_porcelain=>full_tree(
          it_objects = rs_result-objects
          iv_parent  = rs_result-commit ).

        LOOP AT lt_expanded ASSIGNING <ls_exp>
          WHERE chmod = zif_abapgit_git_definitions=>c_chmod-file.
          READ TABLE rs_result-objects ASSIGNING <ls_blob>
            WITH KEY type COMPONENTS
              type = zif_abapgit_git_definitions=>c_type-blob
              sha1 = <ls_exp>-sha1.
          IF sy-subrc = 0.
            CLEAR ls_file.
            ls_file-path     = <ls_exp>-path.
            ls_file-filename = <ls_exp>-name.
            ls_file-data     = <ls_blob>-data.
            ls_file-sha1     = <ls_exp>-sha1.
            APPEND ls_file TO rs_result-files.
          ENDIF.
        ENDLOOP.

      CATCH zcx_abapgit_exception.
        CLEAR rs_result.
        RETURN.
    ENDTRY.

  ENDMETHOD.


  METHOD persist_pull_result.

    DATA lv_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.

    " Check if store is active
    IF zcl_abapgit_ortec_git_switch=>is_store_active( ) = abap_false.
      RETURN.
    ENDIF.

    " Resolve repo key
    IF iv_repo_key IS NOT INITIAL.
      lv_repo_key = iv_repo_key.
    ELSE.
      lv_repo_key = resolve_repo_key( iv_url ).
    ENDIF.

    IF lv_repo_key IS INITIAL.
      RETURN.
    ENDIF.

    " If decode persistence is active, objects were already persisted
    " during decode_and_persist in upload_pack (Hook 3).
    " We only need to store objects that aren't already in the store.
    DATA ls_dummy TYPE zaog_obj_store.
    DATA lt_new   TYPE STANDARD TABLE OF zaog_obj_store.
    DATA ls_row   TYPE zaog_obj_store.
    DATA lv_ts    TYPE timestampl.
    FIELD-SYMBOLS <ls_obj> LIKE LINE OF it_objects.

    GET TIME STAMP FIELD lv_ts.

    LOOP AT it_objects ASSIGNING <ls_obj>.
      SELECT SINGLE obj_sha1 FROM zaog_obj_store INTO ls_dummy-obj_sha1
        WHERE repo_key = lv_repo_key AND obj_sha1 = <ls_obj>-sha1.
      IF sy-subrc <> 0.
        " Not yet stored — add it
        CLEAR ls_row.
        ls_row-repo_key   = lv_repo_key.
        ls_row-obj_sha1   = <ls_obj>-sha1.
        ls_row-obj_type   = <ls_obj>-type.
        ls_row-obj_data   = <ls_obj>-data.
        ls_row-obj_size   = xstrlen( <ls_obj>-data ).
        ls_row-created_at = lv_ts.
        ls_row-status     = 'R'.
        APPEND ls_row TO lt_new.
      ENDIF.
    ENDLOOP.

    IF lt_new IS NOT INITIAL.
      MODIFY zaog_obj_store FROM TABLE lt_new.
    ENDIF.

    " Update repo state
    zcl_abapgit_ortec_repo_state=>update_after_fetch(
      iv_repo_key    = lv_repo_key
      iv_branch_name = iv_branch_name
      iv_url         = iv_url
      iv_commit      = iv_commit ).

    COMMIT WORK.

  ENDMETHOD.


  METHOD resolve_repo_key.
    " Try to find existing repo key by URL
    rv_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( iv_url ).
  ENDMETHOD.

ENDCLASS.
