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

    " Check master switch
    IF zcl_abapgit_ortec_git_switch=>is_active( ) = abap_false.
      RETURN. " INITIAL - caller continues with standard
    ENDIF.

    " For the first implementation phase, fast-path pull
    " delegates to standard abapGit (returns INITIAL).
    " The value comes from want/have negotiation (Hook 2)
    " and post-pull persistence (Hook 6) being active.
    " A full fast-path reconstitution from the object store
    " will be implemented in a later phase.
    RETURN.

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
      " No repo key resolved - cannot persist without identity
      RETURN.
    ENDIF.

    " Store all objects
    zcl_abapgit_ortec_obj_store=>store_objects(
      iv_repo_key = lv_repo_key
      it_objects  = it_objects ).

    " Update repo state
    zcl_abapgit_ortec_repo_state=>update_after_fetch(
      iv_repo_key    = lv_repo_key
      iv_branch_name = iv_branch_name
      iv_url         = iv_url
      iv_commit      = iv_commit ).

    " Commit to make persistent
    COMMIT WORK.

  ENDMETHOD.


  METHOD resolve_repo_key.
    " Try to find existing repo key by URL
    rv_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( iv_url ).
  ENDMETHOD.

ENDCLASS.
