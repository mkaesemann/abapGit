"! <p class="shorttext synchronized">ORTEC Git Fetch Negotiation</p>
"! Provides known commit SHA1s (have lines) for Git want/have negotiation.
CLASS zcl_abapgit_ortec_fetch_neg DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC.

  PUBLIC SECTION.

    "! Get known commit SHA1s to send as 'have' lines during upload-pack.
    "! Returns empty table if no local state exists or negotiation is inactive.
    "! @parameter iv_url |
    "! Remote URL (used to resolve repository key)
    "! @parameter it_want_hashes |
    "! SHA1s being requested (want lines)
    "! @parameter rt_haves |
    "! Known commit SHA1s to send as have lines
    "! @raising zcx_abapgit_ortec_git |
    "! On error
    CLASS-METHODS get_have_commits
      IMPORTING iv_url          TYPE string
                it_want_hashes  TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RETURNING VALUE(rt_haves) TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RAISING   zcx_abapgit_ortec_git.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.


CLASS zcl_abapgit_ortec_fetch_neg IMPLEMENTATION.

  METHOD get_have_commits.

    DATA lv_repo_key TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.

    " Resolve URL to repo key
    lv_repo_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( iv_url ).
    IF lv_repo_key IS INITIAL.
      RETURN. " No stored state for this URL
    ENDIF.

    " Get all known resolved commits from the object store
    rt_haves = zcl_abapgit_ortec_obj_store=>get_known_commits( lv_repo_key ).

    " Remove any SHA1s that are also in the want list
    " (if we're wanting them, we shouldn't claim to have them)
    DATA lv_want LIKE LINE OF it_want_hashes.
    LOOP AT it_want_hashes INTO lv_want.
      DELETE rt_haves WHERE table_line = lv_want.
    ENDLOOP.

  ENDMETHOD.

ENDCLASS.
