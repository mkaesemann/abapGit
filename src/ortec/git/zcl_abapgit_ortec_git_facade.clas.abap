CLASS zcl_abapgit_ortec_git_facade DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    "! Single entry point for standard hooks (Stage, Diff, Patch) to resolve a
    "! filtered remote file set.
    "! Delegates to the ORTEC filtered walk, which owns the data-validity
    "! fallback chain to the standard get_files_remote. Callers must pass the
    "! result into zcl_abapgit_repo_status=>calculate( it_remote = ... ) so
    "! status is computed in one pass, without a second remote fetch and
    "! without mutating the repository's cached remote-file baseline.
    "! @parameter ii_repo_online |
    "! Repository instance
    "! @parameter ii_obj_filter |
    "! Object filter (Stage-by-transport, single-object diff, or Stage-subset patch/diff)
    "! @parameter rt_files |
    "! Filtered remote files
    "! @raising zcx_abapgit_exception |
    "! Raised only when both the fast path and the standard fallback fail
    CLASS-METHODS resolve_filtered_remote
      IMPORTING
        ii_repo_online  TYPE REF TO zif_abapgit_repo
        ii_obj_filter   TYPE REF TO zif_abapgit_object_filter
      RETURNING
        VALUE(rt_files) TYPE zif_abapgit_git_definitions=>ty_files_tt
      RAISING
        zcx_abapgit_exception.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.


CLASS zcl_abapgit_ortec_git_facade IMPLEMENTATION.

  METHOD resolve_filtered_remote.
    rt_files = zcl_abapgit_ortec_filter_walk=>get_remote_files_for_stage(
      ii_repo_online = ii_repo_online
      ii_obj_filter  = ii_obj_filter ).
  ENDMETHOD.

ENDCLASS.
