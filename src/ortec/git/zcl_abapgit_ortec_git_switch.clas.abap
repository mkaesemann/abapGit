"! <p class="shorttext synchronized">ORTEC Git Extension Feature Toggle</p>
"!
"! Controls ORTEC persistent cache activation and internal-session feature
"! switches. Administrative cache inspection and deletion are owned by
"! ZCL_ABAPGIT_ORTEC_CACHE_ADMIN.
CLASS zcl_abapgit_ortec_git_switch DEFINITION
  PUBLIC FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.
    CONSTANTS:
      BEGIN OF cs_info,
        BEGIN OF settings,
          name  TYPE string VALUE 'use_repo_obj_cache',
          label TYPE string VALUE 'Use Persistent Object Cache',
          hint  TYPE string VALUE 'Use ZAOG_* persisted object cache for this repository (per user setting)',
        END OF settings,
      END OF cs_info.

    "! Local-object bulk-exists optimization switches.
    "! TABL, DTEL, CLAS, and INTF are active after audit validation in IT8.
    CONSTANTS:
      BEGIN OF cs_bulk_exists,
        "! Use the TABL bulk handler instead of standard per-object TABL existence checks.
        tabl_active TYPE abap_bool VALUE abap_true,
        "! Use the DTEL bulk handler instead of standard per-object DTEL existence checks.
        dtel_active TYPE abap_bool VALUE abap_true,
        "! Use the CLAS bulk handler instead of standard per-object CLAS existence checks.
        clas_active TYPE abap_bool VALUE abap_true,
        "! Use the INTF bulk handler instead of standard per-object INTF existence checks.
        intf_active TYPE abap_bool VALUE abap_true,
      END OF cs_bulk_exists.

    "! D4 completeness-strictness mode for object/path resolution (see
    "! .memory/logs/target_design.md §2.3). This is a compile-time constant,
    "! not a session-runtime toggle: switching modes is an explicit code
    "! change + redeploy, used to benchmark the two modes side-by-side.
    "! STRICT (default, ships): a per-commit filtered index is only trusted
    "! as complete once its explicit completion marker row is found; an
    "! index left behind by an interrupted/partial rebuild (corrupt tree,
    "! missing object, decode failure) is correctly detected as incomplete
    "! and is rebuilt again from the persistent object store before any
    "! file is resolved from it. RELAXED (benchmark-only, must never ship as
    "! default): trusts the first indexed row found for the commit, which
    "! does not distinguish a fully-built index from one interrupted
    "! mid-rebuild - kept only to measure the cost of the STRICT marker
    "! check on very large repositories. In both modes, an unresolved
    "! object/path state can never be turned into a Deleted/Modified/Added
    "! verdict (see zcl_abapgit_ortec_obj_store=>cs_object_state); RELAXED
    "! only reduces how many positive resolutions are gathered before an
    "! already-unambiguous outcome is trusted.
    CONSTANTS:
      BEGIN OF cs_absent_strictness,
        mode_strict  TYPE string VALUE 'STRICT',
        mode_relaxed TYPE string VALUE 'RELAXED',
        mode         TYPE string VALUE 'STRICT',
      END OF cs_absent_strictness.

    "! Shared trigger-text prefix for the porcelain 'walk' self-heal
    "! contract (Package E design doc §7, OF-2): zcl_abapgit_ortec_porcelain
    "! =>walk raises every "tree/blob not found" exception with a text
    "! starting with this exact prefix, and its OWN pull_by_branch's CS
    "! check (which decides whether to invalidate all history and retry
    "! once) matches against this same constant - both producer and
    "! consumer must always agree on one literal instead of two independent
    "! copies of the string 'Walk,'.
    CONSTANTS c_walk_error_prefix TYPE string VALUE 'Walk,'.

    "! Check if ORTEC fast path is active for repository URL.
    "! @parameter iv_url |
    "! Repository URL
    "! @parameter rv_active |
    "! ABAP_TRUE if active for current user and repository
    CLASS-METHODS is_active_for_repo
      IMPORTING iv_url           TYPE string
      RETURNING VALUE(rv_active) TYPE abap_bool.

    "! Check if bulk existence checks are enabled in this internal session.
    "! Defaults to ABAP_FALSE so standard abapGit behavior is unchanged.
    "! @parameter rv_active |
    "! ABAP_TRUE if bulk existence checks are enabled.
    CLASS-METHODS is_bulk_exists_active
      RETURNING VALUE(rv_active) TYPE abap_bool.

    "! Enable or disable bulk existence checks in this internal session.
    "! @parameter iv_active |
    "! ABAP_TRUE enables the guarded TADIR bulk-exists hook.
    CLASS-METHODS set_bulk_exists_active
      IMPORTING iv_active TYPE abap_bool.

    "! Check if serializer prefetch is enabled in this internal session.
    "! Defaults to ABAP_FALSE so standard abapGit behavior is unchanged.
    "! @parameter rv_active |
    "! ABAP_TRUE if serializer prefetch is enabled.
    CLASS-METHODS is_serial_prefetch_active
      RETURNING VALUE(rv_active) TYPE abap_bool.

    CLASS-METHODS is_wapa_active
      RETURNING VALUE(rv_active) TYPE abap_bool.

    "! Check if the SER-SLICE-2 adaptive batch serialization path is
    "! enabled in this internal session. Defaults to ABAP_FALSE - this is
    "! new, not-yet-IT8-validated behavior, so it must be explicitly
    "! opted into; the existing sequential/parallel path is always used
    "! when this is off.
    "! @parameter rv_active |
    "! ABAP_TRUE if the adaptive batch path is enabled.
    CLASS-METHODS is_serial_batch_active
      RETURNING VALUE(rv_active) TYPE abap_bool.

    "! Enable or disable the SER-SLICE-2 adaptive batch serialization path
    "! in this internal session.
    "! @parameter iv_active |
    "! ABAP_TRUE enables ZCL_ABAPGIT_ORTEC_SER_ORCH=>SERIALIZE dispatch.
    CLASS-METHODS set_serial_batch_active
      IMPORTING iv_active TYPE abap_bool.

    "! Check if timeout avoidance via TH_REDISPATCH is enabled in this internal session.
    "! Defaults to ABAP_FALSE in IT8 so SAT traces can run without redispatch interference.
    "! @parameter rv_active |
    "! ABAP_TRUE if timeout avoidance is enabled.
    CLASS-METHODS is_avoid_timeout_active
      RETURNING VALUE(rv_active) TYPE abap_bool.

    "! Enable or disable timeout avoidance via TH_REDISPATCH in this internal session.
    "! @parameter iv_active |
    "! ABAP_TRUE enables centralized TH_REDISPATCH calls.
    CLASS-METHODS set_avoid_timeout_active
      IMPORTING iv_active TYPE abap_bool.

    "! Central timeout-avoidance hook. Keep TH_REDISPATCH calls behind this method.
    CLASS-METHODS avoid_timeout.

    "! Enable or disable serializer prefetch in this internal session.
    "! @parameter iv_active |
    "! ABAP_TRUE enables the guarded serialization prefetch hook.
    CLASS-METHODS set_serial_prefetch_active
      IMPORTING iv_active TYPE abap_bool.

    "! Read persistent cache flag from ORTEC user persistence.
    "! @parameter iv_url |
    "! Repository URL
    "! @parameter rv_enabled |
    "! ABAP_TRUE if cache usage is enabled
    CLASS-METHODS get_use_repo_cache
      IMPORTING iv_url            TYPE string
      RETURNING VALUE(rv_enabled) TYPE abap_bool.

    "! Persist repository cache flag in ORTEC user persistence.
    "! @parameter iv_url |
    "! Repository URL.
    "! @parameter iv_enabled |
    "! ABAP_TRUE to enable persistent cache.
    CLASS-METHODS set_use_repo_cache
      IMPORTING iv_url     TYPE string
                iv_enabled TYPE abap_bool.

  PRIVATE SECTION.
    CLASS-DATA mv_bulk_exists_active TYPE abap_bool VALUE abap_true.
    CLASS-DATA mv_serial_prefetch_active TYPE abap_bool VALUE abap_true.
    CLASS-DATA mv_avoid_timeout_active TYPE abap_bool VALUE abap_true.
    CLASS-DATA mv_serial_batch_active TYPE abap_bool VALUE abap_true.

ENDCLASS.


CLASS zcl_abapgit_ortec_git_switch IMPLEMENTATION.
  METHOD avoid_timeout.
    IF is_avoid_timeout_active( ) = abap_false.
      RETURN.
    ENDIF.

*    IF zcl_abapgit_factory=>get_function_module( )->function_exists( 'TH_REDISPATCH' ) = abap_false.
*      RETURN.
*    ENDIF.

    CALL FUNCTION 'TH_REDISPATCH'
      EXCEPTIONS
        OTHERS = 1.
  ENDMETHOD.

    METHOD get_use_repo_cache.
    TRY.
        rv_enabled = zcl_abapgit_persistence_ortec=>get_instance( )->get_repo_use_cache( iv_url ).
      CATCH cx_root.
        rv_enabled = abap_false.
    ENDTRY.
  ENDMETHOD.

  METHOD is_avoid_timeout_active.
    rv_active = mv_avoid_timeout_active.
  ENDMETHOD.

  METHOD is_bulk_exists_active.
    rv_active = mv_bulk_exists_active.
  ENDMETHOD.

  METHOD is_serial_prefetch_active.
    rv_active = mv_serial_prefetch_active.
  ENDMETHOD.

  METHOD set_avoid_timeout_active.
    mv_avoid_timeout_active = iv_active.
  ENDMETHOD.

  METHOD set_bulk_exists_active.
    mv_bulk_exists_active = iv_active.
  ENDMETHOD.

  METHOD set_serial_prefetch_active.
    mv_serial_prefetch_active = iv_active.
  ENDMETHOD.

  METHOD set_use_repo_cache.
    TRY.
        zcl_abapgit_persistence_ortec=>get_instance( )->set_repo_use_cache(
            iv_url       = iv_url
            iv_use_cache = iv_enabled ).
      CATCH cx_root.
        RETURN.
    ENDTRY.
  ENDMETHOD.

  METHOD is_active_for_repo.
    rv_active = get_use_repo_cache( iv_url ).
  ENDMETHOD.

  METHOD is_wapa_active.
    rv_active = abap_true.
  ENDMETHOD.

  METHOD is_serial_batch_active.
    rv_active = mv_serial_batch_active.
  ENDMETHOD.

  METHOD set_serial_batch_active.
    mv_serial_batch_active = iv_active.
  ENDMETHOD.

ENDCLASS.

