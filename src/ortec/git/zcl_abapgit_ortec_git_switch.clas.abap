"! <p class="shorttext synchronized">ORTEC Git Extension Feature Toggle</p>
"! Controls ORTEC persistent cache activation via ORTEC user persistence.
"! Also provides cache clear operation for all ZAOG_* tables per repository key.
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

    TYPES:
      BEGIN OF ty_clear_result,
        repo_key   TYPE zcl_abapgit_ortec_repo_state=>ty_repo_key,
        obj_store  TYPE i,
        obj_index  TYPE i,
        pack_idx   TYPE i,
        pack_meta  TYPE i,
        raw_pack   TYPE i,
        fetch_sess TYPE i,
        repo_state TYPE i,
      END OF ty_clear_result.

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

    "! Clear all ZAOG_* cache rows for one repository.
    "! Resolves the ORTEC repo_key from the repository URL (via SHA1 hash lookup).
    "! @parameter iv_url |
    "! Repository URL (used to derive ORTEC repo_key)
    "! @parameter rs_result |
    "! Number of deleted rows per table
    "! @raising zcx_abapgit_ortec_git |
    "! Raised if no ORTEC cache state can be resolved for the URL.
    CLASS-METHODS clear_repo_cache
      IMPORTING iv_url           TYPE string
      RETURNING VALUE(rs_result) TYPE ty_clear_result
      RAISING   zcx_abapgit_ortec_git.

    "! Build UI message for clear result.
    "! Formats the per-table delete counts into the compact repository-cache message.
    "! @parameter is_result |
    "! Clear result counts.
    "! @parameter rv_message |
    "! User-facing summary text.
    CLASS-METHODS format_clear_result
      IMPORTING is_result         TYPE ty_clear_result
      RETURNING VALUE(rv_message) TYPE string.

  PRIVATE SECTION.
    CLASS-DATA mv_bulk_exists_active TYPE abap_bool VALUE abap_true.
    CLASS-DATA mv_serial_prefetch_active TYPE abap_bool VALUE abap_true.
    CLASS-DATA mv_avoid_timeout_active TYPE abap_bool VALUE abap_true.

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

  METHOD clear_repo_cache.
    DATA lv_repo_key TYPE zcl_abapgit_ortec_repo_state=>ty_repo_key.

    IF iv_url IS INITIAL.
      RETURN.
    ENDIF.

    " Resolve ORTEC repo_key from the repository URL
    lv_repo_key = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( iv_url ).
    IF lv_repo_key IS INITIAL.
      zcx_abapgit_ortec_git=>raise( |No ORTEC cache found for this repository URL| ).
    ENDIF.

    rs_result-repo_key = lv_repo_key.

    DELETE FROM zaog_obj_store WHERE repo_key = lv_repo_key.
    rs_result-obj_store = sy-dbcnt.

    DELETE FROM zaog_obj_index WHERE repo_key = lv_repo_key.
    rs_result-obj_index = sy-dbcnt.

    DELETE FROM zaog_pack_idx WHERE repo_key = lv_repo_key.
    rs_result-pack_idx = sy-dbcnt.

    DELETE FROM zaog_pack_meta WHERE repo_key = lv_repo_key.
    rs_result-pack_meta = sy-dbcnt.

    DELETE FROM zaog_raw_pack WHERE repo_key = lv_repo_key.
    rs_result-raw_pack = sy-dbcnt.

    DELETE FROM zaog_fetch_sess WHERE repo_key = lv_repo_key.
    rs_result-fetch_sess = sy-dbcnt.

    DELETE FROM zaog_repo_state WHERE repo_key = lv_repo_key.
    rs_result-repo_state = sy-dbcnt.

    " Invalidate in-memory session cache
    zcl_abapgit_ortec_obj_store=>invalidate_cache( ).

    COMMIT WORK AND WAIT.
  ENDMETHOD.

  METHOD format_clear_result.
    DATA lv_total TYPE i.

    lv_total = is_result-obj_store
             + is_result-obj_index
             + is_result-pack_idx
             + is_result-pack_meta
             + is_result-raw_pack
             + is_result-fetch_sess
             + is_result-repo_state.

    rv_message = |ORTEC cache cleared for repo key { is_result-repo_key }: { lv_total } row(s) removed|.
    rv_message = |{ rv_message } [OBJ_STORE={ is_result-obj_store }, OBJ_INDEX={ is_result-obj_index },|.
    rv_message = |{ rv_message } PACK_IDX={ is_result-pack_idx }, PACK_META={ is_result-pack_meta },|.
    rv_message = |{ rv_message } RAW_PACK={ is_result-raw_pack }, FETCH_SESS={ is_result-fetch_sess },|.
    rv_message = |{ rv_message } REPO_STATE={ is_result-repo_state }]|.
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

ENDCLASS.
