"! <p class="shorttext synchronized">ORTEC Git Extension Feature Toggle</p>
"! Controls ORTEC persistent cache activation via repository local settings.
"! Also provides cache clear operation for all ZAOG_* tables per repository key.
CLASS zcl_abapgit_ortec_git_switch DEFINITION
  PUBLIC
  FINAL
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

    TYPES:
      BEGIN OF ty_clear_result,
        repo_key    TYPE zcl_abapgit_ortec_repo_state=>ty_repo_key,
        obj_store   TYPE i,
        pack_idx    TYPE i,
        pack_meta   TYPE i,
        raw_pack    TYPE i,
        fetch_sess  TYPE i,
        repo_state  TYPE i,
      END OF ty_clear_result.

    "! Check if ORTEC fast path is active for repository URL.
    "! @parameter rv_active |
    "! ABAP_TRUE if active for current user and repository
    "! @parameter iv_url |
    "! Repository URL
    CLASS-METHODS is_active_for_repo
      IMPORTING iv_url           TYPE string
      RETURNING VALUE(rv_active) TYPE abap_bool.

    "! Compatibility method. Use IS_ACTIVE_FOR_REPO in new code.
    CLASS-METHODS is_active
      RETURNING VALUE(rv_active) TYPE abap_bool.

    "! Read persistent cache flag from repository local settings.
    "! @parameter iv_url |
    "! Repository URL
    "! @parameter rv_enabled |
    "! ABAP_TRUE if cache usage is enabled
    CLASS-METHODS get_use_repo_cache
      IMPORTING iv_url            TYPE string
      RETURNING VALUE(rv_enabled) TYPE abap_bool.

    "! Clear all ZAOG_* cache rows for one repository.
    "! @parameter iv_repo_key |
    "! Repository key
    "! @parameter rs_result |
    "! Number of deleted rows per table
    CLASS-METHODS clear_repo_cache
      IMPORTING iv_repo_key        TYPE zif_abapgit_persistence=>ty_repo-key
      RETURNING VALUE(rs_result)   TYPE ty_clear_result
      RAISING   zcx_abapgit_ortec_git.

    "! Build UI message for clear result.
    CLASS-METHODS format_clear_result
      IMPORTING is_result          TYPE ty_clear_result
      RETURNING VALUE(rv_message)  TYPE string.

  PROTECTED SECTION.
  PRIVATE SECTION.

    CLASS-METHODS read_repo
      IMPORTING iv_url        TYPE string
      RETURNING VALUE(ro_repo) TYPE REF TO zif_abapgit_repo.

ENDCLASS.



CLASS zcl_abapgit_ortec_git_switch IMPLEMENTATION.

  METHOD clear_repo_cache.
    IF iv_repo_key IS INITIAL.
      RETURN.
    ENDIF.

    rs_result-repo_key = iv_repo_key.

    SELECT COUNT( * ) INTO @rs_result-obj_store
      FROM zaog_obj_store
      WHERE repo_key = @iv_repo_key.
    DELETE FROM zaog_obj_store WHERE repo_key = iv_repo_key.

    SELECT COUNT( * ) INTO @rs_result-pack_idx
      FROM zaog_pack_idx
      WHERE repo_key = @iv_repo_key.
    DELETE FROM zaog_pack_idx WHERE repo_key = iv_repo_key.

    SELECT COUNT( * ) INTO @rs_result-pack_meta
      FROM zaog_pack_meta
      WHERE repo_key = @iv_repo_key.
    DELETE FROM zaog_pack_meta WHERE repo_key = iv_repo_key.

    SELECT COUNT( * ) INTO @rs_result-raw_pack
      FROM zaog_raw_pack
      WHERE repo_key = @iv_repo_key.
    DELETE FROM zaog_raw_pack WHERE repo_key = iv_repo_key.

    SELECT COUNT( * ) INTO @rs_result-fetch_sess
      FROM zaog_fetch_sess
      WHERE repo_key = @iv_repo_key.
    DELETE FROM zaog_fetch_sess WHERE repo_key = iv_repo_key.

    SELECT COUNT( * ) INTO @rs_result-repo_state
      FROM zaog_repo_state
      WHERE repo_key = @iv_repo_key.
    DELETE FROM zaog_repo_state WHERE repo_key = iv_repo_key.

    COMMIT WORK AND WAIT.
  ENDMETHOD.

  METHOD format_clear_result.
    DATA lv_total TYPE i.

    lv_total = is_result-obj_store
             + is_result-pack_idx
             + is_result-pack_meta
             + is_result-raw_pack
             + is_result-fetch_sess
             + is_result-repo_state.

    rv_message = |Repository cache cleared (key { is_result-repo_key }): { lv_total } row(s) removed|.
    rv_message = |{ rv_message } [OBJ={ is_result-obj_store }, IDX={ is_result-pack_idx }, META={ is_result-pack_meta }, RAW={ is_result-raw_pack }, SESS={ is_result-fetch_sess }, STATE={ is_result-repo_state }]|.
  ENDMETHOD.

  METHOD get_use_repo_cache.
    DATA lo_repo TYPE REF TO zif_abapgit_repo.
    DATA ls_local TYPE zif_abapgit_persistence=>ty_repo-local_settings.
    FIELD-SYMBOLS <lv_enabled> TYPE any.

    lo_repo = read_repo( iv_url ).
    IF lo_repo IS BOUND.
      ls_local = lo_repo->get_local_settings( ).
      ASSIGN COMPONENT 'USE_REPO_CACHE' OF STRUCTURE ls_local TO <lv_enabled>.
      IF sy-subrc = 0.
        rv_enabled = <lv_enabled>.
      ENDIF.
    ENDIF.
  ENDMETHOD.

  METHOD is_active.
    DATA lt_repos TYPE zif_abapgit_repo_srv=>ty_repo_list.
    DATA lo_repo  TYPE REF TO zif_abapgit_repo.
    DATA ls_local TYPE zif_abapgit_persistence=>ty_repo-local_settings.
    FIELD-SYMBOLS <lv_enabled> TYPE any.

    lt_repos = zcl_abapgit_repo_srv=>get_instance( )->list( ).
    LOOP AT lt_repos INTO lo_repo.
      ls_local = lo_repo->get_local_settings( ).
      ASSIGN COMPONENT 'USE_REPO_CACHE' OF STRUCTURE ls_local TO <lv_enabled>.
      IF sy-subrc = 0 AND <lv_enabled> = abap_true.
        rv_active = abap_true.
        EXIT.
      ENDIF.
    ENDLOOP.
  ENDMETHOD.

  METHOD is_active_for_repo.
    rv_active = get_use_repo_cache( iv_url ).
  ENDMETHOD.

  METHOD read_repo.
    DATA lo_srv TYPE REF TO zif_abapgit_repo_srv.
    DATA lo_repo TYPE REF TO zif_abapgit_repo.

    lo_srv = zcl_abapgit_repo_srv=>get_instance( ).
    TRY.
        lo_srv->get_repo_from_url(
          EXPORTING
            iv_url  = iv_url
          IMPORTING
            ei_repo  = lo_repo ).
      CATCH zcx_abapgit_exception.
        CLEAR lo_repo.
    ENDTRY.

    ro_repo = lo_repo.
  ENDMETHOD.

ENDCLASS.
