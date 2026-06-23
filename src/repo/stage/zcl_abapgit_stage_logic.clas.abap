CLASS zcl_abapgit_stage_logic DEFINITION
  PUBLIC
  CREATE PRIVATE.

  PUBLIC SECTION.

    INTERFACES zif_abapgit_stage_logic.

    CLASS-METHODS get_stage_logic
      RETURNING
        VALUE(ri_logic) TYPE REF TO zif_abapgit_stage_logic.

    CLASS-METHODS set_stage_logic
      IMPORTING
        ii_logic TYPE REF TO zif_abapgit_stage_logic.

  PROTECTED SECTION.
  PRIVATE SECTION.

    CLASS-DATA gi_stage_logic TYPE REF TO zif_abapgit_stage_logic.

    CLASS-METHODS:
      remove_ignored
        IMPORTING ii_repo  TYPE REF TO zif_abapgit_repo
        CHANGING  cs_files TYPE zif_abapgit_definitions=>ty_stage_files,
      remove_identical
        CHANGING cs_files TYPE zif_abapgit_definitions=>ty_stage_files.

ENDCLASS.



CLASS zcl_abapgit_stage_logic IMPLEMENTATION.


  METHOD get_stage_logic.

    IF gi_stage_logic IS INITIAL.
      CREATE OBJECT gi_stage_logic TYPE zcl_abapgit_stage_logic.
    ENDIF.

    ri_logic = gi_stage_logic.

  ENDMETHOD.


  METHOD remove_identical.

    DATA: lv_index  TYPE i,
          ls_remote LIKE LINE OF cs_files-remote.

    FIELD-SYMBOLS: <ls_local> LIKE LINE OF cs_files-local.

    SORT cs_files-remote BY path filename.

    LOOP AT cs_files-local ASSIGNING <ls_local>.
      lv_index = sy-tabix.

      READ TABLE cs_files-remote INTO ls_remote
        WITH KEY path = <ls_local>-file-path filename = <ls_local>-file-filename
        BINARY SEARCH.
      IF sy-subrc = 0.
        DELETE cs_files-remote INDEX sy-tabix.
        IF ls_remote-sha1 = <ls_local>-file-sha1.
          DELETE cs_files-local INDEX lv_index.
        ENDIF.
      ENDIF.
    ENDLOOP.

  ENDMETHOD.


  METHOD remove_ignored.

    DATA: lv_index TYPE i.

    FIELD-SYMBOLS: <ls_remote> LIKE LINE OF cs_files-remote,
                   <ls_local>  LIKE LINE OF cs_files-local.


    LOOP AT cs_files-remote ASSIGNING <ls_remote>.
      lv_index = sy-tabix.

      IF ii_repo->get_dot_abapgit( )->is_ignored(
          iv_path     = <ls_remote>-path
          iv_filename = <ls_remote>-filename ) = abap_true.
        DELETE cs_files-remote INDEX lv_index.
      ELSEIF <ls_remote>-path = zif_abapgit_definitions=>c_root_dir
          AND <ls_remote>-filename = zif_abapgit_definitions=>c_dot_abapgit.
        " Remove .abapgit from remotes - it cannot be removed or ignored
        DELETE cs_files-remote INDEX lv_index.
      ENDIF.

    ENDLOOP.

    LOOP AT cs_files-local ASSIGNING <ls_local>.
      lv_index = sy-tabix.

      IF ii_repo->get_dot_abapgit( )->is_ignored(
          iv_path     = <ls_local>-file-path
          iv_filename = <ls_local>-file-filename ) = abap_true.
        DELETE cs_files-local INDEX lv_index.
      ENDIF.

    ENDLOOP.

  ENDMETHOD.


  METHOD set_stage_logic.
    gi_stage_logic = ii_logic.
  ENDMETHOD.


  METHOD zif_abapgit_stage_logic~get.

    DATA lv_reset_remote_cache TYPE abap_bool.
    DATA lv_use_ortec         TYPE abap_bool.

    " Getting REMOTE before LOCAL is critical to ensure that DATA config is loaded first
    IF ii_obj_filter IS INITIAL.
      rs_files-remote = ii_repo_online->get_files_remote( ii_obj_filter ).
    ELSE.
      TRY.
          lv_use_ortec = zcl_abapgit_ortec_git_switch=>is_active_for_repo(
            CAST zif_abapgit_repo_online( ii_repo_online )->get_url( ) ).
        CATCH cx_root.
          lv_use_ortec = abap_false.
      ENDTRY.

      IF lv_use_ortec = abap_true.
        TRY.
            CALL METHOD ('ZCL_ABAPGIT_ORTEC_FILTER_WALK')=>('GET_REMOTE_FILES_FOR_STAGE')
              EXPORTING
                ii_repo_online = ii_repo_online
                ii_obj_filter  = ii_obj_filter
              RECEIVING
                rt_files       = rs_files-remote.

            " Repo status calculation calls get_files_remote again; preload the filtered
            " set to keep that second call on the same lightweight data.
            ii_repo_online->set_files_remote( rs_files-remote ).
            lv_reset_remote_cache = abap_true.
          CATCH cx_root.
            rs_files-remote = ii_repo_online->get_files_remote( ii_obj_filter ).
        ENDTRY.
      ELSE.
        rs_files-remote = ii_repo_online->get_files_remote( ii_obj_filter ).
      ENDIF.
    ENDIF.

    IF ii_obj_filter IS INITIAL.
      rs_files-local = ii_repo_online->get_files_local( ).
    ELSE.
      rs_files-local = ii_repo_online->get_files_local_filtered( ii_obj_filter ).
    ENDIF.

    rs_files-status = zcl_abapgit_repo_status=>calculate( ii_repo       = ii_repo_online
                                                          ii_obj_filter = ii_obj_filter
                                                          it_local      = rs_files-local ).

    remove_identical( CHANGING cs_files = rs_files ).
    remove_ignored( EXPORTING ii_repo  = ii_repo_online
                    CHANGING  cs_files = rs_files ).

    " Filtered REMOTE list must not remain cached as the repository baseline.
    IF lv_reset_remote_cache = abap_true.
      ii_repo_online->refresh(
        iv_drop_cache = abap_false
        iv_drop_log   = abap_false ).
    ENDIF.

  ENDMETHOD.
ENDCLASS.
