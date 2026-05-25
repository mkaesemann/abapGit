"! <p class="shorttext synchronized">ORTEC Switch-Branch Page Component</p>
"! Inline HOC child presenting the ORTEC branch picker and performing the
"! branch switch once the user makes a selection.
CLASS zcl_abapgit_ortec_branch_swtch DEFINITION
  PUBLIC
  INHERITING FROM zcl_abapgit_gui_component
  FINAL
  CREATE PUBLIC.
  PUBLIC SECTION.
    INTERFACES zif_abapgit_gui_renderable.
    INTERFACES zif_abapgit_gui_event_handler.
    INTERFACES zif_abapgit_gui_page_title.
    METHODS constructor
      IMPORTING
        !iv_key TYPE zif_abapgit_persistence=>ty_repo-key
      RAISING
        zcx_abapgit_exception.
  PROTECTED SECTION.
  PRIVATE SECTION.
    DATA mv_key    TYPE zif_abapgit_persistence=>ty_repo-key.
    DATA mo_picker TYPE REF TO zcl_abapgit_ortec_branch_list.
    METHODS perform_switch
      IMPORTING
        !is_branch TYPE zif_abapgit_git_definitions=>ty_git_branch
      RAISING
        zcx_abapgit_exception.
ENDCLASS.
CLASS zcl_abapgit_ortec_branch_swtch IMPLEMENTATION.
  METHOD constructor.
    DATA li_repo_online TYPE REF TO zif_abapgit_repo_online.
    super->constructor( ).
    mv_key = iv_key.
    li_repo_online ?= zcl_abapgit_repo_srv=>get_instance( )->get( iv_key ).
    mo_picker = zcl_abapgit_ortec_branch_list=>create(
      iv_url             = li_repo_online->get_url( )
      iv_default_branch  = li_repo_online->get_selected_branch( )
      iv_show_new_option = abap_true ).
  ENDMETHOD.
  METHOD perform_switch.
    DATA li_repo_online TYPE REF TO zif_abapgit_repo_online.
    IF is_branch-name = zif_abapgit_popups=>c_new_branch_label.
      zcl_abapgit_services_git=>create_branch( mv_key ).
    ELSE.
      li_repo_online ?= zcl_abapgit_repo_srv=>get_instance( )->get( mv_key ).
      li_repo_online->select_commit( '' ).
      li_repo_online->switch_origin( '' ).
      li_repo_online->select_branch( is_branch-name ).
      COMMIT WORK AND WAIT.
    ENDIF.
  ENDMETHOD.
  METHOD zif_abapgit_gui_page_title~get_page_title.
    rv_title = 'Switch Branch'.
  ENDMETHOD.
  METHOD zif_abapgit_gui_renderable~render.
    register_handlers( ).
    ri_html = mo_picker->zif_abapgit_gui_renderable~render( ).
  ENDMETHOD.
  METHOD zif_abapgit_gui_event_handler~on_event.
    DATA ls_branch TYPE zif_abapgit_git_definitions=>ty_git_branch.
    rs_handled = mo_picker->zif_abapgit_gui_event_handler~on_event( ii_event ).
    IF mo_picker->is_fulfilled( ) = abap_true
        AND mo_picker->was_cancelled( ) = abap_false.
      mo_picker->get_result( IMPORTING es_branch = ls_branch ).
      perform_switch( ls_branch ).
      rs_handled-state = zcl_abapgit_gui=>c_event_state-go_back.
    ENDIF.
  ENDMETHOD.
ENDCLASS.
