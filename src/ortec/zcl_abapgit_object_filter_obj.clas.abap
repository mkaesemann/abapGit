CLASS zcl_abapgit_object_filter_obj DEFINITION
  PUBLIC
  CREATE PUBLIC.

  PUBLIC SECTION.
    INTERFACES zif_abapgit_object_filter.

    TYPES: BEGIN OF ty_e071_filter,
             pgmid    TYPE tadir-pgmid,
             object   TYPE tadir-object,
             obj_name TYPE trobj_name,
           END OF ty_e071_filter,
           ty_e071_filter_tt TYPE STANDARD TABLE OF ty_e071_filter WITH EMPTY KEY.

    METHODS set_filter_values
      IMPORTING iv_package TYPE tadir-devclass
                it_objects TYPE ty_e071_filter_tt
      RAISING   zcx_abapgit_exception.

    METHODS get_filter_values
      EXPORTING ev_package TYPE tadir-devclass
                et_objects TYPE ty_e071_filter_tt.

    CLASS-METHODS create_filter
      IMPORTING io_repo          TYPE REF TO zif_abapgit_repo
                is_file          TYPE zif_abapgit_git_definitions=>ty_file OPTIONAL
                is_object        TYPE zif_abapgit_definitions=>ty_item     OPTIONAL
                it_files         TYPE zif_abapgit_definitions=>ty_stage_tt OPTIONAL
      RETURNING VALUE(ro_filter) TYPE REF TO zcl_abapgit_object_filter_obj
      RAISING   zcx_abapgit_exception.

  PROTECTED SECTION.
    METHODS adjust_local_filter
      IMPORTING it_objects       TYPE ty_e071_filter_tt
                iv_package       TYPE tadir-devclass
      RETURNING VALUE(rt_filter) TYPE zif_abapgit_definitions=>ty_tadir_tt
      RAISING   zcx_abapgit_exception.

  PRIVATE SECTION.
    DATA mt_filter  TYPE zif_abapgit_definitions=>ty_tadir_tt.
    DATA mv_package TYPE tadir-devclass.

    METHODS generate_local_filter
      IMPORTING iv_package       TYPE tadir-devclass
                it_objects       TYPE ty_e071_filter_tt
      RETURNING VALUE(rt_filter) TYPE zif_abapgit_definitions=>ty_tadir_tt
      RAISING   zcx_abapgit_exception.

    METHODS init.

    METHODS get_all_sub_packages
      IMPORTING iv_package       TYPE tadir-devclass
      RETURNING VALUE(rt_filter) TYPE zif_abapgit_definitions=>ty_tadir_tt.
ENDCLASS.


CLASS zcl_abapgit_object_filter_obj IMPLEMENTATION.
  METHOD adjust_local_filter.

    DATA lt_e071_filter    TYPE ty_e071_filter_tt.
    DATA lr_e071_filter    TYPE REF TO ty_e071_filter.
    DATA lr_cts_api        TYPE REF TO zif_abapgit_cts_api.
    DATA lv_trobj_type_new TYPE tadir-object.
    DATA lv_trobj_name_new TYPE trobj_name.
    DATA ls_filter         TYPE zif_abapgit_definitions=>ty_tadir.
    DATA lt_filter         TYPE zif_abapgit_definitions=>ty_tadir_tt.

    lt_e071_filter = it_objects.

    LOOP AT lt_e071_filter REFERENCE INTO lr_e071_filter.

      IF lr_e071_filter->pgmid = 'LIMU'.
        " Get Main Object from LIMU Object (Example the Class (R3TR) of a Method (LIMU))

        lr_cts_api = zcl_abapgit_factory=>get_cts_api( ).

        TRY.
            lr_cts_api->get_r3tr_obj_for_limu_obj(
              EXPORTING
                iv_object   = lr_e071_filter->object
                iv_obj_name = lr_e071_filter->obj_name
              IMPORTING
                ev_object   = lv_trobj_type_new
                ev_obj_name = lv_trobj_name_new ).
          CATCH zcx_abapgit_exception.
            CONTINUE.
        ENDTRY.

        CLEAR ls_filter.
        ls_filter-pgmid    = 'R3TR'.
        ls_filter-object   = lv_trobj_type_new.
        ls_filter-obj_name = lv_trobj_name_new.
      ELSE.
        ls_filter-pgmid    = lr_e071_filter->pgmid.
        ls_filter-object   = lr_e071_filter->object.
        ls_filter-obj_name = lr_e071_filter->obj_name.
      ENDIF.
      INSERT ls_filter INTO TABLE rt_filter.
    ENDLOOP.

    IF iv_package IS NOT INITIAL.
      ls_filter-pgmid    = 'R3TR'.
      ls_filter-object   = 'DEVC'.
      ls_filter-obj_name = iv_package.
      INSERT ls_filter INTO TABLE rt_filter.

      lt_filter = get_all_sub_packages(
                      iv_package ).
      INSERT LINES OF lt_filter INTO TABLE rt_filter.

    ENDIF.

    SORT rt_filter.
    DELETE ADJACENT DUPLICATES FROM rt_filter.

    IF rt_filter IS INITIAL.

      zcx_abapgit_exception=>raise(
          'No objects found for transport filter' ).

    ENDIF.

  ENDMETHOD.

  METHOD create_filter.

    DATA(lt_objects) = VALUE ty_e071_filter_tt( ).

    IF is_file IS NOT INITIAL.
      zcl_abapgit_filename_logic=>file_to_object(
        EXPORTING
          iv_filename = is_file-filename
          iv_path     = is_file-path
          iv_devclass = io_repo->get_package( )
          io_dot      = io_repo->get_dot_abapgit( )
        IMPORTING
          es_item     = DATA(ls_item) ).
      INSERT VALUE ty_e071_filter( pgmid    = 'R3TR'
                                   object   = ls_item-obj_type
                                   obj_name = ls_item-obj_name
        ) INTO TABLE lt_objects.
    ENDIF.

    IF is_object IS NOT INITIAL.
      INSERT VALUE ty_e071_filter( pgmid    = 'R3TR'
                                   object   = is_object-obj_type
                                   obj_name = is_object-obj_name
          ) INTO TABLE lt_objects.
    ENDIF.

    LOOP AT it_files ASSIGNING FIELD-SYMBOL(<ls_file>).
      zcl_abapgit_filename_logic=>file_to_object(
        EXPORTING
          iv_filename = <ls_file>-file-filename
          iv_path     = <ls_file>-file-path
          iv_devclass = io_repo->get_package( )
          io_dot      = io_repo->get_dot_abapgit( )
        IMPORTING
          es_item     = DATA(ls_file_item) ).
      INSERT VALUE ty_e071_filter( pgmid    = 'R3TR'
                                   object   = ls_file_item-obj_type
                                   obj_name = ls_file_item-obj_name
        ) INTO TABLE lt_objects.
    ENDLOOP.

    IF lt_objects IS NOT INITIAL.
      ro_filter = NEW zcl_abapgit_object_filter_obj( ).
      ro_filter->set_filter_values(
          iv_package = io_repo->get_package( )
          it_objects = lt_objects
      ).
    ENDIF.

  ENDMETHOD.

  METHOD generate_local_filter.
    rt_filter = adjust_local_filter(
                    iv_package = iv_package
                    it_objects = it_objects ).
  ENDMETHOD.

  METHOD get_all_sub_packages.

    DATA li_package TYPE REF TO zif_abapgit_sap_package.
    DATA lt_list    TYPE zif_abapgit_sap_package=>ty_devclass_tt.
    DATA lr_list    TYPE REF TO devclass.
    DATA ls_filter  TYPE zif_abapgit_definitions=>ty_tadir.

    li_package = zcl_abapgit_factory=>get_sap_package(
                     iv_package ).
    lt_list = li_package->list_subpackages( ).
    LOOP AT lt_list REFERENCE INTO lr_list.
      ls_filter-pgmid    = 'R3TR'.
      ls_filter-object   = 'DEVC'.
      ls_filter-obj_name = lr_list->*.
      INSERT ls_filter INTO TABLE rt_filter.
    ENDLOOP.

  ENDMETHOD.

  METHOD get_filter_values.
    et_objects = CORRESPONDING #( mt_filter ).
    ev_package = mv_package.
  ENDMETHOD.

  METHOD init.
    CLEAR mt_filter.
    CLEAR mv_package.
  ENDMETHOD.

  METHOD set_filter_values.
    init( ).
    mv_package = iv_package.
    IF it_objects IS NOT INITIAL.
      mt_filter = generate_local_filter(
                      iv_package = mv_package
                      it_objects = it_objects ).
    ENDIF.
  ENDMETHOD.

  METHOD zif_abapgit_object_filter~get_filter.
    rt_filter = mt_filter.
  ENDMETHOD.
ENDCLASS.
