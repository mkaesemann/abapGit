CLASS zcl_abapgit_tadir DEFINITION
  PUBLIC
  FINAL
  CREATE PRIVATE
  GLOBAL FRIENDS zcl_abapgit_factory .

  PUBLIC SECTION.
    INTERFACES zif_abapgit_tadir .

  PROTECTED SECTION.
  PRIVATE SECTION.
    METHODS EXISTS
      IMPORTING
        !IS_ITEM TYPE ZIF_ABAPGIT_DEFINITIONS=>TY_ITEM
      RETURNING
        VALUE(RV_EXISTS) TYPE ABAP_BOOL .
    METHODS CHECK_EXISTS
      IMPORTING
        !IT_TADIR TYPE ZIF_ABAPGIT_DEFINITIONS=>TY_TADIR_TT
      RETURNING
        VALUE(RT_TADIR) TYPE ZIF_ABAPGIT_DEFINITIONS=>TY_TADIR_TT
      RAISING
        ZCX_ABAPGIT_EXCEPTION .
    METHODS BUILD
      IMPORTING
        !iv_package            TYPE tadir-devclass
        !iv_top                TYPE tadir-devclass
        !io_dot                TYPE REF TO zcl_abapgit_dot_abapgit
        !iv_ignore_subpackages TYPE abap_bool DEFAULT abap_false
        !iv_only_local_objects TYPE abap_bool
        !io_log                TYPE REF TO zcl_abapgit_log OPTIONAL
      RETURNING
        VALUE(RT_TADIR) TYPE ZIF_ABAPGIT_DEFINITIONS=>TY_TADIR_TT
      RAISING
        ZCX_ABAPGIT_EXCEPTION .
ENDCLASS.



CLASS ZCL_ABAPGIT_TADIR IMPLEMENTATION.


  METHOD build.

    DATA: lt_tdevc        TYPE STANDARD TABLE OF tdevc,
          lv_path         TYPE string,
          lo_skip_objects TYPE REF TO zcl_abapgit_skip_objects,
          lt_excludes     TYPE RANGE OF trobjtype,
          lt_srcsystem    TYPE RANGE OF tadir-srcsystem,
          ls_srcsystem    LIKE LINE OF lt_srcsystem,
          ls_exclude      LIKE LINE OF lt_excludes,
          lo_folder_logic TYPE REF TO zcl_abapgit_folder_logic,
          lv_last_package TYPE devclass VALUE cl_abap_char_utilities=>horizontal_tab,
          lt_packages     TYPE zif_abapgit_sap_package=>ty_devclass_tt.

    FIELD-SYMBOLS: <ls_tadir>   LIKE LINE OF rt_tadir,
                   <lv_package> TYPE devclass.

    "Determine Packages to Read
    IF iv_ignore_subpackages = abap_false.
      lt_packages = zcl_abapgit_factory=>get_sap_package( iv_package )->list_subpackages( ).
    ENDIF.
    INSERT iv_package INTO lt_packages INDEX 1.

    "Select TADIR Info
    ls_exclude-sign = 'I'.
    ls_exclude-option = 'EQ'.

    ls_exclude-low = 'SOTR'.
    APPEND ls_exclude TO lt_excludes.
    ls_exclude-low = 'SFB1'.
    APPEND ls_exclude TO lt_excludes.
    ls_exclude-low = 'SFB2'.
    APPEND ls_exclude TO lt_excludes.
    ls_exclude-low = 'STOB'. " auto generated by core data services
    APPEND ls_exclude TO lt_excludes.

    IF iv_only_local_objects = abap_true.
      ls_srcsystem-sign   = 'I'.
      ls_srcsystem-option = 'EQ'.
      ls_srcsystem-low    = sy-sysid.
      APPEND ls_srcsystem TO lt_srcsystem.
    ENDIF.

    IF lt_packages IS NOT INITIAL.
      SELECT * FROM tadir
        INTO CORRESPONDING FIELDS OF TABLE rt_tadir
        FOR ALL ENTRIES IN lt_packages
        WHERE devclass = lt_packages-table_line
        AND pgmid = 'R3TR'
        AND object NOT IN lt_excludes
        AND delflag = abap_false
        AND srcsystem IN lt_srcsystem
        ORDER BY PRIMARY KEY.             "#EC CI_GENBUFF "#EC CI_SUBRC
    ENDIF.

    SORT rt_tadir BY devclass pgmid object obj_name.

    "Skip Generated Objects
    CREATE OBJECT lo_skip_objects.
    rt_tadir = lo_skip_objects->skip_sadl_generated_objects(
      it_tadir = rt_tadir
      io_log   = io_log ).

    LOOP AT lt_packages ASSIGNING <lv_package>.
      " Local packages are not in TADIR, only in TDEVC, act as if they were
      IF <lv_package> CP '$*'. " OR <lv_package> CP 'T*' ).
        APPEND INITIAL LINE TO rt_tadir ASSIGNING <ls_tadir>.
        <ls_tadir>-pgmid    = 'R3TR'.
        <ls_tadir>-object   = 'DEVC'.
        <ls_tadir>-obj_name = <lv_package>.
        <ls_tadir>-devclass = <lv_package>.
      ENDIF.
    ENDLOOP.

    LOOP AT rt_tadir ASSIGNING <ls_tadir>.

      IF lv_last_package <> <ls_tadir>-devclass.
        "Change in Package
        lv_last_package = <ls_tadir>-devclass.

        IF NOT io_dot IS INITIAL.
          "Reuse given Folder Logic Instance
          IF lo_folder_logic IS NOT BOUND.
            "Get Folder Logic Instance
            lo_folder_logic = zcl_abapgit_folder_logic=>get_instance( ).
          ENDIF.

          lv_path = lo_folder_logic->package_to_path(
            iv_top     = iv_top
            io_dot     = io_dot
            iv_package = <ls_tadir>-devclass ).
        ENDIF.
      ENDIF.

      <ls_tadir>-path = lv_path.

      CASE <ls_tadir>-object.
        WHEN 'SICF'.
* replace the internal GUID with a hash of the path
          TRY.
              CALL METHOD ('ZCL_ABAPGIT_OBJECT_SICF')=>read_sicf_url
                EXPORTING
                  iv_obj_name = <ls_tadir>-obj_name
                RECEIVING
                  rv_hash     = <ls_tadir>-obj_name+15.
            CATCH cx_sy_dyn_call_illegal_method.
* SICF might not be supported in some systems, assume this code is not called
          ENDTRY.
      ENDCASE.
    ENDLOOP.
  ENDMETHOD.


  METHOD check_exists.

    DATA: li_progress TYPE REF TO zif_abapgit_progress,
          ls_item     TYPE zif_abapgit_definitions=>ty_item.

    FIELD-SYMBOLS: <ls_tadir> LIKE LINE OF it_tadir.


    li_progress = zcl_abapgit_progress=>get_instance( lines( it_tadir ) ).

* rows from database table TADIR are not removed for
* transportable objects until the transport is released
    LOOP AT it_tadir ASSIGNING <ls_tadir>.
      IF sy-tabix MOD 200 = 0.
        li_progress->show(
          iv_current = sy-tabix
          iv_text    = |Check object exists { <ls_tadir>-object } { <ls_tadir>-obj_name }| ).
      ENDIF.

      ls_item-obj_type = <ls_tadir>-object.
      ls_item-obj_name = <ls_tadir>-obj_name.
      ls_item-devclass = <ls_tadir>-devclass.

      IF exists( ls_item ) = abap_true.
        APPEND <ls_tadir> TO rt_tadir.
      ENDIF.
    ENDLOOP.

  ENDMETHOD.


  METHOD exists.

    IF is_item IS INITIAL.
      RETURN.
    ENDIF.

    IF zcl_abapgit_objects=>is_supported( is_item ) = abap_false.
      rv_exists = abap_true.
      RETURN.
    ENDIF.

    rv_exists = zcl_abapgit_objects=>exists( is_item ).

  ENDMETHOD.


  METHOD zif_abapgit_tadir~get_object_package.

    DATA: ls_tadir TYPE zif_abapgit_definitions=>ty_tadir,
          ls_item  TYPE zif_abapgit_definitions=>ty_item.

    ls_tadir = zif_abapgit_tadir~read_single(
      iv_pgmid    = iv_pgmid
      iv_object   = iv_object
      iv_obj_name = iv_obj_name ).

    IF ls_tadir-delflag = 'X'.
      RETURN. "Mark for deletion -> return nothing
    ENDIF.

    ls_item-obj_type = ls_tadir-object.
    ls_item-obj_name = ls_tadir-obj_name.
    ls_item-devclass = ls_tadir-devclass.
    IF exists( ls_item ) = abap_false.
      RETURN.
    ENDIF.

    rv_devclass = ls_tadir-devclass.

  ENDMETHOD.


  METHOD zif_abapgit_tadir~read.

    DATA: li_exit TYPE REF TO zif_abapgit_exit.

* start recursion
* hmm, some problems here, should TADIR also build path?
    rt_tadir = build( iv_package            = iv_package
                      iv_top                = iv_package
                      io_dot                = io_dot
                      iv_ignore_subpackages = iv_ignore_subpackages
                      iv_only_local_objects = iv_only_local_objects
                      io_log                = io_log ).

    li_exit = zcl_abapgit_exit=>get_instance( ).
    li_exit->change_tadir(
      EXPORTING
        iv_package = iv_package
        io_log     = io_log
      CHANGING
        ct_tadir   = rt_tadir ).

    rt_tadir = check_exists( rt_tadir ).

  ENDMETHOD.


  METHOD zif_abapgit_tadir~read_single.

    IF iv_object = 'SICF'.
      TRY.
          CALL METHOD ('ZCL_ABAPGIT_OBJECT_SICF')=>read_tadir
            EXPORTING
              iv_pgmid    = iv_pgmid
              iv_obj_name = iv_obj_name
            RECEIVING
              rs_tadir    = rs_tadir.
        CATCH cx_sy_dyn_call_illegal_method.
* SICF might not be supported in some systems, assume this code is not called
      ENDTRY.
    ELSE.
      SELECT SINGLE * FROM tadir INTO CORRESPONDING FIELDS OF rs_tadir
        WHERE pgmid = iv_pgmid
        AND object = iv_object
        AND obj_name = iv_obj_name.                       "#EC CI_SUBRC
    ENDIF.

  ENDMETHOD.
ENDCLASS.
