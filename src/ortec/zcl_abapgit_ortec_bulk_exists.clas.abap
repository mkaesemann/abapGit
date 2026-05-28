"! <p class="shorttext synchronized">ORTEC bulk abapGit exists checks</p>
"! Filters abapGit TADIR rows with set-based existence checks where exact
"! parity with the standard object-specific implementation is known.
CLASS zcl_abapgit_ortec_bulk_exists DEFINITION
  PUBLIC FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.
    "! Filter TADIR rows to objects that still exist.
    "! Uses bulk providers for proven object types and delegates all other
    "! checks to the standard abapGit per-object path.
    "! @parameter it_tadir |
    "! TADIR rows to check.
    "! @parameter rt_tadir |
    "! Existing rows in the original input order.
    CLASS-METHODS filter_existing
      IMPORTING it_tadir        TYPE zif_abapgit_definitions=>ty_tadir_tt
      RETURNING VALUE(rt_tadir) TYPE zif_abapgit_definitions=>ty_tadir_tt.

  PRIVATE SECTION.
    TYPES ty_tadir TYPE LINE OF zif_abapgit_definitions=>ty_tadir_tt.
    TYPES ty_doma_keys TYPE HASHED TABLE OF dd01l-domname WITH UNIQUE KEY table_line.
    TYPES ty_dsys_keys TYPE HASHED TABLE OF dokil-object WITH UNIQUE KEY table_line.
    TYPES ty_fugr_keys TYPE HASHED TABLE OF tlibg-area WITH UNIQUE KEY table_line.
    TYPES ty_msag_keys TYPE HASHED TABLE OF t100a-arbgb WITH UNIQUE KEY table_line.
    TYPES ty_prog_keys TYPE HASHED TABLE OF reposrc-progname WITH UNIQUE KEY table_line.
    TYPES ty_shlp_keys TYPE HASHED TABLE OF dd30l-shlpname WITH UNIQUE KEY table_line.
    TYPES ty_smim_keys TYPE HASHED TABLE OF smimloio-loio_id WITH UNIQUE KEY table_line.
    TYPES ty_tran_keys TYPE HASHED TABLE OF tstc-tcode WITH UNIQUE KEY table_line.
    TYPES ty_ttyp_keys TYPE HASHED TABLE OF dd40l-typename WITH UNIQUE KEY table_line.

    TYPES:
      BEGIN OF ty_tobj_key,
        objectname TYPE objh-objectname,
        objecttype TYPE objh-objecttype,
      END OF ty_tobj_key.
    TYPES ty_tobj_keys TYPE HASHED TABLE OF ty_tobj_key WITH UNIQUE KEY objectname objecttype.

    CLASS-METHODS build_doma_buffer
      IMPORTING it_tadir    TYPE zif_abapgit_definitions=>ty_tadir_tt
      EXPORTING et_existing TYPE ty_doma_keys
                ev_success  TYPE abap_bool.

    CLASS-METHODS build_dsys_buffer
      IMPORTING it_tadir    TYPE zif_abapgit_definitions=>ty_tadir_tt
      EXPORTING et_existing TYPE ty_dsys_keys
                ev_success  TYPE abap_bool.

    CLASS-METHODS build_fugr_buffer
      IMPORTING it_tadir    TYPE zif_abapgit_definitions=>ty_tadir_tt
      EXPORTING et_existing TYPE ty_fugr_keys
                et_generated TYPE ty_fugr_keys
                ev_success  TYPE abap_bool.

    CLASS-METHODS build_msag_buffer
      IMPORTING it_tadir    TYPE zif_abapgit_definitions=>ty_tadir_tt
      EXPORTING et_existing TYPE ty_msag_keys
                ev_success  TYPE abap_bool.

    CLASS-METHODS build_prog_buffer
      IMPORTING it_tadir    TYPE zif_abapgit_definitions=>ty_tadir_tt
      EXPORTING et_existing TYPE ty_prog_keys
                ev_success  TYPE abap_bool.

    CLASS-METHODS build_shlp_buffer
      IMPORTING it_tadir    TYPE zif_abapgit_definitions=>ty_tadir_tt
      EXPORTING et_existing TYPE ty_shlp_keys
                ev_success  TYPE abap_bool.

    CLASS-METHODS build_smim_buffer
      IMPORTING it_tadir    TYPE zif_abapgit_definitions=>ty_tadir_tt
      EXPORTING et_existing TYPE ty_smim_keys
                ev_success  TYPE abap_bool.

    CLASS-METHODS build_tobj_buffer
      IMPORTING it_tadir    TYPE zif_abapgit_definitions=>ty_tadir_tt
      EXPORTING et_existing TYPE ty_tobj_keys
                ev_success  TYPE abap_bool.

    CLASS-METHODS build_tran_buffer
      IMPORTING it_tadir    TYPE zif_abapgit_definitions=>ty_tadir_tt
      EXPORTING et_existing TYPE ty_tran_keys
                ev_success  TYPE abap_bool.

    CLASS-METHODS build_ttyp_buffer
      IMPORTING it_tadir    TYPE zif_abapgit_definitions=>ty_tadir_tt
      EXPORTING et_existing TYPE ty_ttyp_keys
                ev_success  TYPE abap_bool.

    CLASS-METHODS exists_standard
      IMPORTING is_tadir         TYPE ty_tadir
      RETURNING VALUE(rv_exists) TYPE abap_bool.

    CLASS-METHODS get_dsys_object
      IMPORTING iv_obj_name       TYPE tadir-obj_name
      RETURNING VALUE(rv_object)  TYPE dokil-object.

    CLASS-METHODS get_tobj_key
      IMPORTING iv_obj_name     TYPE tadir-obj_name
      EXPORTING es_key          TYPE ty_tobj_key
                ev_success      TYPE abap_bool.

    CLASS-METHODS is_generated_chdo_prog
      IMPORTING iv_obj_name         TYPE tadir-obj_name
      RETURNING VALUE(rv_generated) TYPE abap_bool.

ENDCLASS.

CLASS zcl_abapgit_ortec_bulk_exists IMPLEMENTATION.
  METHOD build_doma_buffer.
    DATA lt_keys TYPE ty_doma_keys.
    DATA lv_domname TYPE dd01l-domname.

    CLEAR et_existing.
    ev_success = abap_true.

    LOOP AT it_tadir INTO DATA(ls_tadir) WHERE object = 'DOMA'.
      lv_domname = ls_tadir-obj_name.
      INSERT lv_domname INTO TABLE lt_keys.
    ENDLOOP.

    IF lt_keys IS INITIAL.
      RETURN.
    ENDIF.

    TRY.
        SELECT domname
          FROM dd01l
          INTO TABLE @et_existing
          FOR ALL ENTRIES IN @lt_keys
          WHERE domname = @lt_keys-table_line.
      CATCH cx_root.
        CLEAR et_existing.
        ev_success = abap_false.
    ENDTRY.
  ENDMETHOD.

  METHOD build_dsys_buffer.
    DATA lt_keys TYPE ty_dsys_keys.

    CLEAR et_existing.
    ev_success = abap_true.

    LOOP AT it_tadir INTO DATA(ls_tadir) WHERE object = 'DSYS'.
      INSERT get_dsys_object( ls_tadir-obj_name ) INTO TABLE lt_keys.
    ENDLOOP.

    IF lt_keys IS INITIAL.
      RETURN.
    ENDIF.

    TRY.
        SELECT object
          FROM dokil
          INTO TABLE @et_existing
          FOR ALL ENTRIES IN @lt_keys
          WHERE id = 'HY'
            AND object = @lt_keys-table_line.
      CATCH cx_root.
        CLEAR et_existing.
        ev_success = abap_false.
    ENDTRY.
  ENDMETHOD.

  METHOD build_fugr_buffer.
    DATA lt_keys TYPE ty_fugr_keys.
    DATA lv_area TYPE tlibg-area.

    CLEAR et_existing.
    CLEAR et_generated.
    ev_success = abap_true.

    LOOP AT it_tadir INTO DATA(ls_tadir) WHERE object = 'FUGR'.
      lv_area = ls_tadir-obj_name.
      INSERT lv_area INTO TABLE lt_keys.
    ENDLOOP.

    IF lt_keys IS INITIAL.
      RETURN.
    ENDIF.

    TRY.
        SELECT area
          FROM tlibg
          INTO TABLE @et_existing
          FOR ALL ENTRIES IN @lt_keys
          WHERE area = @lt_keys-table_line.

        SELECT fgrp
          FROM tcdrp
          INTO TABLE @et_generated
          FOR ALL ENTRIES IN @lt_keys
          WHERE fgrp = @lt_keys-table_line.
      CATCH cx_root.
        CLEAR et_existing.
        CLEAR et_generated.
        ev_success = abap_false.
    ENDTRY.
  ENDMETHOD.

  METHOD build_msag_buffer.
    DATA lt_keys TYPE ty_msag_keys.
    DATA lv_arbgb TYPE t100a-arbgb.

    CLEAR et_existing.
    ev_success = abap_true.

    LOOP AT it_tadir INTO DATA(ls_tadir) WHERE object = 'MSAG'.
      lv_arbgb = ls_tadir-obj_name.
      INSERT lv_arbgb INTO TABLE lt_keys.
    ENDLOOP.

    IF lt_keys IS INITIAL.
      RETURN.
    ENDIF.

    TRY.
        SELECT arbgb
          FROM t100a
          INTO TABLE @et_existing
          FOR ALL ENTRIES IN @lt_keys
          WHERE arbgb = @lt_keys-table_line.
      CATCH cx_root.
        CLEAR et_existing.
        ev_success = abap_false.
    ENDTRY.
  ENDMETHOD.

  METHOD build_prog_buffer.
    DATA lt_keys TYPE ty_prog_keys.
    DATA lv_progname TYPE reposrc-progname.

    CLEAR et_existing.
    ev_success = abap_true.

    LOOP AT it_tadir INTO DATA(ls_tadir) WHERE object = 'PROG'.
      lv_progname = ls_tadir-obj_name.
      INSERT lv_progname INTO TABLE lt_keys.
    ENDLOOP.

    IF lt_keys IS INITIAL.
      RETURN.
    ENDIF.

    TRY.
        SELECT progname
          FROM reposrc
          INTO TABLE @et_existing
          FOR ALL ENTRIES IN @lt_keys
          WHERE progname = @lt_keys-table_line.
      CATCH cx_root.
        CLEAR et_existing.
        ev_success = abap_false.
    ENDTRY.
  ENDMETHOD.

  METHOD build_shlp_buffer.
    DATA lt_keys TYPE ty_shlp_keys.
    DATA lv_shlpname TYPE dd30l-shlpname.

    CLEAR et_existing.
    ev_success = abap_true.

    LOOP AT it_tadir INTO DATA(ls_tadir) WHERE object = 'SHLP'.
      lv_shlpname = ls_tadir-obj_name.
      INSERT lv_shlpname INTO TABLE lt_keys.
    ENDLOOP.

    IF lt_keys IS INITIAL.
      RETURN.
    ENDIF.

    TRY.
        SELECT shlpname
          FROM dd30l
          INTO TABLE @et_existing
          FOR ALL ENTRIES IN @lt_keys
          WHERE shlpname = @lt_keys-table_line.
      CATCH cx_root.
        CLEAR et_existing.
        ev_success = abap_false.
    ENDTRY.
  ENDMETHOD.

  METHOD build_smim_buffer.
    DATA lt_keys TYPE ty_smim_keys.
    DATA lv_loio TYPE smimloio-loio_id.

    CLEAR et_existing.
    ev_success = abap_true.

    LOOP AT it_tadir INTO DATA(ls_tadir) WHERE object = 'SMIM'.
      lv_loio = ls_tadir-obj_name.
      INSERT lv_loio INTO TABLE lt_keys.
    ENDLOOP.

    IF lt_keys IS INITIAL.
      RETURN.
    ENDIF.

    TRY.
        SELECT loio_id
          FROM smimloio
          INTO TABLE @et_existing
          FOR ALL ENTRIES IN @lt_keys
          WHERE loio_id = @lt_keys-table_line.
      CATCH cx_root.
        CLEAR et_existing.
        ev_success = abap_false.
    ENDTRY.
  ENDMETHOD.

  METHOD build_tobj_buffer.
    DATA lt_keys TYPE ty_tobj_keys.
    DATA ls_key TYPE ty_tobj_key.
    DATA lv_success TYPE abap_bool.

    CLEAR et_existing.
    ev_success = abap_true.

    LOOP AT it_tadir INTO DATA(ls_tadir) WHERE object = 'TOBJ'.
      get_tobj_key(
        EXPORTING iv_obj_name = ls_tadir-obj_name
        IMPORTING es_key      = ls_key
                  ev_success  = lv_success ).
      IF lv_success = abap_false.
        ev_success = abap_false.
        RETURN.
      ENDIF.
      INSERT ls_key INTO TABLE lt_keys.
    ENDLOOP.

    IF lt_keys IS INITIAL.
      RETURN.
    ENDIF.

    TRY.
        SELECT objectname, objecttype
          FROM objh
          INTO TABLE @et_existing
          FOR ALL ENTRIES IN @lt_keys
          WHERE objectname = @lt_keys-objectname
            AND objecttype = @lt_keys-objecttype.
      CATCH cx_root.
        CLEAR et_existing.
        ev_success = abap_false.
    ENDTRY.
  ENDMETHOD.

  METHOD build_tran_buffer.
    DATA lt_keys TYPE ty_tran_keys.
    DATA lv_tcode TYPE tstc-tcode.

    CLEAR et_existing.
    ev_success = abap_true.

    LOOP AT it_tadir INTO DATA(ls_tadir) WHERE object = 'TRAN'.
      lv_tcode = ls_tadir-obj_name.
      INSERT lv_tcode INTO TABLE lt_keys.
    ENDLOOP.

    IF lt_keys IS INITIAL.
      RETURN.
    ENDIF.

    TRY.
        SELECT tcode
          FROM tstc
          INTO TABLE @et_existing
          FOR ALL ENTRIES IN @lt_keys
          WHERE tcode = @lt_keys-table_line.
      CATCH cx_root.
        CLEAR et_existing.
        ev_success = abap_false.
    ENDTRY.
  ENDMETHOD.

  METHOD build_ttyp_buffer.
    DATA lt_keys TYPE ty_ttyp_keys.
    DATA lv_typename TYPE dd40l-typename.

    CLEAR et_existing.
    ev_success = abap_true.

    LOOP AT it_tadir INTO DATA(ls_tadir) WHERE object = 'TTYP'.
      lv_typename = ls_tadir-obj_name.
      INSERT lv_typename INTO TABLE lt_keys.
    ENDLOOP.

    IF lt_keys IS INITIAL.
      RETURN.
    ENDIF.

    TRY.
        SELECT typename
          FROM dd40l
          INTO TABLE @et_existing
          FOR ALL ENTRIES IN @lt_keys
          WHERE typename = @lt_keys-table_line.
      CATCH cx_root.
        CLEAR et_existing.
        ev_success = abap_false.
    ENDTRY.
  ENDMETHOD.

  METHOD exists_standard.
    DATA ls_item TYPE zif_abapgit_definitions=>ty_item.

    ls_item-obj_type = is_tadir-object.
    ls_item-obj_name = is_tadir-obj_name.
    ls_item-devclass = is_tadir-devclass.

    rv_exists = zcl_abapgit_objects=>exists( ls_item ).
  ENDMETHOD.

  METHOD filter_existing.
    DATA lt_existing_doma TYPE ty_doma_keys.
    DATA lt_existing_dsys TYPE ty_dsys_keys.
    DATA lt_existing_fugr TYPE ty_fugr_keys.
    DATA lt_existing_msag TYPE ty_msag_keys.
    DATA lt_existing_prog TYPE ty_prog_keys.
    DATA lt_existing_shlp TYPE ty_shlp_keys.
    DATA lt_existing_smim TYPE ty_smim_keys.
    DATA lt_existing_tobj TYPE ty_tobj_keys.
    DATA lt_existing_tran TYPE ty_tran_keys.
    DATA lt_existing_ttyp TYPE ty_ttyp_keys.
    DATA lt_generated_fugr TYPE ty_fugr_keys.
    DATA lv_doma_success TYPE abap_bool.
    DATA lv_dsys_success TYPE abap_bool.
    DATA lv_fugr_success TYPE abap_bool.
    DATA lv_msag_success TYPE abap_bool.
    DATA lv_prog_success TYPE abap_bool.
    DATA lv_shlp_success TYPE abap_bool.
    DATA lv_smim_success TYPE abap_bool.
    DATA lv_tobj_success TYPE abap_bool.
    DATA lv_tran_success TYPE abap_bool.
    DATA lv_ttyp_success TYPE abap_bool.
    DATA lv_domname TYPE dd01l-domname.
    DATA lv_dsys_object TYPE dokil-object.
    DATA lv_fugr TYPE tlibg-area.
    DATA lv_arbgb TYPE t100a-arbgb.
    DATA lv_progname TYPE reposrc-progname.
    DATA lv_shlpname TYPE dd30l-shlpname.
    DATA lv_loio TYPE smimloio-loio_id.
    DATA lv_tcode TYPE tstc-tcode.
    DATA lv_typename TYPE dd40l-typename.
    DATA ls_tobj_key TYPE ty_tobj_key.
    DATA lv_tobj_key_success TYPE abap_bool.

    build_doma_buffer(
      EXPORTING it_tadir    = it_tadir
      IMPORTING et_existing = lt_existing_doma
                ev_success  = lv_doma_success ).

    build_dsys_buffer(
      EXPORTING it_tadir    = it_tadir
      IMPORTING et_existing = lt_existing_dsys
                ev_success  = lv_dsys_success ).

    build_fugr_buffer(
      EXPORTING it_tadir     = it_tadir
      IMPORTING et_existing  = lt_existing_fugr
                et_generated = lt_generated_fugr
                ev_success   = lv_fugr_success ).

    build_msag_buffer(
      EXPORTING it_tadir    = it_tadir
      IMPORTING et_existing = lt_existing_msag
                ev_success  = lv_msag_success ).

    build_prog_buffer(
      EXPORTING it_tadir    = it_tadir
      IMPORTING et_existing = lt_existing_prog
                ev_success  = lv_prog_success ).

    build_shlp_buffer(
      EXPORTING it_tadir    = it_tadir
      IMPORTING et_existing = lt_existing_shlp
                ev_success  = lv_shlp_success ).

    build_smim_buffer(
      EXPORTING it_tadir    = it_tadir
      IMPORTING et_existing = lt_existing_smim
                ev_success  = lv_smim_success ).

    build_tobj_buffer(
      EXPORTING it_tadir    = it_tadir
      IMPORTING et_existing = lt_existing_tobj
                ev_success  = lv_tobj_success ).

    build_tran_buffer(
      EXPORTING it_tadir    = it_tadir
      IMPORTING et_existing = lt_existing_tran
                ev_success  = lv_tran_success ).

    build_ttyp_buffer(
      EXPORTING it_tadir    = it_tadir
      IMPORTING et_existing = lt_existing_ttyp
                ev_success  = lv_ttyp_success ).

    LOOP AT it_tadir INTO DATA(ls_tadir).
      CASE ls_tadir-object.
        WHEN 'DOMA'.
          lv_domname = ls_tadir-obj_name.
          IF lv_doma_success = abap_false.
            IF exists_standard( ls_tadir ) = abap_true.
              APPEND ls_tadir TO rt_tadir.
            ENDIF.
          ELSEIF line_exists( lt_existing_doma[ table_line = lv_domname ] ).
            APPEND ls_tadir TO rt_tadir.
          ENDIF.

        WHEN 'DSYS'.
          lv_dsys_object = get_dsys_object( ls_tadir-obj_name ).
          IF lv_dsys_success = abap_false.
            IF exists_standard( ls_tadir ) = abap_true.
              APPEND ls_tadir TO rt_tadir.
            ENDIF.
          ELSEIF line_exists( lt_existing_dsys[ table_line = lv_dsys_object ] ).
            APPEND ls_tadir TO rt_tadir.
          ENDIF.

        WHEN 'FUGR'.
          lv_fugr = ls_tadir-obj_name.
          IF lv_fugr_success = abap_false.
            IF exists_standard( ls_tadir ) = abap_true.
              APPEND ls_tadir TO rt_tadir.
            ENDIF.
          ELSEIF line_exists( lt_existing_fugr[ table_line = lv_fugr ] )
              AND NOT line_exists( lt_generated_fugr[ table_line = lv_fugr ] ).
            APPEND ls_tadir TO rt_tadir.
          ENDIF.

        WHEN 'MSAG'.
          lv_arbgb = ls_tadir-obj_name.
          IF lv_msag_success = abap_false.
            IF exists_standard( ls_tadir ) = abap_true.
              APPEND ls_tadir TO rt_tadir.
            ENDIF.
          ELSEIF line_exists( lt_existing_msag[ table_line = lv_arbgb ] ).
            APPEND ls_tadir TO rt_tadir.
          ENDIF.

        WHEN 'PROG'.
          lv_progname = ls_tadir-obj_name.
          IF lv_prog_success = abap_false.
            IF exists_standard( ls_tadir ) = abap_true.
              APPEND ls_tadir TO rt_tadir.
            ENDIF.
          ELSEIF line_exists( lt_existing_prog[ table_line = lv_progname ] )
              AND is_generated_chdo_prog( ls_tadir-obj_name ) = abap_false.
            APPEND ls_tadir TO rt_tadir.
          ENDIF.

        WHEN 'SHLP'.
          lv_shlpname = ls_tadir-obj_name.
          IF lv_shlp_success = abap_false.
            IF exists_standard( ls_tadir ) = abap_true.
              APPEND ls_tadir TO rt_tadir.
            ENDIF.
          ELSEIF line_exists( lt_existing_shlp[ table_line = lv_shlpname ] ).
            APPEND ls_tadir TO rt_tadir.
          ENDIF.

        WHEN 'SMIM'.
          lv_loio = ls_tadir-obj_name.
          IF lv_smim_success = abap_false.
            IF exists_standard( ls_tadir ) = abap_true.
              APPEND ls_tadir TO rt_tadir.
            ENDIF.
          ELSEIF line_exists( lt_existing_smim[ table_line = lv_loio ] ).
            APPEND ls_tadir TO rt_tadir.
          ENDIF.

        WHEN 'TOBJ'.
          get_tobj_key(
            EXPORTING iv_obj_name = ls_tadir-obj_name
            IMPORTING es_key      = ls_tobj_key
                      ev_success  = lv_tobj_key_success ).
          IF lv_tobj_success = abap_false OR lv_tobj_key_success = abap_false.
            IF exists_standard( ls_tadir ) = abap_true.
              APPEND ls_tadir TO rt_tadir.
            ENDIF.
          ELSEIF line_exists( lt_existing_tobj[ objectname = ls_tobj_key-objectname
                                                objecttype = ls_tobj_key-objecttype ] ).
            APPEND ls_tadir TO rt_tadir.
          ENDIF.

        WHEN 'TRAN'.
          lv_tcode = ls_tadir-obj_name.
          IF lv_tran_success = abap_false.
            IF exists_standard( ls_tadir ) = abap_true.
              APPEND ls_tadir TO rt_tadir.
            ENDIF.
          ELSEIF line_exists( lt_existing_tran[ table_line = lv_tcode ] ).
            APPEND ls_tadir TO rt_tadir.
          ENDIF.

        WHEN 'TTYP'.
          lv_typename = ls_tadir-obj_name.
          IF lv_ttyp_success = abap_false.
            IF exists_standard( ls_tadir ) = abap_true.
              APPEND ls_tadir TO rt_tadir.
            ENDIF.
          ELSEIF line_exists( lt_existing_ttyp[ table_line = lv_typename ] ).
            APPEND ls_tadir TO rt_tadir.
          ENDIF.

        WHEN OTHERS.
          IF exists_standard( ls_tadir ) = abap_true.
            APPEND ls_tadir TO rt_tadir.
          ENDIF.
      ENDCASE.
    ENDLOOP.
  ENDMETHOD.

  METHOD get_dsys_object.
    DATA lv_prefix TYPE namespace.
    DATA lv_bare_name TYPE progname.

    IF iv_obj_name IS NOT INITIAL AND iv_obj_name(1) = '/'.
      CALL FUNCTION 'RS_NAME_SPLIT_NAMESPACE'
        EXPORTING
          name_with_namespace    = iv_obj_name
        IMPORTING
          namespace              = lv_prefix
          name_without_namespace = lv_bare_name.

      rv_object = |{ lv_bare_name+0(4) }{ lv_prefix }{ lv_bare_name+4(*) }|.
    ELSE.
      rv_object = iv_obj_name.
    ENDIF.
  ENDMETHOD.

  METHOD get_tobj_key.
    DATA lv_type_pos TYPE i.

    CLEAR es_key.
    CLEAR ev_success.

    IF iv_obj_name IS INITIAL.
      RETURN.
    ENDIF.

    lv_type_pos = strlen( iv_obj_name ) - 1.
    es_key-objectname = iv_obj_name(lv_type_pos).
    es_key-objecttype = iv_obj_name+lv_type_pos.
    ev_success = abap_true.
  ENDMETHOD.

  METHOD is_generated_chdo_prog.
    FIND REGEX '^F.*CD[C|F|T|V]' IN iv_obj_name ##REGEX_POSIX.
    IF sy-subrc <> 0.
      FIND REGEX '^/.*/F.*CD[C|F|T|V]' IN iv_obj_name ##REGEX_POSIX.
    ENDIF.

    rv_generated = boolc( sy-subrc = 0 ).
  ENDMETHOD.
ENDCLASS.
