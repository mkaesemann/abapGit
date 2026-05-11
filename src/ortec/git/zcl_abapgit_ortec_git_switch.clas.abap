"! <p class="shorttext synchronized">ORTEC Git Extension Feature Toggle</p>
"! Controls activation of ORTEC persistent Git object store features.
"! Reads activation flags from TVARVC entries.
CLASS zcl_abapgit_ortec_git_switch DEFINITION
  PUBLIC
  FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.

    "! Check if ORTEC fast path is globally active.
    "! @parameter rv_active |
    "! ABAP_TRUE if active
    CLASS-METHODS is_active
      RETURNING VALUE(rv_active) TYPE abap_bool.

    "! Check if incremental pack decode is active.
    "! @parameter rv_active |
    "! ABAP_TRUE if active
    CLASS-METHODS is_decode_active
      RETURNING VALUE(rv_active) TYPE abap_bool.

    "! Check if want/have negotiation is active.
    "! @parameter rv_active |
    "! ABAP_TRUE if active
    CLASS-METHODS is_negotiation_active
      RETURNING VALUE(rv_active) TYPE abap_bool.

    "! Check if object persistence is active.
    "! @parameter rv_active |
    "! ABAP_TRUE if active
    CLASS-METHODS is_store_active
      RETURNING VALUE(rv_active) TYPE abap_bool.

    "! Reset cached flags (for testing).
    CLASS-METHODS reset.

  PROTECTED SECTION.
  PRIVATE SECTION.

    CONSTANTS:
      mc_var_active      TYPE rvari_vnam VALUE 'ZAOG_FASTPATH_ACTIVE',
      mc_var_decode       TYPE rvari_vnam VALUE 'ZAOG_DECODE_ACTIVE',
      mc_var_negotiation  TYPE rvari_vnam VALUE 'ZAOG_NEGOTIATION_ACTIVE',
      mc_var_store        TYPE rvari_vnam VALUE 'ZAOG_STORE_ACTIVE'.

    CLASS-DATA gv_initialized TYPE abap_bool.
    CLASS-DATA gv_active TYPE abap_bool.
    CLASS-DATA gv_decode TYPE abap_bool.
    CLASS-DATA gv_negotiation TYPE abap_bool.
    CLASS-DATA gv_store TYPE abap_bool.

    CLASS-METHODS initialize.

    CLASS-METHODS read_tvarvc
      IMPORTING iv_name        TYPE rvari_vnam
      RETURNING VALUE(rv_flag) TYPE abap_bool.

ENDCLASS.



CLASS zcl_abapgit_ortec_git_switch IMPLEMENTATION.

  METHOD is_active.
    IF gv_initialized = abap_false.
      initialize( ).
    ENDIF.
    rv_active = gv_active.
  ENDMETHOD.


  METHOD is_decode_active.
    IF gv_initialized = abap_false.
      initialize( ).
    ENDIF.
    rv_active = gv_decode.
  ENDMETHOD.


  METHOD is_negotiation_active.
    IF gv_initialized = abap_false.
      initialize( ).
    ENDIF.
    rv_active = gv_negotiation.
  ENDMETHOD.


  METHOD is_store_active.
    IF gv_initialized = abap_false.
      initialize( ).
    ENDIF.
    rv_active = gv_store.
  ENDMETHOD.


  METHOD reset.
    CLEAR: gv_initialized, gv_active, gv_decode, gv_negotiation, gv_store.
  ENDMETHOD.


  METHOD initialize.
    gv_active      = read_tvarvc( mc_var_active ).
    gv_decode       = read_tvarvc( mc_var_decode ).
    gv_negotiation  = read_tvarvc( mc_var_negotiation ).
    gv_store        = read_tvarvc( mc_var_store ).
    gv_initialized = abap_true.
  ENDMETHOD.


  METHOD read_tvarvc.
    DATA lv_val TYPE tvarvc-low.
    SELECT SINGLE low INTO lv_val FROM tvarvc
      WHERE name = iv_name AND type = 'P'.
    IF sy-subrc = 0 AND lv_val = 'X'.
      rv_flag = abap_true.
    ENDIF.
  ENDMETHOD.

ENDCLASS.
