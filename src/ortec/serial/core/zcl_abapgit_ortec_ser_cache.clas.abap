CLASS zcl_abapgit_ortec_ser_cache DEFINITION
  PUBLIC
  FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.
    CLASS-METHODS serialize
      IMPORTING
        is_item                TYPE zif_abapgit_definitions=>ty_item
        is_i18n_params         TYPE zif_abapgit_definitions=>ty_i18n_params
      RETURNING VALUE(rs_serialization) TYPE zif_abapgit_objects=>ty_serialization
      RAISING zcx_abapgit_exception.
  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.



CLASS zcl_abapgit_ortec_ser_cache IMPLEMENTATION.
  METHOD serialize.
    CASE is_item-obj_type.
      WHEN 'FDT0'.
        rs_serialization = zcl_abapgit_ortec_fdt0_cache=>serialize(
          is_item        = is_item
          io_i18n_params = zcl_abapgit_i18n_params=>new( is_params = is_i18n_params ) ).
      WHEN 'SSFO'.
        rs_serialization = zcl_abapgit_ortec_ssfo_cache=>serialize(
          is_item        = is_item
          is_i18n_params = is_i18n_params ).
      WHEN OTHERS.
        rs_serialization = zcl_abapgit_objects=>serialize(
          is_item        = is_item
          io_i18n_params = zcl_abapgit_i18n_params=>new( is_params = is_i18n_params ) ).
    ENDCASE.
  ENDMETHOD.
ENDCLASS.
