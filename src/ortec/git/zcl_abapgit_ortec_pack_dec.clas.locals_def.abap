*"* use this source file for any type of declarations (class
*"* definitions, interfaces or type declarations) you need for
*"* components in the private section

*----------------------------------------------------------------------*
* Local handler for CL_ABAP_UNGZIP_BINARY_STREAM (Optimization #6)
*----------------------------------------------------------------------*
CLASS lcl_ungzip_handler DEFINITION.
  PUBLIC SECTION.
    INTERFACES if_abap_ungzip_binary_handler.
    CLASS-METHODS reset.
    CLASS-METHODS get_data RETURNING VALUE(rv_data) TYPE xstring.
  PRIVATE SECTION.
    CLASS-DATA gv_data TYPE xstring.
ENDCLASS.
