*"* use this source file for the definition and implementation of
*"* local helper classes, interface definitions and type
*"* declarations

CLASS lcl_ungzip_handler IMPLEMENTATION.
  METHOD if_abap_ungzip_binary_handler~use_out_buf.
    " Append the decompressed chunk to the accumulator
    DATA lv_chunk TYPE xstring.
    lv_chunk = out_buf(out_buf_len).
    CONCATENATE gv_data lv_chunk INTO gv_data IN BYTE MODE.
  ENDMETHOD.

  METHOD reset.
    CLEAR gv_data.
  ENDMETHOD.

  METHOD get_data.
    rv_data = gv_data.
  ENDMETHOD.
ENDCLASS.
