class ZCL_ABAPGIT_ZLIB_CONVERT definition
  public
  create public .

public section.

  class-methods BYTE_TO_BITS
    importing
      !IV_HEX type X
    returning
      value(RV_BITS) type STRING .
  class-methods HEX_TO_BITS
    importing
      !IV_HEX type XSEQUENCE
    returning
      value(RV_BITS) type STRING .
  class-methods BITS_TO_INT
    importing
      !IV_BITS type CLIKE
    returning
      value(RV_INT) type I .
  class-methods INT_TO_HEX
    importing
      !IV_INT type I
    returning
      value(RV_HEX) type XSTRING .
ENDCLASS.



CLASS ZCL_ABAPGIT_ZLIB_CONVERT IMPLEMENTATION.


  METHOD bits_to_int.

    DATA: lv_c    TYPE c LENGTH 1,
          lv_bits TYPE string.

    lv_bits = iv_bits.

    WHILE NOT lv_bits IS INITIAL.
      lv_c = lv_bits.
      rv_int = rv_int * 2.
      rv_int = rv_int + lv_c.
      lv_bits = lv_bits+1.
    ENDWHILE.

  ENDMETHOD.


  METHOD byte_to_bits.

    DATA: lv_x TYPE x LENGTH 1,
          lv_c TYPE c LENGTH 8.

    GET BIT 1 OF iv_hex INTO lv_c+0(1).
    GET BIT 2 OF iv_hex INTO lv_c+1(1).
    GET BIT 3 OF iv_hex INTO lv_c+2(1).
    GET BIT 4 OF iv_hex INTO lv_c+3(1).
    GET BIT 5 OF iv_hex INTO lv_c+4(1).
    GET BIT 6 OF iv_hex INTO lv_c+5(1).
    GET BIT 7 OF iv_hex INTO lv_c+6(1).
    GET BIT 8 OF iv_hex INTO lv_c+7(1).

    rv_bits = lv_c.

  ENDMETHOD.


  METHOD hex_to_bits.

    DATA: lv_x   TYPE x LENGTH 1,
          lv_c   TYPE c LENGTH 1,
          lv_bit TYPE i,
          lv_hex TYPE xstring.

    lv_hex = iv_hex.
    WHILE NOT lv_hex IS INITIAL.
      lv_x = lv_hex.
      DO 8 TIMES.
        lv_bit = sy-index.
        GET BIT lv_bit OF lv_x INTO lv_c.
        CONCATENATE rv_bits lv_c INTO rv_bits.
      ENDDO.
      lv_hex = lv_hex+1.
    ENDWHILE.

  ENDMETHOD.


  METHOD int_to_hex.

    DATA: lv_x TYPE x.


    lv_x = iv_int.
    rv_hex = lv_x.

  ENDMETHOD.
ENDCLASS.
