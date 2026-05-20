CLASS zcl_abapgit_zlib_stream DEFINITION
  PUBLIC
  CREATE PUBLIC .

  PUBLIC SECTION.

    CLASS-METHODS class_constructor .
    METHODS constructor
      IMPORTING
        !iv_data TYPE xstring .
    METHODS take_bits
      IMPORTING
        !iv_length     TYPE i
      RETURNING
        VALUE(rv_bits) TYPE string .
    METHODS take_bit
      RETURNING
        VALUE(rv_bit) TYPE i .
    METHODS take_int
      IMPORTING
        !iv_length    TYPE i
      RETURNING
        VALUE(rv_int) TYPE i .
    METHODS remaining
      RETURNING
        VALUE(rv_length) TYPE i .
    "! Take bytes, there's an implicit realignment to start at the beginning of a byte
    "! i.e. if next bit of current byte is not the first bit, then this byte is skipped
    "! and the bytes are taken from the next one.
    "! @parameter iv_length | <p class="shorttext synchronized" lang="en"></p>
    "! @parameter rv_bytes | <p class="shorttext synchronized" lang="en"></p>
    METHODS take_bytes
      IMPORTING
        !iv_length      TYPE i
      RETURNING
        VALUE(rv_bytes) TYPE xstring .
    METHODS clear_bits .
    "! Enable integer-based bit I/O (optimization #2 + #4).
    "! When enabled, take_bit and take_int use an integer accumulator
    "! instead of string-based bit manipulation — significantly faster.
    METHODS enable_fast_mode .
  PROTECTED SECTION.
  PRIVATE SECTION.

    CLASS-DATA gt_byte_bits TYPE STANDARD TABLE OF string WITH DEFAULT KEY .
    DATA mv_bits TYPE string .
    DATA mv_compressed TYPE xstring .
    DATA mv_offset TYPE i.
    " Integer bit accumulator for fast mode (#2)
    DATA mv_fast_mode   TYPE abap_bool.
    DATA mv_bit_buf     TYPE i.   " current byte value (0-255)
    DATA mv_bits_left   TYPE i.   " bits remaining in mv_bit_buf (0-8)
ENDCLASS.



CLASS zcl_abapgit_zlib_stream IMPLEMENTATION.


  METHOD class_constructor.

    DATA:
      lv_x    TYPE x LENGTH 1,
      lv_bits TYPE string,
      lv_c    TYPE c,
      lv_int  TYPE i.

    DO 256 TIMES.
      lv_int = sy-index - 1.
      lv_x = lv_int.
      CLEAR lv_bits.
      DO 8 TIMES.
        GET BIT sy-index OF lv_x INTO lv_c.
        CONCATENATE lv_bits lv_c INTO lv_bits.
      ENDDO.
      APPEND lv_bits TO gt_byte_bits.
    ENDDO.

  ENDMETHOD.


  METHOD enable_fast_mode.
    mv_fast_mode = abap_true.
    mv_bits_left = 0.
  ENDMETHOD.


  METHOD clear_bits.
    IF mv_fast_mode = abap_true.
      mv_bits_left = 0.
    ELSE.
      CLEAR mv_bits.
    ENDIF.
  ENDMETHOD.


  METHOD constructor.

    mv_compressed = iv_data.
    mv_offset     = 0.

  ENDMETHOD.


  METHOD remaining.

    rv_length = xstrlen( mv_compressed ) + 1 - mv_offset.

  ENDMETHOD.


  METHOD take_bits.

    DATA:
      lv_left  TYPE i,
      lv_index TYPE i.

    IF mv_fast_mode = abap_true.
      " Fast-mode path: read bits from integer accumulator and build
      " the result string in the same format as the legacy path.
      " Bits are extracted LSB-first; the result string has the first
      " extracted bit at the rightmost position (same as legacy layout).
      DATA lv_bit TYPE i.
      DATA lv_bit_c TYPE c LENGTH 1.
      WHILE strlen( rv_bits ) < iv_length.
        IF mv_bits_left = 0.
          mv_bit_buf = mv_compressed+mv_offset(1).
          mv_offset += 1.
          mv_bits_left = 8.
        ENDIF.
        lv_bit = mv_bit_buf MOD 2.
        mv_bit_buf = mv_bit_buf DIV 2.
        mv_bits_left -= 1.
        lv_bit_c = lv_bit.
        " Prepend: first extracted bit (LSB) goes to the right
        CONCATENATE lv_bit_c rv_bits INTO rv_bits.
      ENDWHILE.
      RETURN.
    ENDIF.

    WHILE strlen( rv_bits ) < iv_length.
      IF mv_bits IS INITIAL.
        lv_index = mv_compressed+mv_offset(1) + 1.
        " take precalculated bits for the byte value
        READ TABLE gt_byte_bits INTO mv_bits INDEX lv_index.
        mv_offset = mv_offset + 1.
      ENDIF.
      lv_left = iv_length - strlen( rv_bits ).
      IF lv_left >= strlen( mv_bits ).
        CONCATENATE mv_bits rv_bits INTO rv_bits.
        CLEAR mv_bits.
      ELSE.
        lv_index = strlen( mv_bits ) - lv_left.
        CONCATENATE mv_bits+lv_index(lv_left) rv_bits INTO rv_bits.
        mv_bits = mv_bits(lv_index).
      ENDIF.

    ENDWHILE.

  ENDMETHOD.


  METHOD take_bit.

    IF mv_fast_mode = abap_true.
      " Optimization #2: Integer bit accumulator.
      " DEFLATE reads bits LSB-first within each byte.
      IF mv_bits_left = 0.
        mv_bit_buf = mv_compressed+mv_offset(1).
        mv_offset += 1.
        mv_bits_left = 8.
      ENDIF.
      rv_bit = mv_bit_buf MOD 2.
      mv_bit_buf = mv_bit_buf DIV 2.
      mv_bits_left -= 1.
    ELSE.
      " Legacy path: string-based bit extraction
      DATA: lv_index TYPE i,
            lv_len TYPE i.

      IF mv_bits IS INITIAL.
        lv_index = mv_compressed+mv_offset(1) + 1.
        READ TABLE gt_byte_bits INTO mv_bits INDEX lv_index.
        mv_offset = mv_offset + 1.
      ENDIF.

      lv_len = strlen( mv_bits ) - 1.
      rv_bit = mv_bits+lv_len(1).
      mv_bits = mv_bits(lv_len).
    ENDIF.

  ENDMETHOD.


  METHOD take_bytes.

    IF mv_fast_mode = abap_true.
      " Discard remaining bits in current byte (realignment)
      mv_bits_left = 0.
    ENDIF.

    rv_bytes = mv_compressed+mv_offset(iv_length).
    mv_offset = mv_offset + iv_length.

  ENDMETHOD.


  METHOD take_int.

    IF mv_fast_mode = abap_true AND iv_length > 0.
      " Optimization #4: Direct integer accumulation without string intermediary.
      " DEFLATE extra-bits are read LSB-first, so bit i has weight 2^i.
      DATA lv_factor TYPE i VALUE 1.
      DO iv_length TIMES.
        IF mv_bits_left = 0.
          mv_bit_buf = mv_compressed+mv_offset(1).
          mv_offset += 1.
          mv_bits_left = 8.
        ENDIF.
        rv_int = rv_int + ( mv_bit_buf MOD 2 ) * lv_factor.
        mv_bit_buf = mv_bit_buf DIV 2.
        mv_bits_left -= 1.
        lv_factor = lv_factor * 2.
      ENDDO.
    ELSE.
      " Legacy path or zero-length (no-op for iv_length = 0)
      DATA:
        lv_bits   TYPE string,
        lv_i      TYPE i,
        lv_offset TYPE i.

      lv_bits = take_bits( iv_length ).

      " inlining bits_to_int for better performance
      DO strlen( lv_bits ) TIMES.
        lv_i = lv_bits+lv_offset(1).
        rv_int = rv_int * 2 + lv_i.
        lv_offset = lv_offset + 1.
      ENDDO.
    ENDIF.

  ENDMETHOD.
ENDCLASS.
