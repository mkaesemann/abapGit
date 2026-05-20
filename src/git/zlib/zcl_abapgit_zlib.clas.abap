CLASS zcl_abapgit_zlib DEFINITION
  PUBLIC
  CREATE PUBLIC .

  PUBLIC SECTION.

    TYPES:
      BEGIN OF ty_decompress,
        raw            TYPE xstring,
        compressed_len TYPE i,
      END OF ty_decompress .

    CLASS-METHODS decompress
      IMPORTING
        !iv_compressed TYPE xsequence
      RETURNING
        VALUE(rs_data) TYPE ty_decompress .

  PROTECTED SECTION.
  PRIVATE SECTION.
    CONSTANTS: c_maxdcodes TYPE i VALUE 30.
    "! Optimization switch for #2-#5: integer bit I/O, chunked output, inlined decode.
    "! abap_true = optimized path, abap_false = legacy string-based path.
    CONSTANTS: c_opt2to5_fast_zlib TYPE abap_bool VALUE abap_true.

    CLASS-DATA: gv_out      TYPE xstring,
                go_lencode  TYPE REF TO zcl_abapgit_zlib_huffman,
                go_distcode TYPE REF TO zcl_abapgit_zlib_huffman,
                go_stream   TYPE REF TO zcl_abapgit_zlib_stream,
                gt_order    TYPE TABLE OF i.  " RFC 1951 §3.2.7 code-length order (cached across calls)

    TYPES: BEGIN OF ty_pair,
             length   TYPE i,
             distance TYPE i,
           END OF ty_pair.

    CLASS-METHODS:
      decode
        IMPORTING io_huffman       TYPE REF TO zcl_abapgit_zlib_huffman
        RETURNING VALUE(rv_symbol) TYPE i,
      map_length
        IMPORTING iv_code          TYPE i
        RETURNING VALUE(rv_length) TYPE i,
      map_distance
        IMPORTING iv_code            TYPE i
        RETURNING VALUE(rv_distance) TYPE i,
      dynamic,
      fixed,
      not_compressed,
      decode_loop,
      decode_loop_fast,
      read_pair
        IMPORTING iv_length      TYPE i
        RETURNING VALUE(rs_pair) TYPE ty_pair,
      copy_out
        IMPORTING is_pair TYPE ty_pair.

ENDCLASS.



CLASS zcl_abapgit_zlib IMPLEMENTATION.


  METHOD copy_out.

    " Seed-tiling strategy (equivalent to original byte-by-byte, fewer CONCATENATE calls):
    "   Non-overlapping (length <= distance):  one CONCATENATE for the whole slice.
    "   Overlapping LZ77 run (length > distance): capture the seed pattern once, then tile
    "   it in chunks of 'distance' bytes until 'length' bytes have been appended.
    "   Proof: each byte position i (0-based) maps to gv_out[src + i MOD distance],
    "   which is exactly what the original lv_index loop produced.

    DATA lv_src_offset TYPE i.
    DATA lv_src        TYPE xstring.
    DATA lv_chunk      TYPE xstring.
    DATA lv_remaining  TYPE i.

    lv_src_offset = xstrlen( gv_out ) - is_pair-distance.
    IF lv_src_offset < 0.
      " Invalid back-reference: distance exceeds output produced so far.
      " This indicates a corrupt DEFLATE stream or bit-read desynchronization.
      ASSERT 1 = 0.
    ENDIF.
    lv_src        = gv_out+lv_src_offset(is_pair-distance).

    IF is_pair-length <= is_pair-distance.
      " Non-overlapping: the required slice lives entirely in lv_src.
      lv_chunk = lv_src(is_pair-length).
      CONCATENATE gv_out lv_chunk INTO gv_out IN BYTE MODE.
    ELSE.
      " Overlapping LZ77 run: tile the seed pattern.
      lv_remaining = is_pair-length.
      WHILE lv_remaining > 0.
        IF lv_remaining >= is_pair-distance.
          CONCATENATE gv_out lv_src INTO gv_out IN BYTE MODE.
          lv_remaining -= is_pair-distance.
        ELSE.
          lv_chunk = lv_src(lv_remaining).
          CONCATENATE gv_out lv_chunk INTO gv_out IN BYTE MODE.
          CLEAR lv_remaining.
        ENDIF.
      ENDWHILE.
    ENDIF.

  ENDMETHOD.


  METHOD decode.

    DATA: lv_count TYPE i,
          lv_code  TYPE i,
          lv_index TYPE i,
          lv_first TYPE i.


    DO zcl_abapgit_zlib_huffman=>c_maxbits TIMES.
      lv_count = io_huffman->get_count( sy-index ).

      lv_code = go_stream->take_bit( ) + lv_code * 2.

      IF lv_code - lv_count < lv_first.
        rv_symbol = io_huffman->get_symbol( lv_index + lv_code - lv_first + 1 ).
        RETURN.
      ENDIF.
      lv_index = lv_index + lv_count.
      lv_first = ( lv_first + lv_count ) * 2.
    ENDDO.

  ENDMETHOD.


  METHOD decode_loop.

    DATA lv_x TYPE x.
    DATA lv_symbol TYPE i.

    DO.
      lv_symbol = decode( go_lencode ).

      IF lv_symbol < 256.
        lv_x = lv_symbol.
        CONCATENATE gv_out lv_x INTO gv_out IN BYTE MODE.
      ELSEIF lv_symbol = 256.
        EXIT.
      ELSE.
        copy_out( read_pair( lv_symbol ) ).
      ENDIF.

    ENDDO.

  ENDMETHOD.


  METHOD decode_loop_fast.

    " Optimized decode loop combining:
    "   #3: Chunked literal output (batch single-byte literals into lv_chunk,
    "       flush to gv_out every 4096 bytes — reduces full-buffer copies)
    "   #5: Inlined decode call for the hot path (eliminate method-call overhead
    "       for every symbol by embedding the Huffman traversal here)

    DATA lv_x       TYPE x.
    DATA lv_symbol  TYPE i.
    DATA lv_chunk   TYPE xstring.
    DATA lv_count   TYPE i.
    DATA lv_code    TYPE i.
    DATA lv_index   TYPE i.
    DATA lv_first   TYPE i.

    DO.
      " --- Inlined decode( go_lencode ) ---
      CLEAR: lv_code, lv_index, lv_first.
      DO zcl_abapgit_zlib_huffman=>c_maxbits TIMES.
        lv_count = go_lencode->get_count( sy-index ).
        lv_code = go_stream->take_bit( ) + lv_code * 2.
        IF lv_code - lv_count < lv_first.
          lv_symbol = go_lencode->get_symbol( lv_index + lv_code - lv_first + 1 ).
          EXIT.
        ENDIF.
        lv_index = lv_index + lv_count.
        lv_first = ( lv_first + lv_count ) * 2.
      ENDDO.
      " --- end inlined decode ---

      IF lv_symbol < 256.
        lv_x = lv_symbol.
        CONCATENATE lv_chunk lv_x INTO lv_chunk IN BYTE MODE.
        IF xstrlen( lv_chunk ) >= 4096.
          CONCATENATE gv_out lv_chunk INTO gv_out IN BYTE MODE.
          CLEAR lv_chunk.
        ENDIF.
      ELSEIF lv_symbol = 256.
        " Flush remaining chunk before exiting
        IF lv_chunk IS NOT INITIAL.
          CONCATENATE gv_out lv_chunk INTO gv_out IN BYTE MODE.
        ENDIF.
        EXIT.
      ELSE.
        " Flush chunk before copy_out (it reads from gv_out tail)
        IF lv_chunk IS NOT INITIAL.
          CONCATENATE gv_out lv_chunk INTO gv_out IN BYTE MODE.
          CLEAR lv_chunk.
        ENDIF.
        copy_out( read_pair( lv_symbol ) ).
      ENDIF.

    ENDDO.

  ENDMETHOD.


  METHOD decompress.

    DATA: lv_bfinal TYPE c LENGTH 1,
          lv_btype  TYPE c LENGTH 2.


    IF iv_compressed IS INITIAL.
      RETURN.
    ENDIF.

    CLEAR gv_out.
    CREATE OBJECT go_stream
      EXPORTING
        iv_data = iv_compressed.

    " Enable integer bit-I/O in stream when optimizations #2-#5 are active
    IF c_opt2to5_fast_zlib = abap_true.
      go_stream->enable_fast_mode( ).
    ENDIF.

    DO.
      lv_bfinal = go_stream->take_bits( 1 ).

      lv_btype = go_stream->take_bits( 2 ).
      CASE lv_btype.
        WHEN '00'.
          not_compressed( ).
        WHEN '01'.
          fixed( ).
          IF c_opt2to5_fast_zlib = abap_true.
            decode_loop_fast( ).
          ELSE.
            decode_loop( ).
          ENDIF.
        WHEN '10'.
          dynamic( ).
          IF c_opt2to5_fast_zlib = abap_true.
            decode_loop_fast( ).
          ELSE.
            decode_loop( ).
          ENDIF.
        WHEN OTHERS.
          ASSERT 1 = 0.
      ENDCASE.

      IF lv_bfinal = '1'.
        EXIT.
      ENDIF.

    ENDDO.

    rs_data-raw = gv_out.
    rs_data-compressed_len = xstrlen( iv_compressed ) - go_stream->remaining( ).

  ENDMETHOD.


  METHOD dynamic.

    DATA: lv_nlen    TYPE i,
          lv_ndist   TYPE i,
          lv_ncode   TYPE i,
          lv_index   TYPE i,
          lv_length  TYPE i,
          lv_symbol  TYPE i,
          lt_lengths TYPE zcl_abapgit_zlib_huffman=>ty_lengths,
          lt_dists   TYPE zcl_abapgit_zlib_huffman=>ty_lengths.

    FIELD-SYMBOLS: <lv_length> LIKE LINE OF lt_lengths.


    " RFC 1951 §3.2.7 code-length alphabet reorder — constant sequence, cached globally.
    IF gt_order IS INITIAL.
      gt_order = VALUE #( ( 16 ) ( 17 ) ( 18 ) ( 0 ) ( 8 ) ( 7 ) ( 9 ) ( 6 )
                          ( 10 ) ( 5 ) ( 11 ) ( 4 ) ( 12 ) ( 3 ) ( 13 ) ( 2 )
                          ( 14 ) ( 1 ) ( 15 ) ).
    ENDIF.

    lv_nlen = go_stream->take_int( 5 ) + 257.
    lv_ndist = go_stream->take_int( 5 ) + 1.
    lv_ncode = go_stream->take_int( 4 ) + 4.

    DO 19 TIMES.
      APPEND 0 TO lt_lengths.
    ENDDO.

    DO lv_ncode TIMES.
      READ TABLE gt_order INDEX sy-index INTO lv_index.
      ASSERT sy-subrc = 0.
      lv_index = lv_index + 1.
      READ TABLE lt_lengths INDEX lv_index ASSIGNING <lv_length>.
      ASSERT sy-subrc = 0.
      <lv_length> = go_stream->take_int( 3 ).
    ENDDO.

    CREATE OBJECT go_lencode
      EXPORTING
        it_lengths = lt_lengths.

    CLEAR lt_lengths.
    WHILE lines( lt_lengths ) < lv_nlen + lv_ndist.
      lv_symbol = decode( go_lencode ).

      IF lv_symbol < 16.
        APPEND lv_symbol TO lt_lengths.
      ELSE.
        lv_length = 0.
        IF lv_symbol = 16.
          READ TABLE lt_lengths INDEX lines( lt_lengths ) INTO lv_length.
          ASSERT sy-subrc = 0.
          lv_symbol = go_stream->take_int( 2 ) + 3.
        ELSEIF lv_symbol = 17.
          lv_symbol = go_stream->take_int( 3 ) + 3.
        ELSE.
          lv_symbol = go_stream->take_int( 7 ) + 11.
        ENDIF.
        DO lv_symbol TIMES.
          APPEND lv_length TO lt_lengths.
        ENDDO.
      ENDIF.
    ENDWHILE.

    lt_dists = lt_lengths.
    DELETE lt_lengths FROM lv_nlen + 1.
    DELETE lt_dists TO lv_nlen.

    CREATE OBJECT go_lencode
      EXPORTING
        it_lengths = lt_lengths.

    CREATE OBJECT go_distcode
      EXPORTING
        it_lengths = lt_dists.

  ENDMETHOD.


  METHOD fixed.

    DATA: lt_lengths TYPE zcl_abapgit_zlib_huffman=>ty_lengths.


    DO 144 TIMES.
      APPEND 8 TO lt_lengths.
    ENDDO.
    DO 112 TIMES.
      APPEND 9 TO lt_lengths.
    ENDDO.
    DO 24 TIMES.
      APPEND 7 TO lt_lengths.
    ENDDO.
    DO 8 TIMES.
      APPEND 8 TO lt_lengths.
    ENDDO.

    CREATE OBJECT go_lencode
      EXPORTING
        it_lengths = lt_lengths.

    CLEAR lt_lengths.
    DO c_maxdcodes TIMES.
      APPEND 5 TO lt_lengths.
    ENDDO.

    CREATE OBJECT go_distcode
      EXPORTING
        it_lengths = lt_lengths.

  ENDMETHOD.


  METHOD map_distance.

    " RFC 1951 §3.2.5 distance alphabet — condensed from 30 WHEN to 15 groups.
    " Codes 0-3: no extra bits, base = 1 + code.
    " Codes 4-29 (pairs): extra_bits = (code DIV 2) - 1; within pair: stride = 2 ^ extra_bits.
    " Formula: rv_distance = take_int(extra) + pair_base + (code MOD 2) * stride
    " Spot-checks:
    "   0→1  1→2  2→3  3→4
    "   4→5+0=5  5→5+2=7   6→9+0=9   7→9+4=13
    "   8→17+0=17 9→17+8=25  10→33+0=33 11→33+16=49
    "   28→16385+0=16385  29→16385+8192=24577

    IF iv_code BETWEEN 0 AND 3.
      rv_distance = go_stream->take_int( 0 ) + 1 + iv_code.
    ELSEIF iv_code BETWEEN 4 AND 5.
      rv_distance = go_stream->take_int( 1 ) + 5 + ( iv_code - 4 ) * 2.
    ELSEIF iv_code BETWEEN 6 AND 7.
      rv_distance = go_stream->take_int( 2 ) + 9 + ( iv_code - 6 ) * 4.
    ELSEIF iv_code BETWEEN 8 AND 9.
      rv_distance = go_stream->take_int( 3 ) + 17 + ( iv_code - 8 ) * 8.
    ELSEIF iv_code BETWEEN 10 AND 11.
      rv_distance = go_stream->take_int( 4 ) + 33 + ( iv_code - 10 ) * 16.
    ELSEIF iv_code BETWEEN 12 AND 13.
      rv_distance = go_stream->take_int( 5 ) + 65 + ( iv_code - 12 ) * 32.
    ELSEIF iv_code BETWEEN 14 AND 15.
      rv_distance = go_stream->take_int( 6 ) + 129 + ( iv_code - 14 ) * 64.
    ELSEIF iv_code BETWEEN 16 AND 17.
      rv_distance = go_stream->take_int( 7 ) + 257 + ( iv_code - 16 ) * 128.
    ELSEIF iv_code BETWEEN 18 AND 19.
      rv_distance = go_stream->take_int( 8 ) + 513 + ( iv_code - 18 ) * 256.
    ELSEIF iv_code BETWEEN 20 AND 21.
      rv_distance = go_stream->take_int( 9 ) + 1025 + ( iv_code - 20 ) * 512.
    ELSEIF iv_code BETWEEN 22 AND 23.
      rv_distance = go_stream->take_int( 10 ) + 2049 + ( iv_code - 22 ) * 1024.
    ELSEIF iv_code BETWEEN 24 AND 25.
      rv_distance = go_stream->take_int( 11 ) + 4097 + ( iv_code - 24 ) * 2048.
    ELSEIF iv_code BETWEEN 26 AND 27.
      rv_distance = go_stream->take_int( 12 ) + 8193 + ( iv_code - 26 ) * 4096.
    ELSEIF iv_code BETWEEN 28 AND 29.
      rv_distance = go_stream->take_int( 13 ) + 16385 + ( iv_code - 28 ) * 8192.
    ELSE.
      ASSERT 1 = 0.
    ENDIF.

  ENDMETHOD.


  METHOD map_length.

    " RFC 1951 §3.2.5 length alphabet — condensed from 29 WHEN to 7 arithmetic groups.
    " Formula per group: rv_length = take_int(extra_bits) + base + (iv_code - grp_start) * stride
    " Verification spot-checks (matching original WHEN values):
    "   257 → 0+3+0=3   264 → 0+3+7=10
    "   265 → 1+11+0=11 268 → 1+11+6=17
    "   269 → 2+19+0=19 272 → 2+19+12=31
    "   273 → 3+35+0=35 276 → 3+35+24=59
    "   277 → 4+67+0=67 280 → 4+67+48=115
    "   281 → 5+131+0=131 284 → 5+131+96=227
    "   285 → 0+258=258

    IF iv_code BETWEEN 257 AND 264.
      rv_length = go_stream->take_int( 0 ) + 3 + iv_code - 257.
    ELSEIF iv_code BETWEEN 265 AND 268.
      rv_length = go_stream->take_int( 1 ) + 11 + ( iv_code - 265 ) * 2.
    ELSEIF iv_code BETWEEN 269 AND 272.
      rv_length = go_stream->take_int( 2 ) + 19 + ( iv_code - 269 ) * 4.
    ELSEIF iv_code BETWEEN 273 AND 276.
      rv_length = go_stream->take_int( 3 ) + 35 + ( iv_code - 273 ) * 8.
    ELSEIF iv_code BETWEEN 277 AND 280.
      rv_length = go_stream->take_int( 4 ) + 67 + ( iv_code - 277 ) * 16.
    ELSEIF iv_code BETWEEN 281 AND 284.
      rv_length = go_stream->take_int( 5 ) + 131 + ( iv_code - 281 ) * 32.
    ELSEIF iv_code = 285.
      rv_length = go_stream->take_int( 0 ) + 258.
    ELSE.
      ASSERT 1 = 0.
    ENDIF.

  ENDMETHOD.


  METHOD not_compressed.

    DATA: lv_len  TYPE i,
          lv_nlen TYPE i ##NEEDED.
    DATA lv_bytes TYPE xstring.

* skip any remaining bits in current partially processed byte
    go_stream->clear_bits( ).

    lv_len = go_stream->take_int( 16 ).
    lv_nlen = go_stream->take_int( 16 ).

    lv_bytes = go_stream->take_bytes( lv_len ).
    CONCATENATE gv_out lv_bytes INTO gv_out IN BYTE MODE.

  ENDMETHOD.


  METHOD read_pair.

    DATA: lv_symbol TYPE i.


    rs_pair-length = map_length( iv_length ).

    lv_symbol = decode( go_distcode ).
    rs_pair-distance = map_distance( lv_symbol ).

  ENDMETHOD.
ENDCLASS.
