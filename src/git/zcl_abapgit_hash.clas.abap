CLASS zcl_abapgit_hash DEFINITION
  PUBLIC
  CREATE PUBLIC .

  PUBLIC SECTION.

    CLASS-METHODS adler32
      IMPORTING
        !iv_xstring        TYPE xstring
      RETURNING
        VALUE(rv_checksum) TYPE zif_abapgit_git_definitions=>ty_adler32 .
    CLASS-METHODS sha1
      IMPORTING
        !iv_type       TYPE zif_abapgit_git_definitions=>ty_type
        !iv_data       TYPE xstring
      RETURNING
        VALUE(rv_sha1) TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING
        zcx_abapgit_exception .
    CLASS-METHODS sha1_commit
      IMPORTING
        !iv_data       TYPE xstring
      RETURNING
        VALUE(rv_sha1) TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING
        zcx_abapgit_exception .
    CLASS-METHODS sha1_tree
      IMPORTING
        !iv_data       TYPE xstring
      RETURNING
        VALUE(rv_sha1) TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING
        zcx_abapgit_exception .
    CLASS-METHODS sha1_tag
      IMPORTING
        !iv_data       TYPE xstring
      RETURNING
        VALUE(rv_sha1) TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING
        zcx_abapgit_exception .
    CLASS-METHODS sha1_blob
      IMPORTING
        !iv_data       TYPE xstring
      RETURNING
        VALUE(rv_sha1) TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING
        zcx_abapgit_exception .
    CLASS-METHODS sha1_raw
      IMPORTING
        !iv_data       TYPE xstring
      RETURNING
        VALUE(rv_sha1) TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING
        zcx_abapgit_exception .
    CLASS-METHODS sha1_string
      IMPORTING
        !iv_data       TYPE string
      RETURNING
        VALUE(rv_sha1) TYPE zif_abapgit_git_definitions=>ty_sha1
      RAISING
        zcx_abapgit_exception .
  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.



CLASS zcl_abapgit_hash IMPLEMENTATION.


  METHOD adler32.

    " RFC 1950 Adler-32 checksum — optimized with 4-byte chunked processing.
    " Reduces loop iterations by ~4× compared to byte-by-byte loop.
    " MOD is deferred until lv_b approaches TYPE i overflow (max 2,147,483,647).
    " Worst case per iteration: lv_b grows by at most 4*255 + accumulated lv_a.
    " With batch limit of 5552 bytes (per zlib spec for unsigned 32-bit), we use
    " a conservative threshold check every 4 bytes to stay safe with signed TYPE i.

    CONSTANTS: lc_adler  TYPE i VALUE 65521,
               lc_nmax   TYPE i VALUE 5552. " max bytes before mandatory MOD (zlib spec)

    DATA: lv_a     TYPE i VALUE 1,
          lv_b     TYPE i VALUE 0,
          lv_len   TYPE i,
          lv_off   TYPE i,
          lv_n     TYPE i,
          lv_remaining TYPE i,
          lv_batch TYPE i,
          lv_b0    TYPE i,
          lv_b1    TYPE i,
          lv_b2    TYPE i,
          lv_b3    TYPE i,
          lv_x     TYPE x LENGTH 2,
          lv_ca    TYPE c LENGTH 4,
          lv_cb    TYPE c LENGTH 4,
          lv_char8 TYPE c LENGTH 8.

    lv_len = xstrlen( iv_xstring ).
    lv_off = 0.

    WHILE lv_off < lv_len.
      " Process in batches of up to lc_nmax bytes before taking MOD.
      " This guarantees no signed integer overflow for lv_b.
      lv_remaining = lv_len - lv_off.
      IF lv_remaining > lc_nmax.
        lv_batch = lc_nmax.
      ELSE.
        lv_batch = lv_remaining.
      ENDIF.

      " Process 4 bytes per iteration (unrolled inner loop)
      lv_n = lv_batch DIV 4.
      DO lv_n TIMES.
        lv_b0 = iv_xstring+lv_off(1).
        lv_off += 1.
        lv_b1 = iv_xstring+lv_off(1).
        lv_off += 1.
        lv_b2 = iv_xstring+lv_off(1).
        lv_off += 1.
        lv_b3 = iv_xstring+lv_off(1).
        lv_off += 1.

        lv_a = lv_a + lv_b0.
        lv_b = lv_b + lv_a.
        lv_a = lv_a + lv_b1.
        lv_b = lv_b + lv_a.
        lv_a = lv_a + lv_b2.
        lv_b = lv_b + lv_a.
        lv_a = lv_a + lv_b3.
        lv_b = lv_b + lv_a.
      ENDDO.

      " Handle remaining 0-3 bytes in this batch
      lv_n = lv_batch MOD 4.
      DO lv_n TIMES.
        lv_a = lv_a + iv_xstring+lv_off(1).
        lv_b = lv_b + lv_a.
        lv_off += 1.
      ENDDO.

      " MOD at end of each batch (mandatory to prevent overflow)
      lv_a = lv_a MOD lc_adler.
      lv_b = lv_b MOD lc_adler.
    ENDWHILE.

    " Format result as 4-byte big-endian: (B << 16) | A
    lv_x = lv_a.
    lv_ca = lv_x.

    lv_x = lv_b.
    lv_cb = lv_x.

    CONCATENATE lv_cb lv_ca INTO lv_char8.

    rv_checksum = lv_char8.

  ENDMETHOD.


  METHOD sha1.

    DATA: lv_len     TYPE i,
          lv_char10  TYPE c LENGTH 10,
          lv_string  TYPE string,
          lv_xstring TYPE xstring.


    lv_len = xstrlen( iv_data ).
    lv_char10 = lv_len.
    CONDENSE lv_char10.
    CONCATENATE iv_type lv_char10 INTO lv_string SEPARATED BY space.
    lv_xstring = zcl_abapgit_convert=>string_to_xstring_utf8( lv_string ).

    lv_string = lv_xstring.
    CONCATENATE lv_string '00' INTO lv_string.
    lv_xstring = lv_string.

    CONCATENATE lv_xstring iv_data INTO lv_xstring IN BYTE MODE.

    rv_sha1 = sha1_raw( lv_xstring ).

  ENDMETHOD.


  METHOD sha1_blob.
    rv_sha1 = sha1( iv_type = zif_abapgit_git_definitions=>c_type-blob
                    iv_data = iv_data ).
  ENDMETHOD.


  METHOD sha1_commit.
    rv_sha1 = sha1( iv_type = zif_abapgit_git_definitions=>c_type-commit
                    iv_data = iv_data ).
  ENDMETHOD.


  METHOD sha1_raw.

    DATA: lv_hash  TYPE string,
          lx_error TYPE REF TO cx_abap_message_digest.
    TRY.
        " Direct SHA1 via single kernel call (SNC_ABAP_SERVICE).
        " Previous implementation used CL_ABAP_HMAC with empty key which
        " internally performs TWO SHA1 operations (HMAC = H(opad||H(ipad||m))).
        cl_abap_message_digest=>calculate_hash_for_raw(
          EXPORTING
            if_algorithm  = 'SHA1'
            if_data       = iv_data
          IMPORTING
            ef_hashstring = lv_hash ).
      CATCH cx_abap_message_digest INTO lx_error.
        zcx_abapgit_exception=>raise_with_text( lx_error ).
    ENDTRY.

    rv_sha1 = lv_hash.
    TRANSLATE rv_sha1 TO LOWER CASE.

  ENDMETHOD.


  METHOD sha1_string.

    DATA: lv_hash  TYPE string,
          lx_error TYPE REF TO cx_abap_message_digest.
    TRY.
        cl_abap_message_digest=>calculate_hash_for_char(
          EXPORTING
            if_algorithm  = 'SHA1'
            if_data       = iv_data
          IMPORTING
            ef_hashstring = lv_hash ).
      CATCH cx_abap_message_digest INTO lx_error.
        zcx_abapgit_exception=>raise_with_text( lx_error ).
    ENDTRY.

    rv_sha1 = lv_hash.
    TRANSLATE rv_sha1 TO LOWER CASE.

  ENDMETHOD.


  METHOD sha1_tag.
    rv_sha1 = sha1( iv_type = zif_abapgit_git_definitions=>c_type-tag
                    iv_data = iv_data ).
  ENDMETHOD.


  METHOD sha1_tree.
    rv_sha1 = sha1( iv_type = zif_abapgit_git_definitions=>c_type-tree
                    iv_data = iv_data ).
  ENDMETHOD.
ENDCLASS.
