CLASS ltcl_base_cache DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    METHODS setup.
    METHODS teardown.
    METHODS put_get_round_trip FOR TESTING RAISING cx_static_check.
    METHODS get_missing_returns_initial FOR TESTING RAISING cx_static_check.
    METHODS lru_eviction FOR TESTING RAISING cx_static_check.
    METHODS oversize_blob_is_not_cached FOR TESTING RAISING cx_static_check.
    METHODS clear_removes_entries FOR TESTING RAISING cx_static_check.
    METHODS zero_byte_blob_is_a_hit FOR TESTING RAISING cx_static_check.
    METHODS re_put_same_sha1_no_dump FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_base_cache IMPLEMENTATION.
  METHOD setup.
    zcl_abapgit_ortec_base_cache=>get_instance( )->clear( ).
  ENDMETHOD.

  METHOD teardown.
    zcl_abapgit_ortec_base_cache=>get_instance( )->clear( ).
  ENDMETHOD.

  METHOD put_get_round_trip.
    DATA lo_cache TYPE REF TO zcl_abapgit_ortec_base_cache.
    DATA lv_data TYPE xstring.
    CONSTANTS lc_sha1 TYPE c LENGTH 40 VALUE '1111111111111111111111111111111111111111'.

    lo_cache = zcl_abapgit_ortec_base_cache=>get_instance( ).
    lv_data = '48656C6C6F'.

    lo_cache->put( iv_sha1 = lc_sha1 iv_data = lv_data ).
    cl_abap_unit_assert=>assert_equals(
      act = lo_cache->get( iv_sha1 = lc_sha1 )
      exp = lv_data
      msg = 'A stored entry must round-trip through get' ).
  ENDMETHOD.

  METHOD get_missing_returns_initial.
    DATA lo_cache TYPE REF TO zcl_abapgit_ortec_base_cache.
    CONSTANTS lc_sha1 TYPE c LENGTH 40 VALUE '2222222222222222222222222222222222222222'.

    lo_cache = zcl_abapgit_ortec_base_cache=>get_instance( ).
    cl_abap_unit_assert=>assert_initial( act = lo_cache->get( iv_sha1 = lc_sha1 )
      msg = 'A missing SHA1 must return an initial xstring' ).
  ENDMETHOD.

  METHOD lru_eviction.
    DATA lo_cache TYPE REF TO zcl_abapgit_ortec_base_cache.
    DATA lv_a TYPE xstring.
    DATA lv_b TYPE xstring.
    DATA lv_c TYPE xstring.
    CONSTANTS lc_a TYPE c LENGTH 40 VALUE '3333333333333333333333333333333333333333'.
    CONSTANTS lc_b TYPE c LENGTH 40 VALUE '4444444444444444444444444444444444444444'.
    CONSTANTS lc_c TYPE c LENGTH 40 VALUE '5555555555555555555555555555555555555555'.
    CONSTANTS lc_bytes TYPE i VALUE 134217728.

    lo_cache = zcl_abapgit_ortec_base_cache=>get_instance( ).
    lv_a = zcl_abapgit_convert=>string_to_xstring_utf8( iv_string = repeat( val = 'A' occ = lc_bytes ) ).
    lv_b = zcl_abapgit_convert=>string_to_xstring_utf8( iv_string = repeat( val = 'B' occ = lc_bytes ) ).
    lv_c = zcl_abapgit_convert=>string_to_xstring_utf8( iv_string = repeat( val = 'C' occ = 20971520 ) ).

    lo_cache->put( iv_sha1 = lc_a iv_data = lv_a ).
    lo_cache->put( iv_sha1 = lc_b iv_data = lv_b ).
    cl_abap_unit_assert=>assert_equals( act = lo_cache->get( iv_sha1 = lc_a ) exp = lv_a
      msg = 'Touching A must mark it as recently used' ).
    lo_cache->put( iv_sha1 = lc_c iv_data = lv_c ).

    cl_abap_unit_assert=>assert_initial( act = lo_cache->get( iv_sha1 = lc_b )
      msg = 'The untouched B entry must be evicted before the touched A entry' ).
    cl_abap_unit_assert=>assert_equals( act = lo_cache->get( iv_sha1 = lc_a ) exp = lv_a
      msg = 'The touched A entry must survive eviction' ).
    cl_abap_unit_assert=>assert_equals( act = lo_cache->get( iv_sha1 = lc_c ) exp = lv_c
      msg = 'The newly added C entry must be retained' ).
  ENDMETHOD.

  METHOD oversize_blob_is_not_cached.
    DATA lo_cache TYPE REF TO zcl_abapgit_ortec_base_cache.
    DATA lv_blob TYPE xstring.
    DATA lv_string TYPE string.
    CONSTANTS lc_sha1 TYPE c LENGTH 40 VALUE '6666666666666666666666666666666666666666'.
    CONSTANTS lc_budget TYPE i VALUE 268435456.

    lo_cache = zcl_abapgit_ortec_base_cache=>get_instance( ).
    lv_string = repeat( val = 'D' occ = lc_budget + 1 ).
    lv_blob = zcl_abapgit_convert=>string_to_xstring_utf8( iv_string = lv_string ).

    lo_cache->put( iv_sha1 = lc_sha1 iv_data = lv_blob ).
    cl_abap_unit_assert=>assert_initial( act = lo_cache->get( iv_sha1 = lc_sha1 )
      msg = 'An oversize object must not be admitted to the cache' ).

    " The oversize admission must not clear or evict unrelated entries.
    lo_cache->put( iv_sha1 = '7777777777777777777777777777777777777777' iv_data = '48656C6C6F' ).
    cl_abap_unit_assert=>assert_equals( act = lo_cache->get( iv_sha1 = '7777777777777777777777777777777777777777' ) exp = '48656C6C6F'
      msg = 'Oversize admission must not disturb already-cached entries' ).
  ENDMETHOD.

  METHOD clear_removes_entries.
    DATA lo_cache TYPE REF TO zcl_abapgit_ortec_base_cache.
    CONSTANTS lc_sha1 TYPE c LENGTH 40 VALUE '8888888888888888888888888888888888888888'.

    lo_cache = zcl_abapgit_ortec_base_cache=>get_instance( ).
    lo_cache->put( iv_sha1 = lc_sha1 iv_data = '48656C6C6F' ).
    lo_cache->clear( ).
    cl_abap_unit_assert=>assert_initial( act = lo_cache->get( iv_sha1 = lc_sha1 )
      msg = 'clear( ) must make a previously cached entry disappear' ).
  ENDMETHOD.

  METHOD zero_byte_blob_is_a_hit.
    " A real, legitimate 0-byte Git object (e.g. an empty blob) must be
    " distinguishable from "not cached" - get( ) alone returns an initial
    " xstring in BOTH cases, so callers needing to tell them apart (like
    " get_base_bytes) must use has( ).
    DATA lo_cache TYPE REF TO zcl_abapgit_ortec_base_cache.
    CONSTANTS lc_sha1 TYPE c LENGTH 40 VALUE '9999999999999999999999999999999999999999'.

    lo_cache = zcl_abapgit_ortec_base_cache=>get_instance( ).
    cl_abap_unit_assert=>assert_false( act = lo_cache->has( iv_sha1 = lc_sha1 )
      msg = 'A never-put SHA1 must not be reported as cached' ).

    lo_cache->put( iv_sha1 = lc_sha1 iv_data = value xstring( ) ).
    cl_abap_unit_assert=>assert_true( act = lo_cache->has( iv_sha1 = lc_sha1 )
      msg = 'A 0-byte object that was put( ) must be reported as cached' ).
    cl_abap_unit_assert=>assert_initial( act = lo_cache->get( iv_sha1 = lc_sha1 )
      msg = 'get( ) on a 0-byte cached object still returns an initial xstring' ).
  ENDMETHOD.

  METHOD re_put_same_sha1_no_dump.
    " put( ) for a SHA1 already in the cache must overwrite in place, not
    " raise ITAB_DUPLICATE_KEY (mt_entries has a UNIQUE secondary key on
    " sha1 - this guards against a regression back to APPEND).
    DATA lo_cache TYPE REF TO zcl_abapgit_ortec_base_cache.
    CONSTANTS lc_sha1 TYPE c LENGTH 40 VALUE 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'.

    lo_cache = zcl_abapgit_ortec_base_cache=>get_instance( ).
    lo_cache->put( iv_sha1 = lc_sha1 iv_data = value xstring( ) ).
    lo_cache->put( iv_sha1 = lc_sha1 iv_data = value xstring( ) ).
    lo_cache->put( iv_sha1 = lc_sha1 iv_data = '48656C6C6F' ).
    cl_abap_unit_assert=>assert_equals( act = lo_cache->get( iv_sha1 = lc_sha1 ) exp = '48656C6C6F'
      msg = 'Repeated put( ) for the same SHA1 must overwrite, not dump or duplicate' ).
  ENDMETHOD.
ENDCLASS.
