"! <p class="shorttext synchronized">ORTEC byte-budgeted LRU base-cache</p>
"! Standalone cache for recently-used delta-base object bytes.
"! Intended lifecycle: one instance per decode pass, reused for the duration of
"! that pass and then cleared/discarded. A fresh instance is also acceptable if
"! the caller wants a shorter-lived cache.
CLASS zcl_abapgit_ortec_base_cache DEFINITION
  PUBLIC FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.
    TYPES ty_sha1 TYPE c LENGTH 40.

    CONSTANTS c_budget_bytes TYPE i VALUE 268435456.

    CLASS-METHODS get_instance
      RETURNING VALUE(ro_cache) TYPE REF TO zcl_abapgit_ortec_base_cache.

    METHODS get
      IMPORTING iv_sha1 TYPE ty_sha1
      RETURNING VALUE(rv_data) TYPE xstring.

    "! Returns whether iv_sha1 is currently cached - distinct from get( )'s
    "! result being initial, since a genuinely cached 0-byte object (a valid,
    "! real Git object - e.g. an empty blob) also returns an initial xstring
    "! from get( ). Callers that need to tell "cached but empty" apart from
    "! "not cached" (e.g. to avoid a redundant DB fetch + re-cache) must use
    "! this instead of checking get( )'s result for IS NOT INITIAL.
    METHODS has
      IMPORTING iv_sha1 TYPE ty_sha1
      RETURNING VALUE(rv_found) TYPE abap_bool.

    METHODS put
      IMPORTING iv_sha1 TYPE ty_sha1
                iv_data TYPE xstring.

    METHODS clear.

  PRIVATE SECTION.
    TYPES: BEGIN OF ty_entry,
             sha1  TYPE ty_sha1,
             data  TYPE xstring,
             bytes TYPE i,
           END OF ty_entry.
    "! Secondary hashed key on sha1 gives find_entry O(1) lookup while the
    "! primary STANDARD-table index order is still used for LRU eviction
    "! (index 1 = oldest, APPEND = newest, move-to-end = DELETE + APPEND).
    "! Without this, find_entry was an O(n) linear scan called on every
    "! get/put/touch - fine for a handful of test entries, but a genuine
    "! O(n^2) bottleneck once thousands of distinct bases are resolved in a
    "! single pass at real repo scale.
    TYPES ty_entries_tt TYPE STANDARD TABLE OF ty_entry WITH EMPTY KEY
      WITH UNIQUE HASHED KEY by_sha1 COMPONENTS sha1.

    CLASS-DATA go_instance TYPE REF TO zcl_abapgit_ortec_base_cache.

    DATA mt_entries TYPE ty_entries_tt.
    DATA mv_total_bytes TYPE i.

    METHODS find_entry
      IMPORTING iv_sha1 TYPE ty_sha1
      RETURNING VALUE(rv_index) TYPE i.

    METHODS touch
      IMPORTING iv_sha1 TYPE ty_sha1.

    METHODS remove_oldest.
ENDCLASS.


CLASS zcl_abapgit_ortec_base_cache IMPLEMENTATION.
  METHOD get_instance.
    IF go_instance IS INITIAL.
      CREATE OBJECT go_instance.
    ENDIF.

    ro_cache = go_instance.
  ENDMETHOD.

  METHOD get.
    DATA lv_index TYPE i.

    lv_index = find_entry( iv_sha1 ).
    IF lv_index > 0.
      READ TABLE mt_entries INDEX lv_index INTO DATA(ls_entry).
      IF sy-subrc = 0.
        rv_data = ls_entry-data.
        touch( iv_sha1 ).
      ENDIF.
    ENDIF.
  ENDMETHOD.

  METHOD has.
    rv_found = xsdbool( find_entry( iv_sha1 ) > 0 ).
  ENDMETHOD.

  METHOD put.
    DATA lv_index TYPE i.
    DATA lv_size TYPE i.
    DATA ls_entry TYPE ty_entry.

    lv_size = xstrlen( iv_data ).
    IF lv_size > c_budget_bytes.
      RETURN.
    ENDIF.

    lv_index = find_entry( iv_sha1 ).
    IF lv_index > 0.
      READ TABLE mt_entries INDEX lv_index INTO ls_entry.
      IF sy-subrc = 0.
        mv_total_bytes = mv_total_bytes - ls_entry-bytes.
        DELETE mt_entries INDEX lv_index.
      ENDIF.
    ENDIF.

    WHILE mv_total_bytes + lv_size > c_budget_bytes AND mt_entries IS NOT INITIAL.
      remove_oldest( ).
    ENDWHILE.

    ls_entry-sha1 = iv_sha1.
    ls_entry-data = iv_data.
    ls_entry-bytes = lv_size.
    " INSERT INTO TABLE (not APPEND): for a table with a UNIQUE secondary
    " key, APPEND raises the uncatchable runtime error ITAB_DUPLICATE_KEY if
    " a row with the same key is already present, whereas INSERT INTO TABLE
    " degrades gracefully to sy-subrc <> 0. The find_entry+DELETE above
    " should already have removed any pre-existing row for iv_sha1, but this
    " is the difference between a graceful no-op and a hard dump if that
    " invariant is ever violated. For a STANDARD table with EMPTY primary
    " key, INSERT INTO TABLE appends at the end exactly like APPEND, so LRU
    " ordering (index 1 = oldest) is unaffected.
    INSERT ls_entry INTO TABLE mt_entries.
    IF sy-subrc = 0.
      mv_total_bytes = mv_total_bytes + lv_size.
    ENDIF.
  ENDMETHOD.

  METHOD clear.
    CLEAR mt_entries.
    CLEAR mv_total_bytes.
  ENDMETHOD.

  METHOD find_entry.
    " O(1) via the by_sha1 secondary hashed key - SY-TABIX is reliably set to
    " the PRIMARY table index even when the row is found through a secondary
    " key (documented ABAP behavior), so callers can still use the returned
    " index for INDEX-based READ/DELETE against the primary (order-preserving)
    " table.
    READ TABLE mt_entries WITH TABLE KEY by_sha1 COMPONENTS sha1 = iv_sha1
      TRANSPORTING NO FIELDS.
    IF sy-subrc = 0.
      rv_index = sy-tabix.
    ENDIF.
  ENDMETHOD.

  METHOD touch.
    DATA lv_index TYPE i.
    DATA ls_entry TYPE ty_entry.

    lv_index = find_entry( iv_sha1 ).
    IF lv_index > 0.
      READ TABLE mt_entries INDEX lv_index INTO ls_entry.
      IF sy-subrc = 0.
        DELETE mt_entries INDEX lv_index.
        " See put( ) for why INSERT INTO TABLE is used instead of APPEND.
        INSERT ls_entry INTO TABLE mt_entries.
      ENDIF.
    ENDIF.
  ENDMETHOD.

  METHOD remove_oldest.
    DATA ls_entry TYPE ty_entry.

    READ TABLE mt_entries INDEX 1 INTO ls_entry.
    IF sy-subrc = 0.
      mv_total_bytes = mv_total_bytes - ls_entry-bytes.
      DELETE mt_entries INDEX 1.
    ENDIF.
  ENDMETHOD.
ENDCLASS.
