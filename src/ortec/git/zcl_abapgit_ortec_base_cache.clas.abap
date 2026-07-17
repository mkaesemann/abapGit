! <p class="shorttext synchronized">ORTEC byte-budgeted LRU base-cache</p>
! Standalone cache for recently-used delta-base object bytes.
! Intended lifecycle: one instance per decode pass, reused for the duration of
! that pass and then cleared/discarded. A fresh instance is also acceptable if
! the caller wants a shorter-lived cache.
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
    TYPES ty_entries_tt TYPE STANDARD TABLE OF ty_entry WITH EMPTY KEY.

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
    APPEND ls_entry TO mt_entries.
    mv_total_bytes = mv_total_bytes + lv_size.
  ENDMETHOD.

  METHOD clear.
    CLEAR mt_entries.
    CLEAR mv_total_bytes.
  ENDMETHOD.

  METHOD find_entry.
    LOOP AT mt_entries INTO DATA(ls_entry) WHERE sha1 = iv_sha1.
      rv_index = sy-tabix.
      EXIT.
    ENDLOOP.
  ENDMETHOD.

  METHOD touch.
    DATA lv_index TYPE i.
    DATA ls_entry TYPE ty_entry.

    lv_index = find_entry( iv_sha1 ).
    IF lv_index > 0.
      READ TABLE mt_entries INDEX lv_index INTO ls_entry.
      IF sy-subrc = 0.
        DELETE mt_entries INDEX lv_index.
        APPEND ls_entry TO mt_entries.
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
