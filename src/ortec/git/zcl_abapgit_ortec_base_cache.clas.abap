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
             seq   TYPE i,
           END OF ty_entry.
    "! Secondary hashed key on sha1 gives find_entry O(1) lookup. Secondary
    "! sorted key on seq gives O(1) access to the least-recently-used entry
    "! (lowest seq = oldest) for eviction, WITHOUT needing LRU order to be
    "! reflected in the table's own physical/primary position - see the
    "! class-level doc comment for why that matters.
    TYPES ty_entries_tt TYPE STANDARD TABLE OF ty_entry WITH EMPTY KEY
      WITH UNIQUE HASHED KEY by_sha1 COMPONENTS sha1
      WITH UNIQUE SORTED KEY by_seq COMPONENTS seq.

    CLASS-DATA go_instance TYPE REF TO zcl_abapgit_ortec_base_cache.

    DATA mt_entries TYPE ty_entries_tt.
    DATA mv_total_bytes TYPE i.
    DATA mv_next_seq TYPE i.

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

    mv_next_seq = mv_next_seq + 1.

    lv_index = find_entry( iv_sha1 ).
    IF lv_index > 0.
      " Already cached (e.g. re-resolved, or re-requested as a shared base
      " by another delta) - update the EXISTING row in place via MODIFY
      " INDEX and bump its seq, instead of DELETE + re-INSERT/APPEND. This
      " is the actual fix for a live ITAB_DUPLICATE_KEY dump (2026-07-19,
      " recurring across several real pulls, including for ordinary
      " non-empty tree/blob objects, not just an edge case): under the full
      " churn of a real ~100k-object pack, this table's UNIQUE by_sha1
      " secondary key was repeatedly deleted-from and re-inserted-into for
      " the SAME hot/shared SHA1s (every LRU "touch" and every re-put moved
      " the row via DELETE+APPEND) - MODIFY INDEX never removes or
      " re-adds the row at all, so it can never trip the unique-key
      " insert-time check that DELETE+APPEND/INSERT eventually did. LRU
      " order no longer depends on physical table position at all (see
      " mv_next_seq / by_seq / remove_oldest).
      READ TABLE mt_entries INDEX lv_index INTO ls_entry.
      mv_total_bytes = mv_total_bytes - ls_entry-bytes + lv_size.
      ls_entry-data  = iv_data.
      ls_entry-bytes = lv_size.
      ls_entry-seq   = mv_next_seq.
      MODIFY mt_entries INDEX lv_index FROM ls_entry.
      RETURN.
    ENDIF.

    WHILE mv_total_bytes + lv_size > c_budget_bytes AND mt_entries IS NOT INITIAL.
      remove_oldest( ).
    ENDWHILE.

    ls_entry-sha1  = iv_sha1.
    ls_entry-data  = iv_data.
    ls_entry-bytes = lv_size.
    ls_entry-seq   = mv_next_seq.
    TRY.
        " INSERT INTO TABLE (not APPEND), inside TRY/CATCH, as a last-resort
        " safety net: find_entry( ) above already established iv_sha1 is
        " genuinely new, so this should never actually raise - but content
        " is addressed by SHA1 (the same key always means the same bytes),
        " so silently keeping whichever copy is already present on the
        " extremely unlikely chance this DOES fire is always safe, and
        " infinitely preferable to a dump.
        INSERT ls_entry INTO TABLE mt_entries.
        mv_total_bytes = mv_total_bytes + lv_size.
      CATCH cx_sy_itab_duplicate_key.
    ENDTRY.
  ENDMETHOD.

  METHOD clear.
    CLEAR mt_entries.
    CLEAR mv_total_bytes.
    CLEAR mv_next_seq.
  ENDMETHOD.

  METHOD find_entry.
    " O(1) via the by_sha1 secondary hashed key - SY-TABIX is reliably set to
    " the PRIMARY table index even when the row is found through a secondary
    " key (documented ABAP behavior), so callers can still use the returned
    " index for INDEX-based READ/MODIFY against the primary table.
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
      " MODIFY INDEX in place - see put( ) for why this replaced
      " DELETE + re-APPEND/INSERT (the actual fix for a live
      " ITAB_DUPLICATE_KEY dump, 2026-07-19).
      mv_next_seq = mv_next_seq + 1.
      ls_entry-seq = mv_next_seq.
      MODIFY mt_entries INDEX lv_index FROM ls_entry TRANSPORTING seq.
    ENDIF.
  ENDMETHOD.

  METHOD remove_oldest.
    DATA ls_entry TYPE ty_entry.

    " Lowest seq = least-recently-used, found in O(1) via the by_seq
    " secondary sorted key's own index ordering - independent of the
    " primary table's physical row order/position.
    READ TABLE mt_entries INDEX 1 USING KEY by_seq INTO ls_entry.
    IF sy-subrc = 0.
      mv_total_bytes = mv_total_bytes - ls_entry-bytes.
      DELETE mt_entries INDEX 1 USING KEY by_seq.
    ENDIF.
  ENDMETHOD.
ENDCLASS.
