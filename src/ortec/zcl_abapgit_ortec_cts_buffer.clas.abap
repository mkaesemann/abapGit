"! <p class="shorttext synchronized">ORTEC CTS Transport Buffer</p>
"! Bulk-optimized transport determination for repository content lists.
"! Pre-reads TLOCK and E070/E071 in one pass and resolves transports in-memory.
"! Replaces the per-item get_transport_for_object calls (16K+ DB accesses)
"! with a single bulk read + in-memory matching (~2 DB accesses total).
CLASS zcl_abapgit_ortec_cts_buffer DEFINITION
  PUBLIC FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.
    "! Determine transports for all repository items in bulk.
    "! Replaces the per-item loop in zcl_abapgit_repo_content_list=>determine_transports.
    "! Uses pre-loaded TLOCK buffer and bulk E070/E071 query.
    "! @parameter ct_repo_items |
    "! Repository items - transport field is filled on return
    CLASS-METHODS determine_transports_bulk
      CHANGING ct_repo_items TYPE zif_abapgit_definitions=>ty_repo_item_tt.

  PRIVATE SECTION.
    TYPES:
      BEGIN OF ty_e070_e071_result,
        obj_type TYPE tadir-object,
        obj_name TYPE tadir-obj_name,
        trkorr   TYPE trkorr,
      END OF ty_e070_e071_result.
    TYPES ty_e070_e071_results TYPE SORTED TABLE OF ty_e070_e071_result
          WITH NON-UNIQUE KEY obj_type obj_name.

    "! Bulk query of open transports containing objects (for non-lockable types).
    "! @parameter it_items |
    "! Items to check
    "! @parameter rt_results |
    "! Transport assignments
    CLASS-METHODS bulk_transport_from_db
      IMPORTING it_items          TYPE zif_abapgit_definitions=>ty_items_tt
      RETURNING VALUE(rt_results) TYPE ty_e070_e071_results.

ENDCLASS.


CLASS zcl_abapgit_ortec_cts_buffer IMPLEMENTATION.

  METHOD determine_transports_bulk.
    " Replicates the logic of zif_abapgit_cts_api~get_transports_for_list but operates
    " directly on ty_repo_item_tt and also bulk-resolves non-lockable objects via E070/E071.

    DATA lt_tlock TYPE SORTED TABLE OF tlock WITH NON-UNIQUE KEY object hikey.
    DATA ls_object_key TYPE e071.
    DATA lv_type_check_result TYPE c LENGTH 1.
    DATA ls_lock_key TYPE tlock_int.
    DATA lv_request TYPE trkorr.
    DATA lt_non_lockable TYPE zif_abapgit_definitions=>ty_items_tt.
    DATA ls_item TYPE zif_abapgit_definitions=>ty_item.

    FIELD-SYMBOLS <ls_repo_item> LIKE LINE OF ct_repo_items.
    FIELD-SYMBOLS <ls_tlock> LIKE LINE OF lt_tlock.

    " STEP 1: Bulk-read all TLOCK entries (single DB access)
    SELECT * FROM tlock INTO TABLE lt_tlock.
    IF sy-subrc <> 0.
      " No locks at all - still check for DB transport lookup
      LOOP AT ct_repo_items ASSIGNING <ls_repo_item>
           WHERE obj_type IS NOT INITIAL AND obj_name IS NOT INITIAL.
        ls_object_key-pgmid    = 'R3TR'.
        ls_object_key-object   = <ls_repo_item>-obj_type.
        ls_object_key-obj_name = <ls_repo_item>-obj_name.
        CALL FUNCTION 'TR_CHECK_TYPE'
          EXPORTING
            wi_e071   = ls_object_key
          IMPORTING
            pe_result = lv_type_check_result.
        IF lv_type_check_result CA 'RT'.
          ls_item-obj_type = <ls_repo_item>-obj_type.
          ls_item-obj_name = <ls_repo_item>-obj_name.
          APPEND ls_item TO lt_non_lockable.
        ENDIF.
      ENDLOOP.
      IF lt_non_lockable IS NOT INITIAL.
        DATA(lt_db_results_empty) = bulk_transport_from_db( lt_non_lockable ).
        LOOP AT ct_repo_items ASSIGNING <ls_repo_item>
             WHERE obj_type IS NOT INITIAL AND obj_name IS NOT INITIAL.
          READ TABLE lt_db_results_empty INTO DATA(ls_db_empty)
               WITH TABLE KEY obj_type = <ls_repo_item>-obj_type
                              obj_name = <ls_repo_item>-obj_name.
          IF sy-subrc = 0.
            <ls_repo_item>-transport = ls_db_empty-trkorr.
          ENDIF.
        ENDLOOP.
      ENDIF.
      RETURN.
    ENDIF.

    " STEP 2: In-memory lock matching + collect non-lockable items for DB fallback
    LOOP AT ct_repo_items ASSIGNING <ls_repo_item>
         WHERE obj_type IS NOT INITIAL AND obj_name IS NOT INITIAL.

      CLEAR lv_request.
      ls_object_key-pgmid    = 'R3TR'.
      ls_object_key-object   = <ls_repo_item>-obj_type.
      ls_object_key-obj_name = <ls_repo_item>-obj_name.

      CALL FUNCTION 'TR_CHECK_TYPE'
        EXPORTING
          wi_e071     = ls_object_key
        IMPORTING
          we_lock_key = ls_lock_key
          pe_result   = lv_type_check_result.

      IF lv_type_check_result = 'L'.
        " Lockable object: match against pre-loaded TLOCK buffer
        LOOP AT lt_tlock ASSIGNING <ls_tlock>
            WHERE object = ls_lock_key-obj
            AND hikey >= ls_lock_key-low
            AND lokey <= ls_lock_key-hi.                      "#EC PORTABLE
          IF lv_request IS INITIAL.
            lv_request = <ls_tlock>-trkorr.
          ELSEIF lv_request <> <ls_tlock>-trkorr.
            lv_request = zif_abapgit_definitions=>c_multiple_transports.
            EXIT.
          ENDIF.
        ENDLOOP.
        <ls_repo_item>-transport = lv_request.
      ELSEIF lv_type_check_result CA 'RT'.
        " Transportable but not lockable: collect for bulk DB query
        ls_item-obj_type = <ls_repo_item>-obj_type.
        ls_item-obj_name = <ls_repo_item>-obj_name.
        APPEND ls_item TO lt_non_lockable.
      ENDIF.

    ENDLOOP.

    " STEP 3: Bulk DB lookup for non-lockable transportable objects
    IF lt_non_lockable IS NOT INITIAL.
      DATA(lt_db_results) = bulk_transport_from_db( lt_non_lockable ).
      LOOP AT ct_repo_items ASSIGNING <ls_repo_item>
           WHERE obj_type IS NOT INITIAL AND obj_name IS NOT INITIAL
             AND transport IS INITIAL.
        READ TABLE lt_db_results INTO DATA(ls_db_result)
             WITH TABLE KEY obj_type = <ls_repo_item>-obj_type
                            obj_name = <ls_repo_item>-obj_name.
        IF sy-subrc = 0.
          <ls_repo_item>-transport = ls_db_result-trkorr.
        ENDIF.
      ENDLOOP.
    ENDIF.

  ENDMETHOD.


  METHOD bulk_transport_from_db.
    " Bulk replacement for get_current_transport_from_db.
    " Reads all matching E070+E071 entries for the items in one query
    " using FOR ALL ENTRIES.

    DATA lt_e071_keys TYPE STANDARD TABLE OF e071 WITH DEFAULT KEY.
    DATA ls_key LIKE LINE OF lt_e071_keys.
    DATA ls_result TYPE ty_e070_e071_result.

    FIELD-SYMBOLS <ls_item> LIKE LINE OF it_items.

    IF it_items IS INITIAL.
      RETURN.
    ENDIF.

    " Build key table for FOR ALL ENTRIES
    LOOP AT it_items ASSIGNING <ls_item>.
      CLEAR ls_key.
      ls_key-pgmid    = 'R3TR'.
      ls_key-object   = <ls_item>-obj_type.
      ls_key-obj_name = <ls_item>-obj_name.
      APPEND ls_key TO lt_e071_keys.
    ENDLOOP.
    SORT lt_e071_keys BY pgmid object obj_name.
    DELETE ADJACENT DUPLICATES FROM lt_e071_keys COMPARING pgmid object obj_name.

    " Single bulk query: open transports containing these objects
    " Same WHERE conditions as get_current_transport_from_db (status D/L, not SAP piece list)
    SELECT b~object AS obj_type,
           b~obj_name AS obj_name,
           a~trkorr
      FROM e070 AS a
      JOIN e071 AS b ON a~trkorr = b~trkorr
      INTO TABLE @DATA(lt_raw)
      FOR ALL ENTRIES IN @lt_e071_keys
      WHERE ( a~trstatus = 'D' OR a~trstatus = 'L' )
        AND a~trfunction <> 'G'
        AND NOT ( a~trfunction = 'F' AND ( a~tarsystem = '' OR a~tarsystem = 'SAP' ) )
        AND b~pgmid    = @lt_e071_keys-pgmid
        AND b~object   = @lt_e071_keys-object
        AND b~obj_name = @lt_e071_keys-obj_name.

    " Deduplicate: keep first match per object (same as SELECT SINGLE behavior)
    SORT lt_raw BY obj_type obj_name trkorr.
    DELETE ADJACENT DUPLICATES FROM lt_raw COMPARING obj_type obj_name.

    LOOP AT lt_raw INTO DATA(ls_raw).
      CLEAR ls_result.
      ls_result-obj_type = ls_raw-obj_type.
      ls_result-obj_name = ls_raw-obj_name.
      ls_result-trkorr   = ls_raw-trkorr.
      INSERT ls_result INTO TABLE rt_results.
    ENDLOOP.

  ENDMETHOD.

ENDCLASS.
