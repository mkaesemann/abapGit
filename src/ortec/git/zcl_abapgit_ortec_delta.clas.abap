"! <p class="shorttext synchronized">ORTEC Unified Delta Resolver (REF + OFS)</p>
"! Resolves both OBJ_REF_DELTA and OBJ_OFS_DELTA pack entries into fully decoded
"! objects, in dependency order (a delta whose base is itself an unresolved
"! delta is resolved first). This is the Ortec-only counterpart to standard
"! <em>zcl_abapgit_git_delta=>decode_deltas</em>, which only understands
"! OBJ_REF_DELTA and resolves in pack-index order. Lives exclusively on the
"! Ortec-guarded decode path (D1 Option B): standard
"! <em>zcl_abapgit_git_pack</em>/<em>zcl_abapgit_git_delta</em> are untouched and
"! never need OFS support, because ofs-delta/thin-pack are only ever negotiated
"! on the Ortec fastpath's own upload-pack request.
"! See .memory/logs/target_design_phase5.md for the full design.
CLASS zcl_abapgit_ortec_delta DEFINITION
  PUBLIC
  FINAL
  CREATE PRIVATE.

  PUBLIC SECTION.
    "! Git pack object type 6 (OBJ_OFS_DELTA). Kept Ortec-local (not added to the
    "! shared zif_abapgit_git_definitions=>c_type) per design decision D-P5-1:
    "! this type is only ever produced on the Ortec-guarded decode path, so
    "! adding it to the standard, widely-referenced type interface would be an
    "! unnecessary standard-code touch.
    CONSTANTS c_type_ofs_d TYPE zif_abapgit_git_definitions=>ty_type VALUE 'ofs_d'.

    "! Maximum delta-chain depth resolved before raising (decision D-P5-6).
    "! Git's own default pack.depth is 50; this is a safety margin against a
    "! malformed/hostile pack presenting an excessively long or cyclic chain.
    "! Never unbounded recursion.
    CONSTANTS c_max_chain_depth TYPE i VALUE 64.

    "! One OFS_DELTA entry's resolved base position, collected during the single
    "! decode pass (the raw negative offset is consumed and resolved to an
    "! absolute pack byte offset immediately, since that is all get_offset needs
    "! - see zcl_abapgit_ortec_pack_dec=>resumable_decode).
    TYPES: BEGIN OF ty_ofs_meta,
             "! Matches zif_abapgit_definitions=>ty_object-index (the object's
             "! 1-based position in this pack, "uindex" in the decoder).
             obj_index   TYPE i,
             "! Absolute byte offset of this entry's base object's own header,
             "! i.e. (this entry's own header offset) - (decoded negative offset).
             base_offset TYPE i,
           END OF ty_ofs_meta.
    TYPES ty_ofs_meta_tt TYPE STANDARD TABLE OF ty_ofs_meta WITH EMPTY KEY
      WITH UNIQUE SORTED KEY by_index COMPONENTS obj_index.

    "! One row per object parsed from the pack (every type, not just deltas),
    "! correlating its own pack byte offset with its object index - the map
    "! OFS_DELTA base-offset lookups resolve against. Built by the caller
    "! during the single parse pass (it already computes each object's offset
    "! for other reasons), not by this class.
    TYPES: BEGIN OF ty_offset_entry,
             pack_offset TYPE i,
             obj_index   TYPE i,
           END OF ty_offset_entry.
    TYPES ty_offset_map_tt TYPE STANDARD TABLE OF ty_offset_entry WITH EMPTY KEY
      WITH UNIQUE SORTED KEY by_offset COMPONENTS pack_offset.

    "! Decode the git pack "offset encoding" used exclusively by OBJ_OFS_DELTA
    "! entries: base-128, MSB-first continuation, with a +1 bias added to the
    "! accumulator before each continuation shift. This is a DIFFERENT encoding
    "! from the object-size varint (zcl_abapgit_ortec_pack_dec=>get_length) -
    "! see the official Git pack-format.txt "offset encoding" and
    "! .memory/logs/target_design_phase5.md §1.2. Consumes bytes from cv_data
    "! (advances it past the encoded value), mirroring the existing
    "! get_length/get_type xstring-slicing convention.
    "! @parameter cv_data |
    "! Remaining pack bytes, positioned at the first byte of the ofs varint.
    "! Advanced past the consumed varint on return.
    "! @parameter rv_offset |
    "! The decoded value: the byte distance backwards from this entry's own
    "! header start to its base object's header start.
    "! @raising zcx_abapgit_exception |
    "! If the varint is truncated (a continuation byte was expected but no
    "! data remains).
    CLASS-METHODS get_offset
      CHANGING cv_data           TYPE xstring
      RETURNING VALUE(rv_offset) TYPE i
      RAISING   zcx_abapgit_exception.

    "! Apply a single git delta instruction stream to a base object's raw bytes.
    "! Ortec-owned copy of the byte-level copy/insert algorithm already used for
    "! REF_DELTA by standard zcl_abapgit_git_delta (decision D-P5-3: kept as a
    "! separate copy to avoid refactoring the standard class; pinned against
    "! identical exact vectors in the unit tests so drift is caught).
    "! @parameter iv_base |
    "! The base object's fully-resolved raw decoded bytes.
    "! @parameter iv_delta |
    "! The raw delta instruction bytes, i.e. exactly the DEFLATE-decompressed
    "! payload of a REF_DELTA/OFS_DELTA pack entry (zlib header and trailing
    "! adler32 already stripped by the caller, same as every other object).
    "! @parameter rv_result |
    "! The reconstructed object bytes.
    "! @raising zcx_abapgit_exception |
    "! On a malformed/truncated delta instruction stream.
    CLASS-METHODS apply
      IMPORTING iv_base          TYPE xstring
                iv_delta         TYPE xstring
      RETURNING VALUE(rv_result) TYPE xstring
      RAISING   zcx_abapgit_exception.

    "! Resolve every REF_DELTA and OFS_DELTA entry in ct_objects into a fully
    "! decoded object with its real content SHA1, in dependency order (a delta
    "! whose base is itself an unresolved delta - a "chain" - is resolved
    "! before the entry that depends on it; a chain longer than
    "! c_max_chain_depth raises rather than recursing unboundedly). This is
    "! the Ortec decode path's replacement for
    "! zcl_abapgit_git_delta=>decode_deltas, which only understands REF_DELTA
    "! and resolves in pack-index order (not dependency order).
    "! @parameter it_offset_map |
    "! Pack-offset -> object-index correlation for every object in ct_objects
    "! (see ty_offset_entry), used to resolve OFS_DELTA base_offset lookups.
    "! @parameter it_ofs_meta |
    "! One row per OFS_DELTA entry in ct_objects giving its resolved
    "! base_offset (see ty_ofs_meta). Entries not present in this table are
    "! assumed to be REF_DELTA or already-resolved objects.
    "! @parameter iv_repo_key |
    "! Repository key, used to resolve a thin base (referenced by a REF_DELTA
    "! SHA1 but absent from ct_objects) from the persistent object store -
    "! mirrors the existing Ortec fallback already present in standard
    "! zcl_abapgit_git_delta=>delta().
    "! @parameter ct_objects |
    "! On entry: full objects as-is; REF_DELTA entries with sha1 = base SHA1
    "! and data = raw delta bytes; OFS_DELTA entries (type = c_type_ofs_d)
    "! with data = raw delta bytes and their base recorded in it_ofs_meta (NOT
    "! in this table - ty_object has no offset field). On exit: every delta
    "! entry is replaced in place by its resolved object (real sha1, type
    "! inherited from its ultimate base, real data) - the same contract as
    "! decode_deltas's CHANGING table.
    "! @raising zcx_abapgit_exception |
    "! On an unresolvable base (missing from both ct_objects and the object
    "! store), a chain deeper than c_max_chain_depth, an out-of-range/invalid
    "! OFS base_offset, or a delta stream that fails to apply. Callers must
    "! treat this as "the whole decode failed" and never partially trust
    "! ct_objects afterwards.
    CLASS-METHODS resolve_all
      IMPORTING it_offset_map TYPE ty_offset_map_tt
                it_ofs_meta   TYPE ty_ofs_meta_tt
                iv_repo_key   TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
      CHANGING  ct_objects    TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_exception.

    "! Bulk-loads every genuinely external delta base in it_sha1s in ONE
    "! zcl_abapgit_ortec_obj_store=>get_objects(iv_bulk_fetch = abap_true)
    "! call, deduplicating first - never a per-base fetch. Shared by both
    "! resolve_all's phase 1.5 (after its in-pack fixpoint converges) and
    "! zcl_abapgit_ortec_pack_stream=>resolve_streaming's own phase 1.5, so
    "! a pack's external-base cost is bounded by K (this pack's own distinct
    "! external bases), never by repository size (Package D design §4/§5.1).
    "! get_objects itself already raises if ANY requested SHA1 is missing -
    "! this method adds two further checks get_objects does not perform:
    "! a loaded base must not itself be an unresolved delta (REF_DELTA/
    "! OFS_DELTA - the object store only ever stores fully-resolved content,
    "! so this would indicate store corruption), and its content must
    "! recompute to the SHA1 it was requested under.
    "! @parameter it_sha1s |
    "! Declared base SHA1s to load - duplicates and blank entries are
    "! tolerated and ignored.
    "! @parameter rt_objects |
    "! One row per unique, non-blank requested SHA1. Empty if it_sha1s has
    "! no non-blank entries (no call is made to the object store in that
    "! case).
    "! @raising zcx_abapgit_exception |
    "! If any requested base is missing, is itself a delta type, or fails
    "! content-hash verification.
    CLASS-METHODS bulk_resolve_external_bases
      IMPORTING iv_repo_key       TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                it_sha1s          TYPE zif_abapgit_git_definitions=>ty_sha1_tt
      RETURNING VALUE(rt_objects) TYPE zif_abapgit_definitions=>ty_objects_tt
      RAISING   zcx_abapgit_exception.

  PRIVATE SECTION.
    "! Approved bounded observability counter (Package D1 diff-audit
    "! disposition, .memory/logs/regression_variant_b_package_d_d1.md):
    "! counts calls to bulk_resolve_external_bases, i.e. how many bulk
    "! get_objects round trips this pack's delta resolution performed.
    "! Lifecycle: private CLASS-DATA, incremented exactly once per
    "! bulk_resolve_external_bases invocation, at most once per resolve_all
    "! call (Phase 1.5 calls it at most once). Reset: only ever reset by the
    "! LOCAL FRIEND test class's setup (see
    "! zcl_abapgit_ortec_delta.clas.testclasses.abap) - production code
    "! never resets or reads it, so it simply accumulates for the life of
    "! the ABAP session/work process; this is safe because no productive
    "! behavior ever branches on its value. Concurrency: CLASS-DATA is
    "! session/work-process-local, not shared across users - never used for
    "! cross-request coordination. Production semantics: a plain integer
    "! increment, never persisted, never logged, never exposed via any
    "! public API - purely a call-shape verification seam for D1's
    "! bulk_base_one_call_only/no_sql_in_pack_phase tests (Package D design
    "! §16). A pure test-seam alternative (spying on the call without any
    "! productive field) was evaluated and rejected: get_objects/get_object
    "! are static object-store methods with no injection point, and the
    "! object store's own session cache means a DB-deletion trap cannot
    "! reliably distinguish "the bulk call ran" from "a second call ran" -
    "! see the diff-audit note in the D1 handoff for the full analysis.
    CLASS-DATA gv_bulk_load_calls  TYPE i.
    "! Approved bounded observability counter, same disposition as
    "! gv_bulk_load_calls above: counts calls to resolve_one's on-demand
    "! per-object thin-fetch fallback, which Phase 1.5's bulk load is
    "! designed to make unreachable in the normal path. Lifecycle/reset/
    "! concurrency/production semantics are identical to gv_bulk_load_calls
    "! (private, test-reset only, session-local, never read productively).
    "! Kept at zero in the normal path is the exact invariant D1's
    "! no_sql_in_pack_phase test asserts.
    CLASS-DATA gv_thin_fetch_calls TYPE i.
    "! Maps an object's stable pack "index" (ty_object-index) to its current
    "! PRIMARY table index in ct_objects. Built once, up front, in resolve_all
    "! (a plain, non-keyed LOOP AT, where sy-tabix IS the correct primary
    "! index), and extended incrementally whenever a thin base is appended
    "! during resolution. Exists so every base lookup by "index" is an O(1)
    "! hashed read instead of a linear scan over ct_objects (which has no
    "! secondary key on "index") - resolve_one runs once per delta object, so
    "! a per-call linear scan would risk O(n^2) total cost on large packs.
    TYPES: BEGIN OF ty_tabix_by_index,
             obj_index TYPE i,
             tabix     TYPE i,
           END OF ty_tabix_by_index.
    TYPES ty_tabix_by_index_tt TYPE HASHED TABLE OF ty_tabix_by_index WITH UNIQUE KEY obj_index.

    CLASS-METHODS resolve_one
      IMPORTING iv_tabix            TYPE i
                iv_depth            TYPE i
                it_offset_map       TYPE ty_offset_map_tt
                it_ofs_meta         TYPE ty_ofs_meta_tt
                iv_repo_key         TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key
                "! When abap_false, a REF_DELTA base not yet reconstructable
                "! from ct_objects alone is left unresolved (RETURN, no
                "! object-store fetch, no raise) instead of being treated as
                "! authoritative. Used by resolve_all's phase-1 sweeps: a
                "! single ascending pass cannot resolve a REF_DELTA chained
                "! onto ANOTHER unresolved REF_DELTA positioned later in the
                "! pack (REF_DELTA carries no ordering guarantee), so
                "! multiple in-pack-only sweeps run first: each sweep can
                "! only newly resolve entries whose base was resolved by a
                "! PRIOR sweep, so repeating them converges on any chain
                "! regardless of topological order without repeatedly
                "! hitting the object store for the same still-unresolved
                "! object. Defaults to abap_true (fetch/raise immediately)
                "! for the final pass and every existing/recursive caller.
                iv_allow_thin_fetch TYPE abap_bool DEFAULT abap_true
      CHANGING  ct_objects        TYPE zif_abapgit_definitions=>ty_objects_tt
                ct_tabix_by_index TYPE ty_tabix_by_index_tt
      RAISING   zcx_abapgit_exception.

    "! Skip the two leading size varints (base size, result size) that every
    "! git delta instruction stream begins with - neither value is needed by
    "! the copy/insert algorithm itself (identical semantics to standard
    "! zcl_abapgit_git_delta's private delta_header, duplicated per D-P5-3).
    CLASS-METHODS skip_size_header
      CHANGING cv_data TYPE xstring.
ENDCLASS.


CLASS zcl_abapgit_ortec_delta IMPLEMENTATION.

  METHOD get_offset.

    CONSTANTS lc_msb  TYPE x LENGTH 1 VALUE '80'.
    CONSTANTS lc_low7 TYPE x LENGTH 1 VALUE '7F'.
    CONSTANTS lc_zero TYPE x LENGTH 1 VALUE '00'.

    DATA lv_byte TYPE x LENGTH 1.
    DATA lv_low  TYPE x LENGTH 1.
    DATA lv_int  TYPE i.

    TRY.
        IF xstrlen( cv_data ) = 0.
          zcx_abapgit_exception=>raise( |OFS varint truncated| ).
        ENDIF.

        lv_byte   = cv_data(1).
        cv_data   = cv_data+1.
        lv_low    = lv_byte BIT-AND lc_low7.
        rv_offset = lv_low.

        WHILE lv_byte BIT-AND lc_msb <> lc_zero.
          IF xstrlen( cv_data ) = 0.
            zcx_abapgit_exception=>raise( |OFS varint truncated| ).
          ENDIF.
          lv_byte   = cv_data(1).
          cv_data   = cv_data+1.
          lv_low    = lv_byte BIT-AND lc_low7.
          lv_int    = lv_low.
          rv_offset = ( rv_offset + 1 ) * 128 + lv_int.  " ((off+1) << 7) | payload
        ENDWHILE.
      CATCH cx_sy_range_out_of_bounds INTO DATA(lx_range_offset).
        zcx_abapgit_exception=>raise( |OFS varint decode error: { lx_range_offset->get_text( ) }| ).
    ENDTRY.

  ENDMETHOD.


  METHOD skip_size_header.
    CONSTANTS lc_msb  TYPE x LENGTH 1 VALUE '80'.
    CONSTANTS lc_zero TYPE x LENGTH 1 VALUE '00'.

    DATA lv_byte TYPE x LENGTH 1.

    TRY.
        DO 2 TIMES.
          DO.
            IF xstrlen( cv_data ) = 0.
              zcx_abapgit_exception=>raise( |Delta header truncated| ).
            ENDIF.
            lv_byte = cv_data(1).
            cv_data = cv_data+1.
            IF lv_byte BIT-AND lc_msb = lc_zero.
              EXIT.
            ENDIF.
          ENDDO.
        ENDDO.
      CATCH cx_sy_range_out_of_bounds INTO DATA(lx_range_header).
        zcx_abapgit_exception=>raise( |Delta header decode error: { lx_range_header->get_text( ) }| ).
    ENDTRY.
  ENDMETHOD.


  METHOD apply.

    CONSTANTS lc_msb  TYPE x LENGTH 1 VALUE '80'.
    CONSTANTS lc_zero TYPE x LENGTH 1 VALUE '00'.
    CONSTANTS lc_1  TYPE x LENGTH 1 VALUE '01'.
    CONSTANTS lc_2  TYPE x LENGTH 1 VALUE '02'.
    CONSTANTS lc_4  TYPE x LENGTH 1 VALUE '04'.
    CONSTANTS lc_8  TYPE x LENGTH 1 VALUE '08'.
    CONSTANTS lc_16 TYPE x LENGTH 1 VALUE '10'.
    CONSTANTS lc_32 TYPE x LENGTH 1 VALUE '20'.
    CONSTANTS lc_64 TYPE x LENGTH 1 VALUE '40'.

    DATA lv_data       TYPE xstring.
    DATA lv_instr      TYPE x LENGTH 1.
    DATA lv_byte       TYPE x LENGTH 1.
    DATA lv_offset     TYPE i.
    DATA lv_length     TYPE i.
    DATA lv_insert_len TYPE i.

    lv_data = iv_delta.
    skip_size_header( CHANGING cv_data = lv_data ).

    TRY.
        WHILE xstrlen( lv_data ) > 0.
          lv_instr = lv_data(1).
          lv_data  = lv_data+1.

          IF lv_instr BIT-AND lc_msb = lc_msb.
            " Copy instruction: offset/length sub-bytes are present only where the
            " corresponding flag bit is set; a missing byte defaults to 0 for that
            " position. A decoded length of 0 means 0x10000 (65536) - a documented
            " quirk of the git delta format, not a special case invented here.
            lv_offset = 0.
            IF lv_instr BIT-AND lc_1 = lc_1.
              IF xstrlen( lv_data ) = 0. zcx_abapgit_exception=>raise( |Delta stream truncated| ). ENDIF.
              lv_byte = lv_data(1). lv_data = lv_data+1.
              lv_offset = lv_byte.
            ENDIF.
            IF lv_instr BIT-AND lc_2 = lc_2.
              IF xstrlen( lv_data ) = 0. zcx_abapgit_exception=>raise( |Delta stream truncated| ). ENDIF.
              lv_byte = lv_data(1). lv_data = lv_data+1.
              lv_offset = lv_offset + lv_byte * 256.
            ENDIF.
            IF lv_instr BIT-AND lc_4 = lc_4.
              IF xstrlen( lv_data ) = 0. zcx_abapgit_exception=>raise( |Delta stream truncated| ). ENDIF.
              lv_byte = lv_data(1). lv_data = lv_data+1.
              lv_offset = lv_offset + lv_byte * 65536.
            ENDIF.
            IF lv_instr BIT-AND lc_8 = lc_8.
              IF xstrlen( lv_data ) = 0. zcx_abapgit_exception=>raise( |Delta stream truncated| ). ENDIF.
              lv_byte = lv_data(1). lv_data = lv_data+1.
              lv_offset = lv_offset + lv_byte * 16777216.
            ENDIF.

            lv_length = 0.
            IF lv_instr BIT-AND lc_16 = lc_16.
              IF xstrlen( lv_data ) = 0. zcx_abapgit_exception=>raise( |Delta stream truncated| ). ENDIF.
              lv_byte = lv_data(1). lv_data = lv_data+1.
              lv_length = lv_byte.
            ENDIF.
            IF lv_instr BIT-AND lc_32 = lc_32.
              IF xstrlen( lv_data ) = 0. zcx_abapgit_exception=>raise( |Delta stream truncated| ). ENDIF.
              lv_byte = lv_data(1). lv_data = lv_data+1.
              lv_length = lv_length + lv_byte * 256.
            ENDIF.
            IF lv_instr BIT-AND lc_64 = lc_64.
              IF xstrlen( lv_data ) = 0. zcx_abapgit_exception=>raise( |Delta stream truncated| ). ENDIF.
              lv_byte = lv_data(1). lv_data = lv_data+1.
              lv_length = lv_length + lv_byte * 65536.
            ENDIF.
            IF lv_length = 0.
              lv_length = 65536.
            ENDIF.

            IF lv_offset + lv_length > xstrlen( iv_base ).
              zcx_abapgit_exception=>raise(
                |Delta copy instruction exceeds base length (offset { lv_offset }, | &&
                |length { lv_length }, base length { xstrlen( iv_base ) })| ).
            ENDIF.

            CONCATENATE rv_result iv_base+lv_offset(lv_length) INTO rv_result IN BYTE MODE.

          ELSEIF lv_instr = lc_zero.
            " 0x00 is reserved (never a valid instruction) per the git delta format.
            zcx_abapgit_exception=>raise( |Reserved delta instruction 0x00| ).
          ELSE.
            " Insert instruction: the low 7 bits ARE the literal length directly.
            lv_insert_len = lv_instr.
            IF lv_insert_len > xstrlen( lv_data ).
              zcx_abapgit_exception=>raise( |Delta insert instruction exceeds stream length| ).
            ENDIF.
            CONCATENATE rv_result lv_data(lv_insert_len) INTO rv_result IN BYTE MODE.
            lv_data = lv_data+lv_insert_len.
          ENDIF.
        ENDWHILE.
      CATCH cx_sy_range_out_of_bounds INTO DATA(lx_range_apply).
        zcx_abapgit_exception=>raise( |Delta apply decode error: { lx_range_apply->get_text( ) }| ).
    ENDTRY.

  ENDMETHOD.


  METHOD resolve_all.
    DATA lv_tabix          TYPE i.
    DATA lt_tabix_by_index TYPE ty_tabix_by_index_tt.
    DATA lv_pass_progress  TYPE abap_bool.
    DATA lt_external_bases TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_loaded_bases   TYPE zif_abapgit_definitions=>ty_objects_tt.

    FIELD-SYMBOLS <ls_object_init> TYPE zif_abapgit_definitions=>ty_object.
    FIELD-SYMBOLS <ls_sweep>       TYPE zif_abapgit_definitions=>ty_object.
    FIELD-SYMBOLS <ls_unresolved>  TYPE zif_abapgit_definitions=>ty_object.
    FIELD-SYMBOLS <ls_loaded_base> TYPE zif_abapgit_definitions=>ty_object.

    " Plain (non-keyed) loop over the primary table - sy-tabix here IS the
    " correct primary index. See ty_tabix_by_index for why this exists.
    LOOP AT ct_objects ASSIGNING <ls_object_init>.
      INSERT VALUE #( obj_index = <ls_object_init>-index tabix = sy-tabix )
        INTO TABLE lt_tabix_by_index.
    ENDLOOP.

    " Phase 1: repeat ascending sweeps, resolving only bases already
    " reconstructable purely in-pack (iv_allow_thin_fetch = abap_false - no
    " object-store round-trip, no raise on "not found yet"). A single
    " ascending pass cannot resolve a REF_DELTA chained onto ANOTHER
    " unresolved REF_DELTA positioned LATER in the pack (REF_DELTA carries
    " no ordering guarantee, and delta-on-delta chains are common in real
    " packs) - each sweep can only newly resolve entries whose base was
    " resolved by a PRIOR sweep, so repeating full sweeps until one makes no
    " further progress converges on any such chain regardless of
    " topological order. Bounded by the pack's actual maximum delta-chain
    " depth (c_max_chain_depth <= 64), not by object count, since real
    " chains are always shallow - never O(n^2) in practice.
    DO.
      lv_pass_progress = abap_false.
      lv_tabix = 0.
      WHILE lv_tabix < lines( ct_objects ).
        lv_tabix = lv_tabix + 1.
        READ TABLE ct_objects ASSIGNING <ls_sweep> INDEX lv_tabix.
        IF sy-subrc <> 0.
          CONTINUE.
        ENDIF.
        IF <ls_sweep>-type <> zif_abapgit_git_definitions=>c_type-ref_d
            AND <ls_sweep>-type <> c_type_ofs_d.
          CONTINUE. " already resolved
        ENDIF.
        resolve_one(
          EXPORTING
            iv_tabix            = lv_tabix
            iv_depth             = 1
            it_offset_map        = it_offset_map
            it_ofs_meta          = it_ofs_meta
            iv_repo_key          = iv_repo_key
            iv_allow_thin_fetch  = abap_false
          CHANGING
            ct_objects        = ct_objects
            ct_tabix_by_index = lt_tabix_by_index ).
        READ TABLE ct_objects ASSIGNING <ls_sweep> INDEX lv_tabix.
        IF sy-subrc = 0
            AND <ls_sweep>-type <> zif_abapgit_git_definitions=>c_type-ref_d
            AND <ls_sweep>-type <> c_type_ofs_d.
          lv_pass_progress = abap_true.
        ENDIF.
      ENDWHILE.
      IF lv_pass_progress = abap_false.
        EXIT.
      ENDIF.
    ENDDO.

    " Phase 1.5 (Package D / D1): phase 1's fixpoint has now converged -
    " any object still type = ref_d has no in-pack candidate (proved
    " exhaustively by the repeated sweeps above), so it is either genuinely
    " external (a prior pack/pull) or truly missing. OFS deltas never reach
    " this point (their base is always physically earlier in the SAME pack,
    " a format guarantee), so only ref_d rows are collected. The full
    " deduplicated set is bulk-loaded in ONE call via the shared
    " bulk_resolve_external_bases helper (also used by the streaming
    " resolver, zcl_abapgit_ortec_pack_stream=>resolve_streaming) instead of
    " a per-base fetch inside phase 2 (Package D design §4/§5.1). Every
    " loaded base is merged with a freshly assigned unique index - the same
    " "index = lines(ct_objects)+1" pattern resolve_one's own on-demand
    " thin-fetch fallback already uses - and registered in
    " lt_tabix_by_index so phase 2's ordinary in-pack sha1 lookup finds it
    " directly; phase 2 below performs no per-base get_object() call of its
    " own once this merge has run.
    LOOP AT ct_objects ASSIGNING <ls_unresolved>
        WHERE type = zif_abapgit_git_definitions=>c_type-ref_d.
      APPEND <ls_unresolved>-sha1 TO lt_external_bases.
    ENDLOOP.

    IF lt_external_bases IS NOT INITIAL.
      lt_loaded_bases = bulk_resolve_external_bases(
        iv_repo_key = iv_repo_key
        it_sha1s    = lt_external_bases ).

      LOOP AT lt_loaded_bases ASSIGNING <ls_loaded_base>.
        <ls_loaded_base>-index = lines( ct_objects ) + 1.
        APPEND <ls_loaded_base> TO ct_objects.
        INSERT VALUE #( obj_index = <ls_loaded_base>-index tabix = lines( ct_objects ) )
          INTO TABLE lt_tabix_by_index.
      ENDLOOP.
    ENDIF.

    " Phase 2: one final ascending pass, now allowing the object-store fetch
    " and the precise "Delta base not found" raise - whatever remains
    " unresolved after phase 1.5's merge is truly missing (phase 1.5 always
    " raises first in that case, so this fallback fetch is dead in the
    " normal path and exists purely as a defensive backstop).
    lv_tabix = 0.
    WHILE lv_tabix < lines( ct_objects ).
      lv_tabix = lv_tabix + 1.
      resolve_one(
        EXPORTING
          iv_tabix      = lv_tabix
          iv_depth      = 1
          it_offset_map = it_offset_map
          it_ofs_meta   = it_ofs_meta
          iv_repo_key   = iv_repo_key
        CHANGING
          ct_objects        = ct_objects
          ct_tabix_by_index = lt_tabix_by_index ).
    ENDWHILE.
  ENDMETHOD.


  METHOD bulk_resolve_external_bases.
    DATA lt_unique_set TYPE HASHED TABLE OF zif_abapgit_git_definitions=>ty_sha1
      WITH UNIQUE KEY table_line.
    DATA lt_request    TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lt_loaded     TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lv_recomputed TYPE zif_abapgit_git_definitions=>ty_sha1.

    FIELD-SYMBOLS <lv_sha1>   TYPE zif_abapgit_git_definitions=>ty_sha1.
    FIELD-SYMBOLS <ls_loaded> LIKE LINE OF lt_loaded.

    LOOP AT it_sha1s ASSIGNING <lv_sha1> WHERE table_line IS NOT INITIAL.
      INSERT <lv_sha1> INTO TABLE lt_unique_set.
    ENDLOOP.

    IF lt_unique_set IS INITIAL.
      RETURN.
    ENDIF.

    LOOP AT lt_unique_set ASSIGNING <lv_sha1>.
      APPEND <lv_sha1> TO lt_request.
    ENDLOOP.

    gv_bulk_load_calls = gv_bulk_load_calls + 1.
    TRY.
        lt_loaded = zcl_abapgit_ortec_obj_store=>get_objects(
          iv_repo_key   = iv_repo_key
          it_sha1s      = lt_request
          iv_bulk_fetch = abap_true ).
      CATCH zcx_abapgit_ortec_git INTO DATA(lx_missing).
        zcx_abapgit_exception=>raise( |Delta base bulk load failed: { lx_missing->get_text( ) }| ).
    ENDTRY.

    LOOP AT lt_loaded ASSIGNING <ls_loaded>.
      IF <ls_loaded>-type = zif_abapgit_git_definitions=>c_type-ref_d
          OR <ls_loaded>-type = c_type_ofs_d.
        zcx_abapgit_exception=>raise(
          |Delta base { <ls_loaded>-sha1 } is itself an unresolved delta (type { <ls_loaded>-type })| ).
      ENDIF.

      lv_recomputed = zcl_abapgit_hash=>sha1( iv_type = <ls_loaded>-type iv_data = <ls_loaded>-data ).
      IF lv_recomputed <> <ls_loaded>-sha1.
        zcx_abapgit_exception=>raise(
          |Delta base { <ls_loaded>-sha1 } failed hash verification (recomputed { lv_recomputed })| ).
      ENDIF.
    ENDLOOP.

    rt_objects = lt_loaded.
  ENDMETHOD.


  METHOD resolve_one.
    DATA ls_base_object     TYPE zif_abapgit_definitions=>ty_object.
    DATA lv_base_tabix      TYPE i.
    DATA lv_base_offset     TYPE i.
    DATA lv_result          TYPE xstring.
    DATA lv_final_sha1      TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA ls_ofs_meta        TYPE ty_ofs_meta.
    DATA ls_offset_entry    TYPE ty_offset_entry.
    DATA lv_found_obj_index TYPE i.
    DATA ls_tabix_by_index  TYPE ty_tabix_by_index.

    FIELD-SYMBOLS <ls_object> TYPE zif_abapgit_definitions=>ty_object.
    FIELD-SYMBOLS <ls_base>   TYPE zif_abapgit_definitions=>ty_object.

    READ TABLE ct_objects ASSIGNING <ls_object> INDEX iv_tabix.
    IF sy-subrc <> 0.
      zcx_abapgit_exception=>raise( |Delta resolve: internal index { iv_tabix } out of range| ).
    ENDIF.

    IF <ls_object>-type <> zif_abapgit_git_definitions=>c_type-ref_d
        AND <ls_object>-type <> c_type_ofs_d.
      RETURN. " Already a fully resolved object - nothing to do (memoized base case).
    ENDIF.

    IF iv_depth > c_max_chain_depth.
      zcx_abapgit_exception=>raise(
        |Delta chain exceeds maximum depth { c_max_chain_depth } (possible cycle)| ).
    ENDIF.

    IF <ls_object>-type = zif_abapgit_git_definitions=>c_type-ref_d.
      " Base identified by content SHA1. -sha1 is overloaded: an unresolved
      " ref_d/ofs_d entry's -sha1 holds ITS OWN declared base (a
      " placeholder), never its own eventual identity - so a genuinely
      " unresolved base can NEVER legitimately match this search. Because
      " the "sha" key is NON-UNIQUE, multiple entries (including a sibling
      " that shares the same declared base, or a true base that happens to
      " be positioned AFTER its dependent in the pack - REF_DELTA carries
      " no ordering guarantee) can simultaneously carry the identical
      " placeholder/identity value. Skip self and anything still
      " ref_d/ofs_d (an unresolved candidate can never be a valid match:
      " resolving it only changes ITS OWN identity, it does not retroactively
      " make it equal the value being searched for) - resolve_all's
      " multi-pass sweep is what makes a later-resolved base find-able here,
      " by the time this same entry is revisited in a subsequent pass.
      CLEAR lv_base_tabix.
      lv_found_obj_index = -1.
      LOOP AT ct_objects ASSIGNING <ls_base>
          USING KEY sha
          WHERE sha1 = <ls_object>-sha1.
        IF <ls_base>-index = <ls_object>-index.
          CONTINUE. " never match self
        ENDIF.
        IF <ls_base>-type <> zif_abapgit_git_definitions=>c_type-ref_d
            AND <ls_base>-type <> c_type_ofs_d.
          lv_found_obj_index = <ls_base>-index.
          EXIT.
        ENDIF.
      ENDLOOP.

      IF lv_found_obj_index <> -1.
        " sy-tabix inside a "LOOP AT ... USING KEY sha" reflects the
        " position within that SORTED SECONDARY key's own iteration order,
        " NOT the primary table index (documented ABAP behavior). Re-derive
        " the correct PRIMARY tabix via the O(1) hashed side-index instead
        " of trusting sy-tabix from the secondary-key loop, and instead of
        " a linear "WITH KEY index = ..." scan (ct_objects has no secondary
        " key on "index"; resolve_one runs once per delta object, so a
        " per-call linear scan would risk O(n^2) total cost on large packs).
        READ TABLE ct_tabix_by_index INTO ls_tabix_by_index
          WITH TABLE KEY obj_index = lv_found_obj_index.
        IF sy-subrc = 0.
          lv_base_tabix = ls_tabix_by_index-tabix.
        ENDIF.
      ENDIF.

      IF lv_base_tabix IS INITIAL.
        IF iv_allow_thin_fetch = abap_false.
          " Not yet resolvable purely in-pack (its true base may itself be
          " an unresolved delta elsewhere in the pack, not reached by a
          " prior sweep yet). Leave this entry as-is; resolve_all's next
          " sweep will retry it. Do NOT fetch from the object store or
          " raise here - that would treat "not yet" as "never", the exact
          " bug this parameter exists to avoid.
          RETURN.
        ENDIF.
        " No already-resolved candidate exists anywhere in the pack, even
        " after every possible in-pack sweep - thin base (fetch from the
        " persistent object store), or a case this decoder cannot safely
        " resolve from the current pack alone. resolve_all's phase 1.5
        " already bulk-loads every such base before phase 2 ever calls
        " resolve_one with iv_allow_thin_fetch = abap_true, so this branch
        " is dead in the normal path - gv_thin_fetch_calls exists purely so
        " D1's tests can assert it stays at zero (see this class's
        " testclasses include).
        gv_thin_fetch_calls = gv_thin_fetch_calls + 1.
        TRY.
            ls_base_object = zcl_abapgit_ortec_obj_store=>get_object(
              iv_repo_key = iv_repo_key
              iv_sha1     = <ls_object>-sha1 ).
          CATCH zcx_abapgit_ortec_git.
            zcx_abapgit_exception=>raise( |Delta base not found, { <ls_object>-sha1 }| ).
        ENDTRY.
        " The object store never populates -index (it is a pack-decode-only
        " concept - get_object/get_objects only set sha1/type/data), so every
        " thin-fetched object would otherwise default to index = 0 and
        " collide with any other thin base fetched in the same pass. Assign
        " a fresh, unique index matching its new primary position instead.
        ls_base_object-index = lines( ct_objects ) + 1.
        APPEND ls_base_object TO ct_objects.
        " Bind directly to the row just appended (its primary index is
        " exactly lines(ct_objects) at this point) instead of a first-match
        " lookup on the non-unique "sha" key, which could otherwise match
        " an unresolved sibling delta that merely shares the same declared
        " base placeholder value.
        READ TABLE ct_objects ASSIGNING <ls_base> INDEX lines( ct_objects ).
        IF sy-subrc <> 0.
          zcx_abapgit_exception=>raise( |Delta base not found, { <ls_object>-sha1 }| ).
        ENDIF.
        INSERT VALUE #( obj_index = ls_base_object-index tabix = lines( ct_objects ) )
          INTO TABLE ct_tabix_by_index.
        " The APPEND above may have reallocated ct_objects, invalidating
        " <ls_object> (obtained before it ran) - re-bind before it is read
        " again by the sanity check below.
        READ TABLE ct_objects ASSIGNING <ls_object> INDEX iv_tabix.
        IF sy-subrc <> 0.
          zcx_abapgit_exception=>raise( |Delta resolve: internal index { iv_tabix } out of range| ).
        ENDIF.
      ELSE.
        " Already resolved (or always a plain object) - resolve_one is a
        " no-op in that case; kept for symmetry/defensiveness.
        resolve_one(
          EXPORTING
            iv_tabix            = lv_base_tabix
            iv_depth            = iv_depth + 1
            it_offset_map       = it_offset_map
            it_ofs_meta         = it_ofs_meta
            iv_repo_key         = iv_repo_key
            iv_allow_thin_fetch = iv_allow_thin_fetch
          CHANGING
            ct_objects        = ct_objects
            ct_tabix_by_index = ct_tabix_by_index ).
        READ TABLE ct_objects ASSIGNING <ls_base> INDEX lv_base_tabix.
        IF sy-subrc <> 0.
          zcx_abapgit_exception=>raise( |Delta resolve: internal index { lv_base_tabix } out of range| ).
        ENDIF.
      ENDIF.

      " Defensive sanity check: the found/fetched base's real identity must
      " exactly match the declared base. Two prior bugs in this exact area
      " (an unresolved-sibling match, then a stale secondary key after
      " in-place promotion) would both have been caught immediately and
      " unambiguously by this check instead of surfacing indirectly as a
      " generic "exceeds base length" failure from apply(). If this ever
      " fires, it proves the base-FINDING mechanism is still at fault,
      " ruling out a separate base-RECONSTRUCTION issue.
      IF <ls_base>-sha1 <> <ls_object>-sha1.
        zcx_abapgit_exception=>raise(
          |Delta base identity mismatch: declared { <ls_object>-sha1 }, | &&
          |resolved { <ls_base>-sha1 } (type { <ls_base>-type }, | &&
          |{ xstrlen( <ls_base>-data ) } bytes)| ).
      ENDIF.

    ELSE. " c_type_ofs_d
      READ TABLE it_ofs_meta INTO ls_ofs_meta
        WITH TABLE KEY by_index COMPONENTS obj_index = <ls_object>-index.
      IF sy-subrc <> 0.
        zcx_abapgit_exception=>raise(
          |OFS delta: no recorded base offset for object index { <ls_object>-index }| ).
      ENDIF.
      lv_base_offset = ls_ofs_meta-base_offset.

      IF lv_base_offset < 0.
        zcx_abapgit_exception=>raise( |OFS delta: base offset { lv_base_offset } is negative| ).
      ENDIF.

      READ TABLE it_offset_map INTO ls_offset_entry
        WITH TABLE KEY by_offset COMPONENTS pack_offset = lv_base_offset.
      IF sy-subrc <> 0.
        zcx_abapgit_exception=>raise(
          |OFS delta: no object found at base offset { lv_base_offset }| ).
      ENDIF.

      " O(1) hashed lookup instead of a linear "WITH KEY index = ..." scan
      " (ct_objects has no secondary key on "index") - see ty_tabix_by_index.
      READ TABLE ct_tabix_by_index INTO ls_tabix_by_index
        WITH TABLE KEY obj_index = ls_offset_entry-obj_index.
      IF sy-subrc <> 0.
        zcx_abapgit_exception=>raise(
          |OFS delta: object index { ls_offset_entry-obj_index } not found| ).
      ENDIF.
      lv_base_tabix = ls_tabix_by_index-tabix.

      READ TABLE ct_objects ASSIGNING <ls_base> INDEX lv_base_tabix.
      IF sy-subrc <> 0.
        zcx_abapgit_exception=>raise(
          |OFS delta: object index { ls_offset_entry-obj_index } not found| ).
      ENDIF.

      " OFS bases are always earlier in the SAME pack byte stream (the format
      " guarantees offsets point strictly backwards), so the base is always
      " already among the parsed objects - but it may itself be an unresolved
      " ref/ofs delta (a chain), so resolve it first.
      IF <ls_base>-type = zif_abapgit_git_definitions=>c_type-ref_d
          OR <ls_base>-type = c_type_ofs_d.
        resolve_one(
          EXPORTING
            iv_tabix            = lv_base_tabix
            iv_depth            = iv_depth + 1
            it_offset_map       = it_offset_map
            it_ofs_meta         = it_ofs_meta
            iv_repo_key         = iv_repo_key
            iv_allow_thin_fetch = iv_allow_thin_fetch
          CHANGING
            ct_objects        = ct_objects
            ct_tabix_by_index = ct_tabix_by_index ).
        READ TABLE ct_objects ASSIGNING <ls_base> INDEX lv_base_tabix.
        IF sy-subrc <> 0.
          zcx_abapgit_exception=>raise( |Delta resolve: internal index { lv_base_tabix } out of range| ).
        ENDIF.
      ENDIF.
    ENDIF.

    IF <ls_base>-type = zif_abapgit_git_definitions=>c_type-ref_d
        OR <ls_base>-type = c_type_ofs_d.
      IF iv_allow_thin_fetch = abap_false.
        " OFS base is itself a REF_DELTA/OFS_DELTA that couldn't be resolved
        " purely in-pack yet this sweep (e.g. ITS OWN base is a REF_DELTA
        " positioned later in the pack) - leave this entry unresolved too;
        " resolve_all's next sweep will retry it once that dependency is
        " resolved.
        RETURN.
      ENDIF.
      " Defensive: resolve_one above must have turned the base into a real
      " object; this should be unreachable during the final pass, but never
      " apply a delta on top of another unresolved delta.
      zcx_abapgit_exception=>raise( |Delta, base still unresolved| ).
    ENDIF.

    " Re-fetch the dependent entry: recursive calls above may have caused
    " ct_objects to be reallocated (APPEND on a thin-base fetch), invalidating
    " the earlier field-symbol assignment.
    READ TABLE ct_objects ASSIGNING <ls_object> INDEX iv_tabix.
    IF sy-subrc <> 0.
      zcx_abapgit_exception=>raise( |Delta resolve: internal index { iv_tabix } out of range| ).
    ENDIF.

    TRY.
        lv_result = apply( iv_base = <ls_base>-data iv_delta = <ls_object>-data ).
      CATCH zcx_abapgit_exception INTO DATA(lx_apply).
        zcx_abapgit_exception=>raise(
          |{ lx_apply->get_text( ) } - base sha1 { <ls_base>-sha1 } type { <ls_base>-type } | &&
          |{ xstrlen( <ls_base>-data ) } bytes, delta { xstrlen( <ls_object>-data ) } bytes, | &&
          |depth { iv_depth }| ).
    ENDTRY.
    lv_final_sha1 = zcl_abapgit_hash=>sha1( iv_type = <ls_base>-type iv_data = lv_result ).

    " Promote via MODIFY, not a field-symbol write: sha1 is a component of
    " the "sha" secondary sorted key (see the type definition of
    " ty_objects_tt). Writing to a key-participating field through a field
    " symbol obtained via ASSIGNING does NOT update the secondary key's
    " internal sort structure - this is documented ABAP behavior, not a
    " defect isolated to this class. Every later REF_DELTA base lookup in
    " the same resolve_all pass would then risk matching the WRONG row via
    " that now-stale key, feeding an unrelated (but genuinely "already
    " resolved") object into apply(). MODIFY correctly re-integrates the
    " row and keeps the "sha" key valid for every subsequent lookup.
    <ls_object>-type = <ls_base>-type.
    <ls_object>-data = lv_result.
    <ls_object>-sha1 = lv_final_sha1.
    MODIFY ct_objects FROM <ls_object> INDEX iv_tabix TRANSPORTING type data sha1.

  ENDMETHOD.

ENDCLASS.
