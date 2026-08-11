CLASS zcl_abapgit_ortec_obj_cover DEFINITION LOCAL FRIENDS ltcl_obj_cover.
CLASS ltcl_obj_cover DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key VALUE 'ZAOGT_OBJCOV'.
    CONSTANTS mc_commit TYPE zif_abapgit_git_definitions=>ty_sha1
      VALUE 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'.

    METHODS setup.
    METHODS teardown.

    METHODS build_coverage
      IMPORTING
        iv_obj_name       TYPE tadir-obj_name
        iv_status         TYPE zaog_obj_cover-resolution_status DEFAULT zcl_abapgit_ortec_obj_cover=>cs_resolution-found
      RETURNING
        VALUE(rs_coverage) TYPE zcl_abapgit_ortec_obj_cover=>ty_coverage.

    METHODS context_hash_stable_for_same_input FOR TESTING RAISING cx_static_check.
    METHODS context_hash_changes_on_devclass FOR TESTING RAISING cx_static_check.
    METHODS context_hash_changes_on_dot_change FOR TESTING RAISING cx_static_check.
    METHODS context_hash_embeds_algo_version FOR TESTING RAISING cx_static_check.

    METHODS coverage_round_trip FOR TESTING RAISING cx_static_check.
    METHODS coverage_context_mismatch_excl FOR TESTING RAISING cx_static_check.
    METHODS coverage_upsert_idempotent FOR TESTING RAISING cx_static_check.
    METHODS coverage_write_chunk_boundary FOR TESTING RAISING cx_static_check.
    METHODS coverage_read_chunk_boundary FOR TESTING RAISING cx_static_check.
    METHODS write_coverage_diag_on_success FOR TESTING RAISING cx_static_check.
ENDCLASS.


CLASS ltcl_obj_cover IMPLEMENTATION.

  METHOD setup.
    DELETE FROM zaog_obj_cover WHERE repo_key = mc_repo.
  ENDMETHOD.

  METHOD teardown.
    DELETE FROM zaog_obj_cover WHERE repo_key = mc_repo.
    ROLLBACK WORK.
  ENDMETHOD.

  METHOD build_coverage.
    rs_coverage-obj_type          = 'PROG'.
    rs_coverage-obj_name          = iv_obj_name.
    rs_coverage-resolution_status = iv_status.
  ENDMETHOD.

  METHOD context_hash_stable_for_same_input.
    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).

    DATA(lv_hash_1) = zcl_abapgit_ortec_obj_cover=>compute_context_hash(
      iv_devclass = '$PACK' io_dot = lo_dot ).
    DATA(lv_hash_2) = zcl_abapgit_ortec_obj_cover=>compute_context_hash(
      iv_devclass = '$PACK' io_dot = lo_dot ).

    cl_abap_unit_assert=>assert_equals( act = lv_hash_2 exp = lv_hash_1
      msg = 'Two calls with identical inputs must produce identical hashes' ).
    cl_abap_unit_assert=>assert_true( act = xsdbool( lv_hash_1 IS NOT INITIAL )
      msg = 'The computed hash must not be blank' ).
  ENDMETHOD.

  METHOD context_hash_changes_on_devclass.
    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).

    DATA(lv_hash_a) = zcl_abapgit_ortec_obj_cover=>compute_context_hash(
      iv_devclass = '$PACKA' io_dot = lo_dot ).
    DATA(lv_hash_b) = zcl_abapgit_ortec_obj_cover=>compute_context_hash(
      iv_devclass = '$PACKB' io_dot = lo_dot ).

    cl_abap_unit_assert=>assert_differs( act = lv_hash_b exp = lv_hash_a
      msg = 'A different devclass must change the context hash' ).
  ENDMETHOD.

  METHOD context_hash_changes_on_dot_change.
    DATA(lo_dot_a) = zcl_abapgit_dot_abapgit=>build_default( ).
    DATA(ls_dot_data) = lo_dot_a->get_data( ).
    ls_dot_data-starting_folder = '/other/'.
    DATA(lo_dot_b) = NEW zcl_abapgit_dot_abapgit( ls_dot_data ).

    DATA(lv_hash_a) = zcl_abapgit_ortec_obj_cover=>compute_context_hash(
      iv_devclass = '$PACK' io_dot = lo_dot_a ).
    DATA(lv_hash_b) = zcl_abapgit_ortec_obj_cover=>compute_context_hash(
      iv_devclass = '$PACK' io_dot = lo_dot_b ).

    cl_abap_unit_assert=>assert_differs( act = lv_hash_b exp = lv_hash_a
      msg = 'A changed .abapgit config (starting_folder) must change the context hash' ).
  ENDMETHOD.

  METHOD context_hash_embeds_algo_version.
    " c_algo_version is a private compile-time constant - it cannot be
    " "bumped" at test runtime without a production source change, so this
    " test does not literally exercise a version bump (see design doc §3.1
    " context_hash_changes_on_algo_bump). Instead it proves the algorithm
    " version is genuinely embedded in the hash input by independently
    " reproducing the documented formula (fixed '0001' prefix + devclass,
    " UTF-8, concatenated with the serialized .abapgit, SHA1-raw) and
    " asserting it matches the production output byte-for-byte - a wrong
    " embedded literal would make this comparison fail.
    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).

    DATA(lv_actual) = zcl_abapgit_ortec_obj_cover=>compute_context_hash(
      iv_devclass = '$PACK' io_dot = lo_dot ).

    DATA(lv_prefix_xstr) = zcl_abapgit_convert=>string_to_xstring_utf8( |0001$PACK| ).
    DATA(lv_expected) = zcl_abapgit_hash=>sha1_raw( lv_prefix_xstr && lo_dot->serialize( ) ).

    cl_abap_unit_assert=>assert_equals( act = lv_actual exp = lv_expected
      msg = 'The documented algo-version literal (0001) must be embedded as a fixed prefix' ).
  ENDMETHOD.

  METHOD coverage_round_trip.
    DATA(lt_filter) = VALUE zif_abapgit_definitions=>ty_tadir_tt(
      ( object = 'PROG' obj_name = 'ZFOUND' )
      ( object = 'PROG' obj_name = 'ZUNCOVERED' ) ).

    zcl_abapgit_ortec_obj_cover=>write_coverage(
      iv_repo_key        = mc_repo
      iv_commit          = mc_commit
      iv_context_hash    = 'A'
      iv_walk_hist_level = zcl_abapgit_ortec_mat_state=>cs_hist_level-unknown
      it_results         = VALUE #( ( build_coverage( iv_obj_name = 'ZFOUND' ) ) ) ).

    DATA(lt_coverage) = zcl_abapgit_ortec_obj_cover=>get_coverage(
      iv_repo_key     = mc_repo
      iv_commit       = mc_commit
      iv_context_hash = 'A'
      it_filter       = lt_filter ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_coverage ) exp = 1
      msg = 'Only the written object must be reported as covered' ).
    cl_abap_unit_assert=>assert_equals( act = lt_coverage[ 1 ]-obj_name exp = 'ZFOUND' ).
    cl_abap_unit_assert=>assert_equals( act = lt_coverage[ 1 ]-resolution_status
      exp = zcl_abapgit_ortec_obj_cover=>cs_resolution-found ).
  ENDMETHOD.

  METHOD coverage_context_mismatch_excl.
    zcl_abapgit_ortec_obj_cover=>write_coverage(
      iv_repo_key        = mc_repo
      iv_commit          = mc_commit
      iv_context_hash    = 'A'
      iv_walk_hist_level = zcl_abapgit_ortec_mat_state=>cs_hist_level-unknown
      it_results         = VALUE #( ( build_coverage( iv_obj_name = 'ZFOUND' ) ) ) ).

    DATA(lt_coverage) = zcl_abapgit_ortec_obj_cover=>get_coverage(
      iv_repo_key     = mc_repo
      iv_commit       = mc_commit
      iv_context_hash = 'B'
      it_filter       = VALUE zif_abapgit_definitions=>ty_tadir_tt( ( object = 'PROG' obj_name = 'ZFOUND' ) ) ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_coverage ) exp = 0
      msg = 'A row written under context A must be invisible to a read under context B' ).
  ENDMETHOD.

  METHOD coverage_upsert_idempotent.
    DATA(lt_results) = VALUE zcl_abapgit_ortec_obj_cover=>ty_coverage_tt(
      ( build_coverage( iv_obj_name = 'ZFOUND' iv_status = zcl_abapgit_ortec_obj_cover=>cs_resolution-found ) ) ).

    zcl_abapgit_ortec_obj_cover=>write_coverage(
      iv_repo_key        = mc_repo
      iv_commit          = mc_commit
      iv_context_hash    = 'A'
      iv_walk_hist_level = zcl_abapgit_ortec_mat_state=>cs_hist_level-unknown
      it_results         = lt_results ).

    lt_results[ 1 ]-resolution_status = zcl_abapgit_ortec_obj_cover=>cs_resolution-resolved_no_files.

    zcl_abapgit_ortec_obj_cover=>write_coverage(
      iv_repo_key        = mc_repo
      iv_commit          = mc_commit
      iv_context_hash    = 'A'
      iv_walk_hist_level = zcl_abapgit_ortec_mat_state=>cs_hist_level-unknown
      it_results         = lt_results ).

    DATA(lt_coverage) = zcl_abapgit_ortec_obj_cover=>get_coverage(
      iv_repo_key     = mc_repo
      iv_commit       = mc_commit
      iv_context_hash = 'A'
      it_filter       = VALUE zif_abapgit_definitions=>ty_tadir_tt( ( object = 'PROG' obj_name = 'ZFOUND' ) ) ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_coverage ) exp = 1
      msg = 'A second write for the same object/context must upsert, never duplicate' ).
    cl_abap_unit_assert=>assert_equals( act = lt_coverage[ 1 ]-resolution_status
      exp = zcl_abapgit_ortec_obj_cover=>cs_resolution-resolved_no_files
      msg = 'The second write must overwrite the first result' ).
  ENDMETHOD.

  METHOD coverage_write_chunk_boundary.
    DATA lt_results TYPE zcl_abapgit_ortec_obj_cover=>ty_coverage_tt.
    DATA lt_filter  TYPE zif_abapgit_definitions=>ty_tadir_tt.

    DO 5001 TIMES.
      DATA(lv_name) = |ZBULK{ sy-index WIDTH = 6 ALIGN = RIGHT PAD = '0' }|.
      APPEND build_coverage( iv_obj_name = lv_name ) TO lt_results.
      APPEND VALUE #( object = 'PROG' obj_name = lv_name ) TO lt_filter.
    ENDDO.

    zcl_abapgit_ortec_obj_cover=>write_coverage(
      iv_repo_key        = mc_repo
      iv_commit          = mc_commit
      iv_context_hash    = 'A'
      iv_walk_hist_level = zcl_abapgit_ortec_mat_state=>cs_hist_level-unknown
      it_results         = lt_results ).

    SELECT COUNT(*) FROM zaog_obj_cover INTO @DATA(lv_count)
      WHERE repo_key = @mc_repo AND commit_sha1 = @mc_commit AND context_hash = 'A'.

    cl_abap_unit_assert=>assert_equals( act = lv_count exp = 5001
      msg = 'A write crossing the chunk boundary must persist every row, not just the first chunk' ).
  ENDMETHOD.

  METHOD coverage_read_chunk_boundary.
    DATA lt_results TYPE zcl_abapgit_ortec_obj_cover=>ty_coverage_tt.
    DATA lt_filter  TYPE zif_abapgit_definitions=>ty_tadir_tt.

    DO 5001 TIMES.
      DATA(lv_name) = |ZBULK{ sy-index WIDTH = 6 ALIGN = RIGHT PAD = '0' }|.
      APPEND build_coverage( iv_obj_name = lv_name ) TO lt_results.
      APPEND VALUE #( object = 'PROG' obj_name = lv_name ) TO lt_filter.
    ENDDO.

    zcl_abapgit_ortec_obj_cover=>write_coverage(
      iv_repo_key        = mc_repo
      iv_commit          = mc_commit
      iv_context_hash    = 'A'
      iv_walk_hist_level = zcl_abapgit_ortec_mat_state=>cs_hist_level-unknown
      it_results         = lt_results ).

    DATA(lt_coverage) = zcl_abapgit_ortec_obj_cover=>get_coverage(
      iv_repo_key     = mc_repo
      iv_commit       = mc_commit
      iv_context_hash = 'A'
      it_filter       = lt_filter ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_coverage ) exp = 5001
      msg = 'A read with a filter crossing the chunk boundary must return every matching row' ).
  ENDMETHOD.

  METHOD write_coverage_diag_on_success.
    DATA(ls_before) = zcl_abapgit_ortec_obj_cover=>get_diagnostics( ).

    zcl_abapgit_ortec_obj_cover=>write_coverage(
      iv_repo_key        = mc_repo
      iv_commit          = mc_commit
      iv_context_hash    = 'A'
      iv_walk_hist_level = zcl_abapgit_ortec_mat_state=>cs_hist_level-unknown
      it_results         = VALUE #( ( build_coverage( iv_obj_name = 'ZFOUND' ) ) ) ).

    DATA(ls_after) = zcl_abapgit_ortec_obj_cover=>get_diagnostics( ).

    cl_abap_unit_assert=>assert_equals( act = ls_after-failure_count exp = ls_before-failure_count
      msg = 'A successful write_coverage call must not increment the failure counter' ).
  ENDMETHOD.

ENDCLASS.
