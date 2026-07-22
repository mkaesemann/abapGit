CLASS ltcl_cold_init DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  PRIVATE SECTION.
    CONSTANTS c_tip TYPE zif_abapgit_git_definitions=>ty_sha1 VALUE 'AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'.

    METHODS empty_repo_key_raises FOR TESTING RAISING cx_static_check.
    METHODS empty_tip_commit_raises FOR TESTING RAISING cx_static_check.
    METHODS graph_response_ceiling_value FOR TESTING RAISING cx_static_check.

ENDCLASS.


CLASS ltcl_cold_init IMPLEMENTATION.

  METHOD empty_repo_key_raises.
    " Both guard clauses run BEFORE any HTTP call is made - this codebase
    " has no HTTP client injection point (zcl_abapgit_ortec_fastpath's
    " own upload_pack_by_commit/upload_pack_by_branch are, for the same
    " reason, likewise only unit tested up to build_request/parse in
    " isolation, never end to end) - so only the pre-HTTP guard clauses of
    " acquire_blobless_graph are directly unit-testable here.
    TRY.
        zcl_abapgit_ortec_cold_init=>acquire_blobless_graph(
          iv_url        = 'https://example.com/x.git'
          iv_repo_key   = ''
          iv_tip_commit = c_tip ).
        cl_abap_unit_assert=>fail( 'Empty repo key must raise' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.

  METHOD empty_tip_commit_raises.
    TRY.
        zcl_abapgit_ortec_cold_init=>acquire_blobless_graph(
          iv_url        = 'https://example.com/x.git'
          iv_repo_key   = 'ZAOG_TEST_01'
          iv_tip_commit = '' ).
        cl_abap_unit_assert=>fail( 'Empty tip commit must raise' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.

  METHOD graph_response_ceiling_value.
    " Pins INV-B-12 (.memory/logs/variant_b_package_b_design.md §10): the
    " memory-risk gate ceiling for a single blobless-graph HTTP response.
    cl_abap_unit_assert=>assert_equals(
      act = zcl_abapgit_ortec_cold_init=>c_max_graph_response_bytes
      exp = 209715200
      msg = '200 MiB memory-risk ceiling must not silently drift' ).
  ENDMETHOD.

ENDCLASS.
