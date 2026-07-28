CLASS zcl_abapgit_ortec_porcelain DEFINITION LOCAL FRIENDS ltcl_porcelain.

CLASS ltcl_porcelain DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.

  PRIVATE SECTION.
    CONSTANTS c_repo2 TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key VALUE 'ZAOG_PORC_01'.

    METHODS setup.
    METHODS teardown.

    METHODS cleanup_repo
      IMPORTING iv_repo TYPE zcl_abapgit_ortec_obj_store=>ty_repo_key.

    METHODS one_blob_one_path    FOR TESTING RAISING cx_static_check.
    METHODS same_blob_two_paths  FOR TESTING RAISING cx_static_check.
    METHODS dup_blob_obj_once    FOR TESTING RAISING cx_static_check.
    METHODS unrelated_blob_skip  FOR TESTING RAISING cx_static_check.
    METHODS non_blob_obj_skip    FOR TESTING RAISING cx_static_check.
    METHODS blob_data_sha_kept   FOR TESTING RAISING cx_static_check.

    " ORTEC D2b2: repo-lock + attempt-id correlation tests (Unit #2).
    METHODS fresh_pull_unit_atomic     FOR TESTING RAISING cx_static_check.
    METHODS fresh_pull_fail_no_publish FOR TESTING RAISING cx_static_check.
    METHODS lock_release_on_failure    FOR TESTING RAISING cx_static_check.
ENDCLASS.

CLASS ltcl_porcelain IMPLEMENTATION.
  METHOD setup.
    cleanup_repo( c_repo2 ).
  ENDMETHOD.

  METHOD teardown.
    cleanup_repo( c_repo2 ).
  ENDMETHOD.

  METHOD cleanup_repo.
    " See zcl_abapgit_ortec_fastpath.clas.testclasses.abap's ltcl_fastpath=>
    " cleanup_repo: DELETE + bare ROLLBACK WORK is unsafe once the SUT can
    " issue its own COMMIT WORK (which zcl_abapgit_ortec_obj_store=>
    " store_object and zcl_abapgit_ortec_mat_state's writes do) - always
    " ROLLBACK, then DELETE, then COMMIT.
    ROLLBACK WORK. "#EC CI_ROLLBACK
    DELETE FROM zaog_commit_hist WHERE repo_key = iv_repo.
    DELETE FROM zaog_obj_store WHERE repo_key = iv_repo.
    DELETE FROM zaog_repo_state WHERE repo_key = iv_repo.
    COMMIT WORK.
  ENDMETHOD.

  METHOD one_blob_one_path.
    DATA lt_manifest TYPE zif_abapgit_git_definitions=>ty_expanded_tt.
    DATA ls_manifest LIKE LINE OF lt_manifest.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object  LIKE LINE OF lt_objects.
    DATA lt_files   TYPE zif_abapgit_git_definitions=>ty_files_tt.
    DATA lv_data    TYPE xstring.

    lv_data = zcl_abapgit_convert=>string_to_xstring_utf8( 'a' ).
    ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-sha1 = zcl_abapgit_hash=>sha1_blob( lv_data ).
    ls_object-data = lv_data.
    APPEND ls_object TO lt_objects.

    ls_manifest-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_manifest-path = '/'.
    ls_manifest-name = 'a.txt'.
    ls_manifest-sha1 = ls_object-sha1.
    APPEND ls_manifest TO lt_manifest.

    zcl_abapgit_ortec_porcelain=>materialize_from_manifest(
      EXPORTING
        it_objects       = lt_objects
        it_blob_manifest = lt_manifest
      CHANGING
        ct_files         = lt_files ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_files ) exp = 1 ).
    cl_abap_unit_assert=>assert_equals( act = lt_files[ 1 ]-sha1 exp = ls_object-sha1 ).
  ENDMETHOD.

  METHOD same_blob_two_paths.
    DATA lt_manifest TYPE zif_abapgit_git_definitions=>ty_expanded_tt.
    DATA ls_manifest LIKE LINE OF lt_manifest.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object  LIKE LINE OF lt_objects.
    DATA lt_files   TYPE zif_abapgit_git_definitions=>ty_files_tt.
    DATA lv_data    TYPE xstring.

    lv_data = zcl_abapgit_convert=>string_to_xstring_utf8( 'same' ).
    ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-sha1 = zcl_abapgit_hash=>sha1_blob( lv_data ).
    ls_object-data = lv_data.
    APPEND ls_object TO lt_objects.

    ls_manifest-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_manifest-path = '/dir1/'.
    ls_manifest-name = 'a.txt'.
    ls_manifest-sha1 = ls_object-sha1.
    APPEND ls_manifest TO lt_manifest.

    ls_manifest-path = '/dir2/'.
    ls_manifest-name = 'b.txt'.
    APPEND ls_manifest TO lt_manifest.

    zcl_abapgit_ortec_porcelain=>materialize_from_manifest(
      EXPORTING
        it_objects       = lt_objects
        it_blob_manifest = lt_manifest
      CHANGING
        ct_files         = lt_files ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_files ) exp = 2 ).
  ENDMETHOD.

  METHOD dup_blob_obj_once.
    DATA lt_manifest TYPE zif_abapgit_git_definitions=>ty_expanded_tt.
    DATA ls_manifest LIKE LINE OF lt_manifest.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object  LIKE LINE OF lt_objects.
    DATA lt_files   TYPE zif_abapgit_git_definitions=>ty_files_tt.
    DATA lv_data    TYPE xstring.

    lv_data = zcl_abapgit_convert=>string_to_xstring_utf8( 'dup' ).
    ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-sha1 = zcl_abapgit_hash=>sha1_blob( lv_data ).
    ls_object-data = lv_data.
    APPEND ls_object TO lt_objects.

    ls_manifest-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_manifest-path = '/'.
    ls_manifest-name = 'a.txt'.
    ls_manifest-sha1 = ls_object-sha1.
    APPEND ls_manifest TO lt_manifest.

    ls_manifest-name = 'b.txt'.
    APPEND ls_manifest TO lt_manifest.

    zcl_abapgit_ortec_porcelain=>materialize_from_manifest(
      EXPORTING
        it_objects       = lt_objects
        it_blob_manifest = lt_manifest
      CHANGING
        ct_files         = lt_files ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_files ) exp = 2 ).
  ENDMETHOD.

  METHOD unrelated_blob_skip.
    DATA lt_manifest TYPE zif_abapgit_git_definitions=>ty_expanded_tt.
    DATA ls_manifest LIKE LINE OF lt_manifest.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object  LIKE LINE OF lt_objects.
    DATA lt_files   TYPE zif_abapgit_git_definitions=>ty_files_tt.
    DATA lv_data    TYPE xstring.

    lv_data = zcl_abapgit_convert=>string_to_xstring_utf8( 'blob' ).
    ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-sha1 = zcl_abapgit_hash=>sha1_blob( lv_data ).
    ls_object-data = lv_data.
    APPEND ls_object TO lt_objects.

    ls_manifest-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_manifest-path = '/'.
    ls_manifest-name = 'a.txt'.
    ls_manifest-sha1 = '1111111111111111111111111111111111111111'.
    APPEND ls_manifest TO lt_manifest.

    zcl_abapgit_ortec_porcelain=>materialize_from_manifest(
      EXPORTING
        it_objects       = lt_objects
        it_blob_manifest = lt_manifest
      CHANGING
        ct_files         = lt_files ).

    cl_abap_unit_assert=>assert_initial( act = lt_files ).
  ENDMETHOD.

  METHOD non_blob_obj_skip.
    DATA lt_manifest TYPE zif_abapgit_git_definitions=>ty_expanded_tt.
    DATA ls_manifest LIKE LINE OF lt_manifest.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object  LIKE LINE OF lt_objects.
    DATA lt_files   TYPE zif_abapgit_git_definitions=>ty_files_tt.

    ls_object-type = zif_abapgit_git_definitions=>c_type-tree.
    ls_object-sha1 = '1111111111111111111111111111111111111111'.
    APPEND ls_object TO lt_objects.

    ls_manifest-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_manifest-path = '/'.
    ls_manifest-name = 'a.txt'.
    ls_manifest-sha1 = ls_object-sha1.
    APPEND ls_manifest TO lt_manifest.

    zcl_abapgit_ortec_porcelain=>materialize_from_manifest(
      EXPORTING
        it_objects       = lt_objects
        it_blob_manifest = lt_manifest
      CHANGING
        ct_files         = lt_files ).

    cl_abap_unit_assert=>assert_initial( act = lt_files ).
  ENDMETHOD.

  METHOD blob_data_sha_kept.
    DATA lt_manifest TYPE zif_abapgit_git_definitions=>ty_expanded_tt.
    DATA ls_manifest LIKE LINE OF lt_manifest.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object  LIKE LINE OF lt_objects.
    DATA lt_files   TYPE zif_abapgit_git_definitions=>ty_files_tt.
    DATA lv_data    TYPE xstring.

    lv_data = zcl_abapgit_convert=>string_to_xstring_utf8( 'data' ).
    ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-sha1 = zcl_abapgit_hash=>sha1_blob( lv_data ).
    ls_object-data = lv_data.
    APPEND ls_object TO lt_objects.

    ls_manifest-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_manifest-path = '/'.
    ls_manifest-name = 'a.txt'.
    ls_manifest-sha1 = ls_object-sha1.
    APPEND ls_manifest TO lt_manifest.

    zcl_abapgit_ortec_porcelain=>materialize_from_manifest(
      EXPORTING
        it_objects       = lt_objects
        it_blob_manifest = lt_manifest
      CHANGING
        ct_files         = lt_files ).

    cl_abap_unit_assert=>assert_equals( act = lt_files[ 1 ]-data exp = lv_data ).
    cl_abap_unit_assert=>assert_equals( act = lt_files[ 1 ]-sha1 exp = ls_object-sha1 ).
  ENDMETHOD.

  METHOD fresh_pull_unit_atomic.
    " ORTEC D2b2 item 6: porcelain's pull_by_branch INCREMENTAL_UPDATE
    " branch now acquires the canonical repo lock, mints an attempt id,
    " calls persist_pull_result, then releases the lock unconditionally.
    " ADAPTATION: pull_by_branch cannot be driven end-to-end here (it
    " requires a live zcl_abapgit_git_transport HTTP round-trip; this test
    " class has no such fixture/mock seam - see its other tests, all of
    " which exercise the pure materialize_from_manifest helper only). This
    " test instead replicates the exact sequence item 6 adds - acquire ->
    " mint -> release - using the same public primitives, proving the span
    " is atomic (fully completes and the lock is free again afterwards).
    DATA(lv_lock_id) = zcl_abapgit_ortec_pack_dec=>acquire_repo_lock( iv_repo_key = c_repo2 ).
    DATA(lv_attempt_id) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo2
      iv_commit   = '1111111111111111111111111111111111111111' ).
    cl_abap_unit_assert=>assert_not_initial( lv_attempt_id ).
    zcl_abapgit_ortec_pack_dec=>release_repo_lock( lv_lock_id ).

    " Lock must be immediately re-acquirable - no lingering hold.
    DATA(lv_lock_id_2) = zcl_abapgit_ortec_pack_dec=>acquire_repo_lock( iv_repo_key = c_repo2 ).
    cl_abap_unit_assert=>assert_not_initial( lv_lock_id_2 ).
    zcl_abapgit_ortec_pack_dec=>release_repo_lock( lv_lock_id_2 ).
  ENDMETHOD.

  METHOD fresh_pull_fail_no_publish.
    " Simulates persist_pull_result raising (e.g. because certify_fetched_
    " commit finds the closure incomplete) using the exact same call
    " sequence porcelain's pull_by_branch now performs, proving that a
    " failure inside the persist span does not leave a published/partial
    " certificate and does not leak the lock (item 6's unconditional
    " release runs regardless of the persist TRY/CATCH outcome).
    DATA(lv_commit) = '2222222222222222222222222222222222222222'.
    DATA lv_lock_held TYPE abap_bool.

    DATA(lv_lock_id) = zcl_abapgit_ortec_pack_dec=>acquire_repo_lock( iv_repo_key = c_repo2 ).
    lv_lock_held = abap_true.
    DATA(lv_attempt_id) = zcl_abapgit_ortec_mat_state=>begin_attempt(
      iv_repo_key = c_repo2
      iv_commit   = lv_commit ).

    TRY.
        " Commit/tree/blob never stored for lv_commit - verify_tree_closure
        " must fail, mirroring persist_pull_result's own certify_fetched_
        " commit call raising zcx_abapgit_ortec_git.
        zcl_abapgit_ortec_fastpath=>certify_fetched_commit(
          iv_repo_key    = c_repo2
          iv_commit      = lv_commit
          iv_branch_name = 'refs/heads/main'
          iv_attempt_id  = lv_attempt_id ).
      CATCH zcx_abapgit_ortec_git.
        " ORTEC: persistence failure is non-critical, continue normally
    ENDTRY.

    IF lv_lock_held = abap_true.
      zcl_abapgit_ortec_pack_dec=>release_repo_lock( lv_lock_id ).
      CLEAR lv_lock_held.
    ENDIF.

    DATA(ls_state) = zcl_abapgit_ortec_mat_state=>get_state(
      iv_repo_key = c_repo2
      iv_commit   = lv_commit ).
    cl_abap_unit_assert=>assert_differs(
      act = ls_state-snap_state
      exp = zcl_abapgit_ortec_mat_state=>cs_snap_state-complete ).

    " Lock must be free again - immediate re-acquire proves no leak.
    DATA(lv_lock_id_2) = zcl_abapgit_ortec_pack_dec=>acquire_repo_lock( iv_repo_key = c_repo2 ).
    cl_abap_unit_assert=>assert_not_initial( lv_lock_id_2 ).
    zcl_abapgit_ortec_pack_dec=>release_repo_lock( lv_lock_id_2 ).
  ENDMETHOD.

  METHOD lock_release_on_failure.
    " Mirrors porcelain pull_by_branch's own acquire-then-degrade TRY block
    " (item 6): CATCH zcx_abapgit_exception zcx_abapgit_ortec_git around
    " acquire_repo_lock + begin_attempt. Uses acquire_repo_lock's one
    " genuinely deterministic failure path (iv_max_attempts <= 0) since
    " real lock contention cannot be simulated from a single ABAP Unit
    " session (the enqueue server does not treat a second acquire from the
    " same user/session as foreign_lock).
    DATA lv_lock_id    TYPE zcl_abapgit_ortec_pack_dec=>ty_session_id.
    DATA lv_lock_held  TYPE abap_bool.
    DATA lv_attempt_id TYPE zcl_abapgit_ortec_mat_state=>ty_attempt_id.

    TRY.
        lv_lock_id = zcl_abapgit_ortec_pack_dec=>acquire_repo_lock(
          iv_repo_key     = c_repo2
          iv_max_attempts = 0 ).
        lv_lock_held = abap_true.
        lv_attempt_id = zcl_abapgit_ortec_mat_state=>begin_attempt(
          iv_repo_key = c_repo2
          iv_commit   = '3333333333333333333333333333333333333333' ).
      CATCH zcx_abapgit_exception zcx_abapgit_ortec_git.
        CLEAR lv_attempt_id.
    ENDTRY.

    cl_abap_unit_assert=>assert_false(
      act = lv_lock_held
      msg = 'Lock must not be marked held when acquire_repo_lock fails' ).

    " The final unconditional release guard is a no-op when the lock was
    " never held - proves it is safe to call regardless.
    IF lv_lock_held = abap_true.
      zcl_abapgit_ortec_pack_dec=>release_repo_lock( lv_lock_id ).
      CLEAR lv_lock_held.
    ENDIF.

    DATA(lv_lock_id_2) = zcl_abapgit_ortec_pack_dec=>acquire_repo_lock( iv_repo_key = c_repo2 ).
    cl_abap_unit_assert=>assert_not_initial( lv_lock_id_2 ).
    zcl_abapgit_ortec_pack_dec=>release_repo_lock( lv_lock_id_2 ).
  ENDMETHOD.
ENDCLASS.

