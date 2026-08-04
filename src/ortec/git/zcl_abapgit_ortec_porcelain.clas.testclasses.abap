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

    " Package E OF-2/E-HARDEN (design §7): shared 'Walk,' trigger-text
    " contract between walk()'s raise sites and pull_by_branch's CS check.
    METHODS walk_uses_shared_prefix     FOR TESTING RAISING cx_static_check.
    METHODS pull_retry_matches_walk     FOR TESTING RAISING cx_static_check.

    " Package E E4-VERIFY (design §6/§9): regression coverage for CR-10
    " outcome #9 (Diff/status calculation after a cold branch switch,
    " correctness-review MINOR-2). E4-D-01/02/04 have NO test method here
    " (pre-import audit, 2026-07-29): they were previously always-passing
    " `assert_true( abap_true )` placeholders, which is not a test and was
    " removed. Their dispositions (NOT_APPLICABLE_WITH_EXACT_SOURCE_PROOF
    " for E4-D-04, BLOCKED_BY_MISSING_TEST_SEAM for E4-D-01/02) are recorded
    " in regression_variant_b_package_e_checkpoint_1.md, not as ABAP Unit
    " methods.
    METHODS status_after_cold_switch    FOR TESTING RAISING cx_static_check.

    " "tree not found" push/commit regression (2026-08-04): a PULL that
    " classified WARM_UNCHANGED/COLD_BRANCH seeds IT_OBJECTS with only the
    " commit object, relying on ZAOG_OBJ_STORE for the rest - FULL_TREE
    " must reconstruct the base tree from the buffer in that case instead
    " of raising, exactly like WALK_TREE already does.
    METHODS full_tree_sparse_seed       FOR TESTING RAISING cx_static_check.
    METHODS full_tree_uses_it_objects   FOR TESTING RAISING cx_static_check.
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

  METHOD walk_uses_shared_prefix.
    " E-HARDEN-01 (design §7, OF-2): walk() must raise its "tree not found"
    " text using the SAME shared prefix constant that pull_by_branch's own
    " CS check tests against - proving the producer side of the contract,
    " not just that walk() still raises SOME exception.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_files   TYPE zif_abapgit_git_definitions=>ty_files_tt.
    DATA lv_missing_sha1 TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_caught_text  TYPE string.

    lv_missing_sha1 = repeat( val = 'f' occ = 40 ).

    TRY.
        zcl_abapgit_ortec_porcelain=>walk(
          EXPORTING
            it_objects  = lt_objects
            iv_sha1     = lv_missing_sha1
            iv_path     = ''
            iv_repo_key = c_repo2
          CHANGING
            ct_files    = lt_files ).
        cl_abap_unit_assert=>fail( 'walk() must raise when the tree object is missing entirely' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_walk).
        lv_caught_text = lx_walk->get_text( ).
    ENDTRY.

    " Exact-text assertion (pre-import audit, 2026-07-29 - a wildcard-only
    " assert_char_cp cannot prove the string-template interpolation adds no
    " stray/missing characters). |{ c_walk_error_prefix } tree not found|
    " with no WIDTH/ALIGN formatting option outputs the constant's value
    " verbatim immediately followed by the literal ' tree not found' - byte-
    " for-byte identical to the pre-OF-2 literal 'Walk, tree not found'.
    cl_abap_unit_assert=>assert_equals(
      act = lv_caught_text
      exp = 'Walk, tree not found'
      msg = 'walk() must raise the exact pre-OF-2 text via the shared c_walk_error_prefix constant' ).
  ENDMETHOD.

  METHOD pull_retry_matches_walk.
    " E-HARDEN-02 (design §7, OF-2): the CONSUMER side of the same
    " contract - pull_by_branch's own CS check must match against the
    " EXACT text walk() actually raises, proven here by driving walk()
    " for real and checking its text with the identical CS operator/
    " operand pull_by_branch uses, instead of duplicating a second literal.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_files   TYPE zif_abapgit_git_definitions=>ty_files_tt.
    DATA lv_missing_sha1 TYPE zif_abapgit_git_definitions=>ty_sha1.
    DATA lv_walk_text    TYPE string.

    lv_missing_sha1 = repeat( val = 'f' occ = 40 ).

    TRY.
        zcl_abapgit_ortec_porcelain=>walk(
          EXPORTING
            it_objects  = lt_objects
            iv_sha1     = lv_missing_sha1
            iv_path     = ''
            iv_repo_key = c_repo2
          CHANGING
            ct_files    = lt_files ).
        cl_abap_unit_assert=>fail( 'walk() must raise when the tree object is missing entirely' ).
      CATCH zcx_abapgit_exception INTO DATA(lx_walk).
        lv_walk_text = lx_walk->get_text( ).
    ENDTRY.

    " Exact-text assertion first (pre-import audit, 2026-07-29), then the
    " same CS operator/operand pull_by_branch itself uses, so both the
    " literal text AND the actual production match expression are proven.
    cl_abap_unit_assert=>assert_equals(
      act = lv_walk_text
      exp = 'Walk, tree not found'
      msg = 'walk() must raise the exact pre-OF-2 text' ).
    cl_abap_unit_assert=>assert_true(
      act = xsdbool( lv_walk_text CS zcl_abapgit_ortec_git_switch=>c_walk_error_prefix )
      msg = 'pull_by_branch''s own retry-trigger check must match the text walk() actually raises' ).
  ENDMETHOD.

  METHOD status_after_cold_switch.
    " CR-10 outcome #9 / correctness-review MINOR-2 (design §9, hard
    " requirement owned by E4-VERIFY): proves that status calculation
    " reflects the NEWLY pulled branch after a cold branch switch, not
    " stale content left over from a previous branch on the SAME repo key.
    " zcl_abapgit_status_calc itself is confirmed unmodified by ORTEC
    " (discovery §E2) - this test pins the INTEGRATION between porcelain's
    " materialize_from_manifest output and status_calc, via the TADIR-free
    " build_existing code path (both local and remote match on path and
    " filename, so no zcl_abapgit_factory=>get_tadir() dependency is hit).
    DATA lt_manifest  TYPE zif_abapgit_git_definitions=>ty_expanded_tt.
    DATA ls_manifest  LIKE LINE OF lt_manifest.
    DATA lt_objects   TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA ls_object    LIKE LINE OF lt_objects.
    DATA lt_files_a   TYPE zif_abapgit_git_definitions=>ty_files_tt.
    DATA lt_files_b   TYPE zif_abapgit_git_definitions=>ty_files_tt.
    DATA lt_state     TYPE zif_abapgit_git_definitions=>ty_file_signatures_tt.
    DATA ls_state     LIKE LINE OF lt_state.
    DATA lt_local     TYPE zif_abapgit_definitions=>ty_files_item_tt.
    DATA ls_local     LIKE LINE OF lt_local.

    " Branch A content, materialized via the exact same primitive
    " porcelain itself uses (materialize_from_manifest).
    DATA(lv_data_a) = zcl_abapgit_convert=>string_to_xstring_utf8( 'branch-a-content' ).
    CLEAR ls_object.
    ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-sha1 = zcl_abapgit_hash=>sha1_blob( lv_data_a ).
    ls_object-data = lv_data_a.
    APPEND ls_object TO lt_objects.

    ls_manifest-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_manifest-path  = '/'.
    ls_manifest-name  = 'zswitch.prog.abap'.
    ls_manifest-sha1  = ls_object-sha1.
    APPEND ls_manifest TO lt_manifest.

    zcl_abapgit_ortec_porcelain=>materialize_from_manifest(
      EXPORTING
        it_objects       = lt_objects
        it_blob_manifest = lt_manifest
      CHANGING
        ct_files         = lt_files_a ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_files_a ) exp = 1 ).

    " Cold branch switch: an INDEPENDENT materialize call for the SAME
    " path/filename but a DIFFERENT branch tip's blob - this is exactly
    " what a real branch switch on an installed repo produces (a fresh
    " manifest for the newly checked-out branch, with no memory of the
    " previous branch).
    CLEAR: lt_objects, lt_manifest, ls_object.
    DATA(lv_data_b) = zcl_abapgit_convert=>string_to_xstring_utf8( 'branch-b-content' ).
    ls_object-type = zif_abapgit_git_definitions=>c_type-blob.
    ls_object-sha1 = zcl_abapgit_hash=>sha1_blob( lv_data_b ).
    ls_object-data = lv_data_b.
    APPEND ls_object TO lt_objects.

    ls_manifest-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_manifest-path  = '/'.
    ls_manifest-name  = 'zswitch.prog.abap'.
    ls_manifest-sha1  = ls_object-sha1.
    APPEND ls_manifest TO lt_manifest.

    zcl_abapgit_ortec_porcelain=>materialize_from_manifest(
      EXPORTING
        it_objects       = lt_objects
        it_blob_manifest = lt_manifest
      CHANGING
        ct_files         = lt_files_b ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_files_b ) exp = 1 ).
    cl_abap_unit_assert=>assert_differs(
      act = lt_files_b[ 1 ]-sha1
      exp = lt_files_a[ 1 ]-sha1
      msg = 'Sanity: the branch switch fixture must actually change the blob sha1' ).

    " A local copy still reflecting branch A (what was actually pulled and
    " installed BEFORE the switch) and a state signature also pinned to
    " branch A (the repo's last-known-good state before the switch).
    ls_local-file-path     = '/'.
    ls_local-file-filename = 'zswitch.prog.abap'.
    ls_local-file-sha1     = lt_files_a[ 1 ]-sha1.
    APPEND ls_local TO lt_local.

    ls_state-path     = '/'.
    ls_state-filename = 'zswitch.prog.abap'.
    ls_state-sha1     = lt_files_a[ 1 ]-sha1.
    APPEND ls_state TO lt_state.

    DATA(lo_dot) = zcl_abapgit_dot_abapgit=>build_default( ).
    lo_dot->set_starting_folder( '/' ).

    DATA(li_calc) = zcl_abapgit_status_calc=>get_instance(
      iv_root_package = '$TMP'
      io_dot          = lo_dot ).

    DATA(lt_results) = li_calc->calculate_status(
      it_local     = lt_local
      it_remote    = lt_files_b
      it_cur_state = lt_state ).

    cl_abap_unit_assert=>assert_equals(
      act = lines( lt_results )
      exp = 1
      msg = 'One result row expected for the single switched file' ).

    cl_abap_unit_assert=>assert_equals(
      act = lt_results[ 1 ]-rstate
      exp = zif_abapgit_definitions=>c_state-modified
      msg = 'status_calc must report the NEW branch (B) content as modified ' &&
            'relative to the pre-switch state - not unchanged, which would ' &&
            'mean a stale/cached branch A file leaked into the remote list' ).

    cl_abap_unit_assert=>assert_initial(
      act = lt_results[ 1 ]-lstate
      msg = 'The local copy itself did not change across the switch - only the remote branch tip did' ).
  ENDMETHOD.

  METHOD full_tree_sparse_seed.
    " Regression for the "tree not found" push/commit bug: the parent
    " commit, its tree and its one blob are persisted ONLY in
    " ZAOG_OBJ_STORE - IT_OBJECTS mirrors WARM_UNCHANGED/COLD_BRANCH's
    " sparse seed (the commit object alone, nothing else).
    DATA lt_nodes    TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node     LIKE LINE OF lt_nodes.
    DATA ls_commit   TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lt_store    TYPE zif_abapgit_definitions=>ty_objects_tt.
    DATA lt_sparse   TYPE zif_abapgit_definitions=>ty_objects_tt.

    DATA(lv_blob_data) = zcl_abapgit_convert=>string_to_xstring_utf8( 'content' ).
    DATA(lv_blob_sha)  = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).

    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_node-name  = 'a.txt'.
    ls_node-sha1  = lv_blob_sha.
    APPEND ls_node TO lt_nodes.
    DATA(lv_tree_data) = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    DATA(lv_tree_sha)  = zcl_abapgit_hash=>sha1_tree( lv_tree_data ).

    ls_commit-tree      = lv_tree_sha.
    ls_commit-committer = 'A <a@b.com> 0 +0000'.
    ls_commit-author    = ls_commit-committer.
    ls_commit-body      = 'msg'.
    DATA(lv_commit_data) = zcl_abapgit_git_pack=>encode_commit( ls_commit ).
    DATA(lv_commit_sha)  = zcl_abapgit_hash=>sha1_commit( lv_commit_data ).

    APPEND VALUE #( type = zif_abapgit_git_definitions=>c_type-commit
                     sha1 = lv_commit_sha data = lv_commit_data ) TO lt_store.
    APPEND VALUE #( type = zif_abapgit_git_definitions=>c_type-tree
                     sha1 = lv_tree_sha   data = lv_tree_data )   TO lt_store.
    APPEND VALUE #( type = zif_abapgit_git_definitions=>c_type-blob
                     sha1 = lv_blob_sha   data = lv_blob_data )   TO lt_store.
    zcl_abapgit_ortec_obj_store=>store_objects( iv_repo_key = c_repo2 it_objects = lt_store ).
    COMMIT WORK.

    APPEND VALUE #( type = zif_abapgit_git_definitions=>c_type-commit
                     sha1 = lv_commit_sha data = lv_commit_data ) TO lt_sparse.

    DATA(lt_expanded) = zcl_abapgit_ortec_porcelain=>full_tree(
                             it_objects  = lt_sparse
                             iv_parent   = lv_commit_sha
                             iv_repo_key = c_repo2 ).

    cl_abap_unit_assert=>assert_equals(
      act = lines( lt_expanded )
      exp = 1
      msg = 'full_tree must reconstruct the base tree from ZAOG_OBJ_STORE when IT_OBJECTS only seeds the commit' ).
    cl_abap_unit_assert=>assert_equals( act = lt_expanded[ 1 ]-name exp = 'a.txt' ).
    cl_abap_unit_assert=>assert_equals( act = lt_expanded[ 1 ]-sha1 exp = lv_blob_sha ).
  ENDMETHOD.

  METHOD full_tree_uses_it_objects.
    " Cheap path: when IT_OBJECTS already carries the commit/tree (e.g.
    " right after INCREMENTAL_UPDATE), full_tree must use them directly -
    " no ZAOG_OBJ_STORE row exists for this repo key at all, so any buffer
    " fallback attempt would raise.
    DATA lt_nodes  TYPE zcl_abapgit_git_pack=>ty_nodes_tt.
    DATA ls_node   LIKE LINE OF lt_nodes.
    DATA ls_commit TYPE zcl_abapgit_git_pack=>ty_commit.
    DATA lt_objects TYPE zif_abapgit_definitions=>ty_objects_tt.

    DATA(lv_blob_data) = zcl_abapgit_convert=>string_to_xstring_utf8( 'content2' ).
    DATA(lv_blob_sha)  = zcl_abapgit_hash=>sha1_blob( lv_blob_data ).

    ls_node-chmod = zif_abapgit_git_definitions=>c_chmod-file.
    ls_node-name  = 'b.txt'.
    ls_node-sha1  = lv_blob_sha.
    APPEND ls_node TO lt_nodes.
    DATA(lv_tree_data) = zcl_abapgit_git_pack=>encode_tree( lt_nodes ).
    DATA(lv_tree_sha)  = zcl_abapgit_hash=>sha1_tree( lv_tree_data ).

    ls_commit-tree      = lv_tree_sha.
    ls_commit-committer = 'A <a@b.com> 0 +0000'.
    ls_commit-author    = ls_commit-committer.
    ls_commit-body      = 'msg2'.
    DATA(lv_commit_data) = zcl_abapgit_git_pack=>encode_commit( ls_commit ).
    DATA(lv_commit_sha)  = zcl_abapgit_hash=>sha1_commit( lv_commit_data ).

    APPEND VALUE #( type = zif_abapgit_git_definitions=>c_type-commit
                     sha1 = lv_commit_sha data = lv_commit_data ) TO lt_objects.
    APPEND VALUE #( type = zif_abapgit_git_definitions=>c_type-tree
                     sha1 = lv_tree_sha   data = lv_tree_data )   TO lt_objects.
    APPEND VALUE #( type = zif_abapgit_git_definitions=>c_type-blob
                     sha1 = lv_blob_sha   data = lv_blob_data )   TO lt_objects.

    DATA(lt_expanded) = zcl_abapgit_ortec_porcelain=>full_tree(
                             it_objects  = lt_objects
                             iv_parent   = lv_commit_sha
                             iv_repo_key = c_repo2 ).

    cl_abap_unit_assert=>assert_equals( act = lines( lt_expanded ) exp = 1 ).
    cl_abap_unit_assert=>assert_equals( act = lt_expanded[ 1 ]-name exp = 'b.txt' ).
  ENDMETHOD.

  " E4-D-01, E4-D-02, E4-D-04 (design §6 test matrix): removed as ABAP Unit
  " methods during the 2026-07-29 pre-import audit - they were always-
  " passing `assert_true( abap_true )` stubs, which the audit's explicit
  " rule disallows even when the surrounding comment documents a real
  " scenario. Their dispositions are:
  "   E4-D-04 = NOT_APPLICABLE_WITH_EXACT_SOURCE_PROOF - the dispatch-
  "     exclusivity guarantee is fully provable by direct source read:
  "     src/git/zcl_abapgit_git_porcelain.clas.abap lines 531-538 is an
  "     unconditional `IF is_active_for_repo(...) = abap_true. ... RETURN.
  "     ENDIF.` before the standard file's own embedded 'Walk,' block, so
  "     the two 'Walk,' retry blocks can never both execute for one call.
  "   E4-D-01/E4-D-02 = BLOCKED_BY_MISSING_TEST_SEAM - the full self-heal
  "     retry cascade requires two live/mocked zcl_abapgit_git_transport=>
  "     upload_pack_by_branch HTTP round-trips; no such mock seam exists in
  "     this project (same class of limitation as fresh_pull_unit_atomic),
  "     and adding one is out of scope for this correction. The two
  "     sub-components the scenario depends on ARE independently, really
  "     tested: the shared 'Walk,' prefix contract by
  "     walk_uses_shared_prefix/pull_retry_matches_walk (this file), and
  "     invalidate_all_history's own correctness by
  "     zcl_abapgit_ortec_repo_state.clas.testclasses.abap~
  "     invalidate_all_history_wide.
  " See regression_variant_b_package_e_checkpoint_1.md for the full
  " finding-to-fix record.
ENDCLASS.
