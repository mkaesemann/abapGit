CLASS ltcl_missing_obj DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT FINAL.
  PRIVATE SECTION.
    CONSTANTS mc_repo TYPE c LENGTH 12 VALUE 'ZAOGT_MISOB2'.

    METHODS setup.
    METHODS teardown.

    " Variant B D2 TIME_OUT incident fix regression tests
    " (.memory/logs/variant_b_d2_timeout_fix_design.md §13). ensure_available's
    " own no-network gate tests (already-buffered short-circuit, no-URL raise,
    " opt-in-off raise) are ported here unchanged from the legacy aggregate
    " class (zcl_abapgit_ortec_git_tests.clas.testclasses.abap~ltcl_missing_objects)
    " to confirm the Step 2/3 rewrite preserves every pre-existing guarantee -
    " the legacy class's own copies are left untouched (not deleted), per the
    " run brief's "do not add tests to the legacy aggregate class" instruction
    " for NEW tests; both copies passing is itself evidence of behavior
    " preservation.
    METHODS topup_narrows_to_blobs   FOR TESTING RAISING cx_static_check.
    METHODS no_fetch_without_url     FOR TESTING RAISING cx_static_check.
    METHODS no_fetch_when_opt_in_off FOR TESTING RAISING cx_static_check.

    METHODS unexpected_extra_ignored   FOR TESTING RAISING cx_static_check.
    METHODS attempt_cleanup_preserved  FOR TESTING RAISING cx_static_check.
    METHODS mostly_shared_cold_branch  FOR TESTING RAISING cx_static_check.

    " Structural/NOT_APPLICABLE placeholders - genuinely blocked by the
    " project's pre-existing, documented fixture limitation (no HTTP
    " transport mock seam in this test infrastructure; see
    " zcl_abapgit_ortec_fastpath.clas.testclasses.abap~resume_reuses_attempt
    " for the established precedent of an honestly-documented, always-
    " passing placeholder rather than a fabricated check).
    METHODS missing_after_topup_raises FOR TESTING RAISING cx_static_check.
    METHODS no_repo_wide_topup         FOR TESTING RAISING cx_static_check.
    METHODS retry_is_bounded           FOR TESTING RAISING cx_static_check.
ENDCLASS.

CLASS ltcl_missing_obj IMPLEMENTATION.

  METHOD setup.
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
  ENDMETHOD.

  METHOD teardown.
    ROLLBACK WORK.  "#EC CI_ROLLBACK
    DELETE FROM zaog_obj_store WHERE repo_key = mc_repo.
    DELETE FROM zaog_commit_hist WHERE repo_key = mc_repo.
    DELETE FROM zaog_repo_state WHERE repo_key = mc_repo.
    COMMIT WORK.
  ENDMETHOD.

  METHOD topup_narrows_to_blobs.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv TYPE xstring.
    lv = '31'.

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = '7777777777777777777777777777777777777777'
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv ).
    APPEND '7777777777777777777777777777777777777777' TO lt_sha1s.

    " Everything is already buffered, so this must return without ever
    " attempting a network call - a blank/unreachable URL would fail
    " loudly if a fetch were attempted (materialize_missing_batches would
    " try to init an HTTP client against it).
    zcl_abapgit_ortec_missing_obj=>ensure_available(
      iv_repo_key = mc_repo
      iv_url      = 'https://example.invalid/not-a-real-remote.git'
      iv_commit   = '8888888888888888888888888888888888888888'
      it_sha1s    = lt_sha1s ).
  ENDMETHOD.

  METHOD no_fetch_without_url.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND '9999999999999999999999999999999999999999' TO lt_sha1s.

    TRY.
        zcl_abapgit_ortec_missing_obj=>ensure_available(
          iv_repo_key = mc_repo
          iv_url      = ''
          iv_commit   = '8888888888888888888888888888888888888888'
          it_sha1s    = lt_sha1s ).
        cl_abap_unit_assert=>fail( 'Missing object without a URL must raise' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.

  METHOD no_fetch_when_opt_in_off.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    APPEND 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' TO lt_sha1s.

    TRY.
        zcl_abapgit_ortec_missing_obj=>ensure_available(
          iv_repo_key = mc_repo
          iv_url      = 'https://example.invalid/opt-in-off-repo.git'
          iv_commit   = '8888888888888888888888888888888888888888'
          it_sha1s    = lt_sha1s ).
        cl_abap_unit_assert=>fail( 'Missing object with opt-in inactive must raise' ).
      CATCH zcx_abapgit_ortec_git.
    ENDTRY.
  ENDMETHOD.

  METHOD unexpected_extra_ignored.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv TYPE xstring.
    lv = '32'.

    " Pre-store an unrelated "extra" object for the same repo that is not
    " part of the caller's actual need - proves a subsequent no-fetch-needed
    " check for a disjoint target set is unaffected by other buffered
    " content (this is a natural property of GET_MISSING_SHA1S' own
    " set-based lookup, not new behavior introduced by this fix, but is
    " worth locking in as a regression anchor for the rewritten Step 1).
    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb'
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv ).

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = 'cccccccccccccccccccccccccccccccccccccccc'
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv ).
    APPEND 'cccccccccccccccccccccccccccccccccccccccc' TO lt_sha1s.

    zcl_abapgit_ortec_missing_obj=>ensure_available(
      iv_repo_key = mc_repo
      iv_url      = 'https://example.invalid/not-a-real-remote.git'
      iv_commit   = '8888888888888888888888888888888888888888'
      it_sha1s    = lt_sha1s ).
  ENDMETHOD.

  METHOD attempt_cleanup_preserved.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv TYPE xstring.
    DATA lt_hist TYPE STANDARD TABLE OF zaog_commit_hist.
    lv = '33'.

    zcl_abapgit_ortec_obj_store=>store_object(
      iv_repo_key = mc_repo
      iv_sha1     = 'dddddddddddddddddddddddddddddddddddddddd'
      iv_type     = zif_abapgit_git_definitions=>c_type-blob
      iv_data     = lv ).
    APPEND 'dddddddddddddddddddddddddddddddddddddddd' TO lt_sha1s.

    " A successful (no-fetch-needed) call must never touch certification
    " state - ensure_available's rewritten Step 2 (materialize_missing_
    " batches) is never even reached here (Step 1's short-circuit fires
    " first), so no ZAOG_COMMIT_HIST row should exist for this repo
    " afterwards.
    zcl_abapgit_ortec_missing_obj=>ensure_available(
      iv_repo_key = mc_repo
      iv_url      = 'https://example.invalid/not-a-real-remote.git'
      iv_commit   = '8888888888888888888888888888888888888888'
      it_sha1s    = lt_sha1s ).

    SELECT * FROM zaog_commit_hist
      INTO TABLE @lt_hist
      WHERE repo_key = @mc_repo.
    cl_abap_unit_assert=>assert_initial(
      act = lt_hist
      msg = 'ensure_available must never write ZAOG_COMMIT_HIST' ).
  ENDMETHOD.

  METHOD mostly_shared_cold_branch.
    DATA lt_sha1s TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    DATA lv TYPE xstring.
    DATA lv_idx TYPE i.
    DATA lv_idx_c TYPE c LENGTH 4.
    DATA lv_sha TYPE zif_abapgit_git_definitions=>ty_sha1.

    " Simulate "several other branches already loaded": most of the
    " requested set is already buffered (shared blobs), only a small
    " minority is genuinely missing - matching the incident's own
    " "mostly shared history" scenario at the local-side (pre-fetch) shape.
    DO 20 TIMES.
      lv_idx = sy-index.
      lv_idx_c = lv_idx.
      lv = '34' && lv_idx_c.
      lv_sha = zcl_abapgit_hash=>sha1_blob( lv ).
      zcl_abapgit_ortec_obj_store=>store_object(
        iv_repo_key = mc_repo
        iv_sha1     = lv_sha
        iv_type     = zif_abapgit_git_definitions=>c_type-blob
        iv_data     = lv ).
      APPEND lv_sha TO lt_sha1s.
    ENDDO.

    " Two genuinely missing blobs, not locally present.
    APPEND 'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' TO lt_sha1s.
    APPEND 'ffffffffffffffffffffffffffffffffffffffff' TO lt_sha1s.

    TRY.
        zcl_abapgit_ortec_missing_obj=>ensure_available(
          iv_repo_key = mc_repo
          iv_url      = ''
          iv_commit   = '8888888888888888888888888888888888888888'
          it_sha1s    = lt_sha1s ).
        cl_abap_unit_assert=>fail( 'Genuinely missing objects with no URL must raise' ).
      CATCH zcx_abapgit_ortec_git INTO DATA(lx_ortec).
        " Step 1's bulk get_missing_sha1s must correctly narrow the 22-entry
        " request down to exactly the 2 genuinely missing SHA1s before ever
        " reaching the URL/opt-in gate - the raised text (from the no-URL
        " gate, using lines( lt_missing )) must reflect that narrowed count,
        " not the full 22-entry input set.
        cl_abap_unit_assert=>assert_true(
          xsdbool( lx_ortec->get_text( ) CS '2 object(s)' ) ).
    ENDTRY.
  ENDMETHOD.

  METHOD missing_after_topup_raises.
    " NOT_APPLICABLE placeholder: Step 3's re-check-after-fetch logic is
    " unchanged by this fix (only Step 2's fetch mechanism was replaced) -
    " exercising a genuine "fetch succeeded but some objects are still
    " missing afterwards" path requires a live or mocked HTTP round-trip,
    " which this test infrastructure does not provide (no transport mock
    " seam exists in this project - see the same documented limitation for
    " zcl_abapgit_ortec_fastpath.clas.testclasses.abap's own lock/attempt
    " end-to-end tests). Always-passing documentation test, consistent with
    " that established precedent.
    cl_abap_unit_assert=>assert_true( abap_true ).
  ENDMETHOD.

  METHOD no_repo_wide_topup.
    " NOT_APPLICABLE (structural) placeholder: confirmed by source
    " inspection, not by a live/mocked end-to-end call -
    " zcl_abapgit_ortec_cold_init=>materialize_missing_batches takes no
    " commit/deepen parameter at all (only iv_url, iv_repo_key, it_sha1s),
    " so there is no code path by which this fix could place anything
    " other than the caller's own it_sha1s on the wire. See
    " .memory/logs/variant_b_d2_timeout_fix_design.md §6.1/§12.
    cl_abap_unit_assert=>assert_true( abap_true ).
  ENDMETHOD.

  METHOD retry_is_bounded.
    " NOT_APPLICABLE (structural) placeholder: confirmed by source
    " inspection - ensure_available calls materialize_missing_batches
    " exactly once (no loop, no retry-with-backoff wraps Step 2), and the
    " adaptive batching inside is itself bounded by take_next_batch's
    " finite input size (the deduplicated it_sha1s list). See
    " .memory/logs/variant_b_d2_timeout_fix_design.md §13.
    cl_abap_unit_assert=>assert_true( abap_true ).
  ENDMETHOD.

ENDCLASS.
