CLASS ltcl_repo_state DEFINITION FOR TESTING RISK LEVEL HARMLESS DURATION SHORT.
  PRIVATE SECTION.
    CONSTANTS mc_url TYPE string VALUE 'https://test.example.com/repo.git'.
    METHODS setup. METHODS teardown.
    METHODS get_or_create_key        FOR TESTING RAISING cx_static_check.
    METHODS get_or_create_idempotent FOR TESTING RAISING cx_static_check.
    METHODS state_roundtrip          FOR TESTING RAISING cx_static_check.
    METHODS stale_tip_invalidated FOR TESTING RAISING cx_static_check.
    METHODS invalidate_all_history_wide FOR TESTING RAISING cx_static_check.
    "! Regression: get_complete_commits must union commit_hist and
    "! repo_state fetch_commit entries, not treat commit_hist as the sole
    "! source whenever it has any row at all.
    METHODS commits_union_repo_state FOR TESTING RAISING cx_static_check.
ENDCLASS.
CLASS ltcl_repo_state IMPLEMENTATION.
  METHOD setup.
    DATA lv_key TYPE c LENGTH 12.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    IF lv_key IS NOT INITIAL.
      zcl_abapgit_ortec_repo_state=>clear_state( lv_key ).
      DELETE FROM zaog_commit_hist WHERE repo_key = lv_key.
    ENDIF.
  ENDMETHOD.
  METHOD teardown.
    DATA lv_key TYPE c LENGTH 12.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    IF lv_key IS NOT INITIAL.
      zcl_abapgit_ortec_repo_state=>clear_state( lv_key ).
      DELETE FROM zaog_obj_store WHERE repo_key = lv_key.
      DELETE FROM zaog_commit_hist WHERE repo_key = lv_key.
    ENDIF. ROLLBACK WORK.
  ENDMETHOD.
  METHOD get_or_create_key.
    DATA lv_key TYPE c LENGTH 12.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    cl_abap_unit_assert=>assert_not_initial( act = lv_key msg = 'Key must be generated' ).
  ENDMETHOD.
  METHOD get_or_create_idempotent.
    DATA lv1 TYPE c LENGTH 12. DATA lv2 TYPE c LENGTH 12.
    lv1 = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    lv2 = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    cl_abap_unit_assert=>assert_equals( act = lv1 exp = lv2 msg = 'Must be idempotent' ).
  ENDMETHOD.
  METHOD state_roundtrip.
    DATA lv_key TYPE c LENGTH 12. DATA ls_state TYPE zcl_abapgit_ortec_repo_state=>ty_state.
    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    zcl_abapgit_ortec_repo_state=>update_after_fetch( iv_repo_key = lv_key
      iv_branch_name = 'refs/heads/main' iv_url = mc_url
      iv_commit = 'aabbccddee00112233445566778899aabbccddee'
      iv_deepen = 250 ).
    DATA lv_found TYPE c LENGTH 12.
    lv_found = zcl_abapgit_ortec_repo_state=>get_repo_key_for_url( mc_url ).
    cl_abap_unit_assert=>assert_equals( act = lv_found exp = lv_key msg = 'DB lookup should find key' ).
    ls_state = zcl_abapgit_ortec_repo_state=>get_state( iv_repo_key = lv_key iv_branch_name = 'refs/heads/main' ).
    cl_abap_unit_assert=>assert_equals( act = ls_state-fetch_commit
      exp = 'aabbccddee00112233445566778899aabbccddee' msg = 'Commit must match' ).
    " Phase 1 (architecture hardening plan, .memory/state.md, 2026-07-20):
    " an explicit deepen level must round-trip so the next fetch can use it
    " as its starting baseline instead of always restarting from 1.
    cl_abap_unit_assert=>assert_equals( act = ls_state-deepen_lvl exp = 250
      msg = 'An explicit deepen level passed to update_after_fetch must round-trip through get_state' ).
  ENDMETHOD.
  METHOD stale_tip_invalidated.
    " Phase 7 coverage: the "stale-tip fallback" behavior relied on by
    " zcl_abapgit_ortec_filter_walk (and the walk/walk_tree repair path) is
    " driven by invalidate_tip_commit removing the "fully materialised"
    " signal that get_complete_commits/have-negotiation trust. Once a live
    " remote tip no longer matches what's cached, invalidating the tip must
    " make the read path treat it as no longer safe to serve from the local
    " store - forcing a fallback/re-fetch instead of silently serving stale
    " data.
    DATA lv_key TYPE c LENGTH 12.
    DATA lt_commits TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    CONSTANTS lc_commit TYPE zif_abapgit_git_definitions=>ty_sha1
      VALUE 'cccc000000000000000000000000000000000009'.

    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    zcl_abapgit_ortec_repo_state=>update_after_fetch(
      iv_repo_key = lv_key iv_branch_name = 'refs/heads/main'
      iv_url = mc_url iv_commit = lc_commit ).

    " ZAOG_COMMIT_HIST is what actually marks a commit as fully materialised
    " for get_complete_commits/have-negotiation purposes.
    INSERT zaog_commit_hist FROM @( VALUE #(
      repo_key = lv_key commit_sha1 = lc_commit branch_name = 'refs/heads/main' ) ).
    COMMIT WORK.

    lt_commits = zcl_abapgit_ortec_repo_state=>get_complete_commits( lv_key ).
    READ TABLE lt_commits WITH KEY table_line = lc_commit TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc(
      msg = 'Commit must be considered fully materialised before invalidation' ).

    zcl_abapgit_ortec_repo_state=>invalidate_tip_commit(
      iv_repo_key = lv_key iv_commit = lc_commit iv_branch_name = 'refs/heads/main' ).

    CLEAR lt_commits.
    lt_commits = zcl_abapgit_ortec_repo_state=>get_complete_commits( lv_key ).
    READ TABLE lt_commits WITH KEY table_line = lc_commit TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_equals( act = sy-subrc exp = 4
      msg = 'A stale/invalidated tip must no longer be considered fully materialised, ' &&
            'forcing the read path to fall back instead of trusting cached data' ).

    DATA(ls_state) = zcl_abapgit_ortec_repo_state=>get_state(
      iv_repo_key = lv_key iv_branch_name = 'refs/heads/main' ).
    cl_abap_unit_assert=>assert_initial( act = ls_state-fetch_commit
      msg = 'fetch_commit must be blanked so a stale tip cannot be reused for Phase 3 reconstitution' ).
  ENDMETHOD.
  METHOD invalidate_all_history_wide.
    " ES6 incident coverage: pull_by_branch's self-heal must guarantee an
    " empty have-set on retry (forcing a full/deepen pack), not just clear
    " the ONE commit/branch that happened to fail its walk. Reproduces two
    " branches sharing one repo, both marked fully materialised, then
    " verifies invalidate_all_history wipes ZAOG_COMMIT_HIST for the WHOLE
    " repo and blanks fetch_commit for EVERY branch, not just one.
    DATA lv_key TYPE c LENGTH 12.
    DATA lt_commits TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    CONSTANTS lc_commit_main TYPE zif_abapgit_git_definitions=>ty_sha1
      VALUE 'dddd000000000000000000000000000000000001'.
    CONSTANTS lc_commit_dev TYPE zif_abapgit_git_definitions=>ty_sha1
      VALUE 'dddd000000000000000000000000000000000002'.

    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    zcl_abapgit_ortec_repo_state=>update_after_fetch(
      iv_repo_key = lv_key iv_branch_name = 'refs/heads/main'
      iv_url = mc_url iv_commit = lc_commit_main ).
    zcl_abapgit_ortec_repo_state=>update_after_fetch(
      iv_repo_key = lv_key iv_branch_name = 'refs/heads/dev'
      iv_url = mc_url iv_commit = lc_commit_dev ).
    INSERT zaog_commit_hist FROM @( VALUE #(
      repo_key = lv_key commit_sha1 = lc_commit_main branch_name = 'refs/heads/main' ) ).
    INSERT zaog_commit_hist FROM @( VALUE #(
      repo_key = lv_key commit_sha1 = lc_commit_dev branch_name = 'refs/heads/dev' ) ).
    COMMIT WORK.

    lt_commits = zcl_abapgit_ortec_repo_state=>get_complete_commits( lv_key ).
    cl_abap_unit_assert=>assert_equals( act = lines( lt_commits ) exp = 2
      msg = 'Both branches'' commits must be considered fully materialised before invalidation' ).

    zcl_abapgit_ortec_repo_state=>invalidate_all_history( lv_key ).

    lt_commits = zcl_abapgit_ortec_repo_state=>get_complete_commits( lv_key ).
    cl_abap_unit_assert=>assert_initial( act = lt_commits
      msg = 'invalidate_all_history must leave NO commit advertisable as a have, ' &&
            'so the retry degrades to a full/deepen pack instead of repeating the same thin fetch' ).

    DATA(ls_main) = zcl_abapgit_ortec_repo_state=>get_state(
      iv_repo_key = lv_key iv_branch_name = 'refs/heads/main' ).
    DATA(ls_dev) = zcl_abapgit_ortec_repo_state=>get_state(
      iv_repo_key = lv_key iv_branch_name = 'refs/heads/dev' ).
    cl_abap_unit_assert=>assert_initial( act = ls_main-fetch_commit
      msg = 'fetch_commit must be blanked for EVERY branch, not just the one that failed its walk' ).
    cl_abap_unit_assert=>assert_initial( act = ls_dev-fetch_commit
      msg = 'fetch_commit must be blanked for EVERY branch, not just the one that failed its walk' ).
  ENDMETHOD.
  METHOD commits_union_repo_state.
    " Regression: get_complete_commits previously used zaog_commit_hist as
    " the ONLY source whenever it had ANY row at all for the repo, silently
    " hiding every OTHER branch's own recorded fetch_commit in
    " zaog_repo_state from have-negotiation - even though those branches
    " were fully fetched and typically share most of their object graph as
    " common ancestry with the branch being switched to. Reproduces exactly
    " that: one branch tracked only in commit_hist, a second tracked only
    " via its own repo_state fetch_commit (never added to commit_hist) -
    " both must be offered as candidates.
    DATA lv_key TYPE c LENGTH 12.
    DATA lt_commits TYPE zif_abapgit_git_definitions=>ty_sha1_tt.
    CONSTANTS lc_commit_hist_only TYPE zif_abapgit_git_definitions=>ty_sha1
      VALUE 'eeee000000000000000000000000000000000001'.
    CONSTANTS lc_commit_state_only TYPE zif_abapgit_git_definitions=>ty_sha1
      VALUE 'eeee000000000000000000000000000000000002'.

    lv_key = zcl_abapgit_ortec_repo_state=>get_or_create_repo_key_for_url( mc_url ).
    zcl_abapgit_ortec_repo_state=>update_after_fetch(
      iv_repo_key = lv_key iv_branch_name = 'refs/heads/history-branch'
      iv_url = mc_url iv_commit = lc_commit_hist_only ).
    zcl_abapgit_ortec_repo_state=>update_after_fetch(
      iv_repo_key = lv_key iv_branch_name = 'refs/heads/state-only-branch'
      iv_url = mc_url iv_commit = lc_commit_state_only ).
    INSERT zaog_commit_hist FROM @( VALUE #(
      repo_key = lv_key commit_sha1 = lc_commit_hist_only branch_name = 'refs/heads/history-branch' ) ).
    COMMIT WORK.

    lt_commits = zcl_abapgit_ortec_repo_state=>get_complete_commits( lv_key ).

    READ TABLE lt_commits WITH KEY table_line = lc_commit_hist_only TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc(
      msg = 'The commit_hist-tracked commit must be a candidate' ).
    READ TABLE lt_commits WITH KEY table_line = lc_commit_state_only TRANSPORTING NO FIELDS.
    cl_abap_unit_assert=>assert_subrc(
      msg = 'A second branch tracked ONLY via its own repo_state fetch_commit ' &&
            '(never added to commit_hist) must ALSO be a candidate, not hidden ' &&
            'just because commit_hist happens to have an unrelated row' ).
  ENDMETHOD.
ENDCLASS.
